use super::*;

#[test]
fn hash_join_conjunction_avoids_cartesian_candidate_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let keys = (1..=10)
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let warehouses = ["1"; 10].join(",");
    let query = format!(
        "WITH supplied AS (
           SELECT item_id, warehouse_id
           FROM UNNEST(ARRAY[{keys}], ARRAY[{warehouses}]) AS s(item_id, warehouse_id)
         )
         SELECT requested.item_id
         FROM UNNEST(ARRAY[{keys}], ARRAY[{warehouses}]) AS requested(item_id, warehouse_id)
         JOIN supplied AS updated
           ON updated.item_id = requested.item_id
          AND updated.warehouse_id = requested.warehouse_id
         ORDER BY requested.item_id"
    );
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlSession::new(&mut db).execute(&query);
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    assert_eq!(
        result.unwrap().rows,
        (1..=10).map(|n| vec![SqlValue::Int(n)]).collect::<Vec<_>>()
    );
    assert_eq!(
        stats.join_candidate_pairs, 10,
        "one key probe per order line"
    );
}

#[test]
fn hash_join_conjunction_keeps_residuals_duplicates_and_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let ctes = "WITH l(k,w,v) AS (VALUES (1,1,5),(1,2,8),(2,1,4),(NULL,1,5)),
                     r(k,w,v) AS (VALUES (1,1,6),(1,1,9),(1,2,7),(2,1,3),(NULL,1,6))";
    let mut sql = SqlSession::new(&mut db);
    let result = sql
        .execute(&format!(
            "{ctes} SELECT l.k,l.w,r.v FROM l JOIN r
         ON l.v < r.v AND ((r.k = l.k) AND l.w = r.w) ORDER BY r.v"
        ))
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Int(6)],
            vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Int(9)],
        ]
    );
    let outer = sql
        .execute(&format!(
            "{ctes} SELECT l.k,l.w,r.v FROM l LEFT JOIN r
         ON l.v < r.v AND ((r.k = l.k) AND l.w = r.w)"
        ))
        .unwrap();
    assert_eq!(outer.rows.len(), 5);
    assert_eq!(
        outer
            .rows
            .iter()
            .filter(|row| row[2] == SqlValue::Null)
            .count(),
        3
    );
    // An OR cannot supply a necessary equality key: matches on the second
    // alternative must survive even when the first equality is false.
    let disjunction = sql
        .execute(
            "WITH l(k,w) AS (VALUES (1,1),(2,2)), r(k,w) AS (VALUES (1,2),(3,1))
         SELECT l.k,r.k FROM l JOIN r ON (l.k = r.k OR l.w = r.w)
         ORDER BY l.k,r.k",
        )
        .unwrap();
    assert_eq!(
        disjunction.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(1)],
            vec![SqlValue::Int(1), SqlValue::Int(3)],
            vec![SqlValue::Int(2), SqlValue::Int(1)],
        ]
    );
}

#[test]
fn hash_join_conjunction_falls_back_for_numeric_key_coercion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (left, right) in [
        ("CAST(1 AS INTEGER)", "CAST(1 AS NUMERIC(6,3))"),
        ("CAST(1 AS NUMERIC(6,2))", "CAST(1 AS INTEGER)"),
    ] {
        let result = sql
            .execute(&format!(
                "WITH l(k,w) AS (VALUES ({left},1)), r(k,w) AS (VALUES ({right},1))
             SELECT l.w,r.w FROM l JOIN r ON l.k = r.k AND l.w = r.w"
            ))
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(1), SqlValue::Int(1)]],
            "numeric equality must survive different representations: {left}, {right}"
        );
    }
}

#[test]
fn snapshot_count_does_not_materialize_resident_rows() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::EmbeddedMemory),
    )
    .unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE bounded_inventory (id INTEGER PRIMARY KEY, payload TEXT)")
            .unwrap();
        sql.execute(&format!(
            "INSERT INTO bounded_inventory SELECT i, '{}' FROM generate_series(1,1024) AS g(i)",
            "x".repeat(2048)
        ))
        .unwrap();
    }
    // Network SELECTs use this transaction-bearing shared-session path even
    // outside an explicit BEGIN. Exercise it instead of the embedded shortcut.
    let transaction = db.begin_transaction().unwrap();
    let mut session = SqlSession::new_shared(&db).with_pending_transaction(transaction);
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = session.execute("SELECT count(*) AS total FROM bounded_inventory");
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    assert_eq!(result.unwrap().rows, vec![vec![SqlValue::Int(1024)]]);
    assert_eq!(
        stats.rows_materialized, 0,
        "count must not deserialize table payloads"
    );
}
use serde_json::json;

fn embedded_text_column(name: &str, primary_key: bool) -> EmbeddedTableColumn {
    EmbeddedTableColumn {
        name: name.to_string(),
        pg_type: "text".to_string(),
        nullable: !primary_key,
        primary_key,
        vector_dimensions: None,
    }
}

#[test]
fn embedded_row_policy_enforces_correlated_select_and_candidate_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("patients").unwrap();
    db.create_collection("care_team").unwrap();
    ensure_embedded_table_schema(
        &mut db,
        "patients",
        &[
            embedded_text_column("id", true),
            embedded_text_column("org_id", false),
            embedded_text_column("name", false),
        ],
    )
    .unwrap();
    ensure_embedded_table_schema(
        &mut db,
        "care_team",
        &[
            embedded_text_column("id", true),
            embedded_text_column("org_id", false),
            embedded_text_column("patient_id", false),
            embedded_text_column("member_email", false),
            embedded_text_column("status", false),
            embedded_text_column("deleted_at", false),
        ],
    )
    .unwrap();
    SqlSession::new(&mut db)
        .execute(
            "INSERT INTO patients (id, org_id, name) VALUES
                ('p1', 'org-a', 'Ada'),
                ('p2', 'org-a', 'Bea'),
                ('p3', 'org-b', 'Cy');
             INSERT INTO care_team
                (id, org_id, patient_id, member_email, status, deleted_at)
             VALUES
                ('m1', 'org-a', 'p1', 'alice@example.test', 'active', NULL),
                ('m2', 'org-a', 'p2', 'bob@example.test', 'active', NULL),
                ('m3', 'org-b', 'p3', 'alice@example.test', 'active', NULL)",
        )
        .unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT p.id FROM patients AS p ORDER BY p.id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("p1".to_string())],
            vec![SqlValue::String("p2".to_string())],
            vec![SqlValue::String("p3".to_string())],
        ],
        "qualified projection must match PostgreSQL before RLS is enabled"
    );
    SqlSession::new(&mut db)
        .execute(
            "ALTER TABLE patients ENABLE ROW LEVEL SECURITY;
             CREATE POLICY outside_open ON patients FOR SELECT USING (TRUE)",
        )
        .unwrap();
    let prior = load_schema(&db, "patients").unwrap().unwrap();
    assert!(prior.rls_enabled);
    assert!(!prior.rls_forced);

    let tenant = "org_id::text = NULLIF(current_setting('carrier.current_tenant', true), '')";
    let external_role = "position(',external_practitioner,' in replace(',' || COALESCE(current_setting('carrier.current_roles', true), '') || ',', ',,', ',')) > 0";
    let admin_role = "position(',org_admin,' in replace(',' || COALESCE(current_setting('carrier.current_roles', true), '') || ',', ',,', ',')) > 0";
    reconcile_embedded_table_row_policy(
        &mut db,
        "care_team",
        Some(&EmbeddedTableRowPolicy {
            select_using: format!(
                "({tenant}) AND ({external_role}) AND member_email = COALESCE(current_setting('carrier.current_email', true), '')"
            ),
            insert_with_check: "FALSE".to_string(),
            update_using: "FALSE".to_string(),
            update_with_check: "FALSE".to_string(),
            delete_using: "FALSE".to_string(),
        }),
    )
    .unwrap();
    let patient_policy = EmbeddedTableRowPolicy {
        select_using: format!(
            "({tenant}) AND (({admin_role}) OR (({external_role}) AND EXISTS (SELECT 1 FROM care_team AS m WHERE m.patient_id = patients.id AND m.org_id = patients.org_id AND m.member_email = COALESCE(current_setting('carrier.current_email', true), '') AND m.status = 'active' AND m.deleted_at IS NULL)))"
        ),
        insert_with_check: format!("({tenant}) AND ({admin_role})"),
        update_using: format!("({tenant}) AND ({admin_role})"),
        update_with_check: format!("({tenant}) AND ({admin_role})"),
        delete_using: format!("({tenant}) AND ({admin_role})"),
    };
    reconcile_embedded_table_row_policy(&mut db, "patients", Some(&patient_policy)).unwrap();
    let installed = load_schema(&db, "patients").unwrap().unwrap();
    assert!(installed.rls_enabled);
    assert!(installed.rls_forced);
    assert!(installed
        .policies
        .iter()
        .any(|policy| policy.name == "outside_open"));
    assert!(installed
        .policies
        .iter()
        .any(|policy| policy.name == embedded_policy_state_name(true, false)));

    let external = SecurityContext::new("external-1", "org-a")
        .with_roles(["external_practitioner"])
        .with_policy_attributes(BTreeMap::from([(
            "email".to_string(),
            "alice@example.test".to_string(),
        )]));
    let mut session = SqlSession::new_secure(&mut db, external);
    assert_eq!(
        session
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT p.id FROM patients AS p")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]],
        "a correlated compiler policy must remain valid when the protected relation is aliased"
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('carrier.current_email', true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("alice@example.test".to_string())]]
    );
    assert!(matches!(
        session.execute("SET carrier.current_email = 'attacker@example.test'"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));

    drop(session);
    let admin = SecurityContext::new("admin-1", "org-a").with_roles(["org_admin"]);
    let mut session = SqlSession::new_secure(&mut db, admin);
    assert!(matches!(
        session.execute("INSERT INTO patients (id, org_id, name) VALUES ('p4', 'org-b', 'Wrong')"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    session
        .execute("INSERT INTO patients (id, org_id, name) VALUES ('p4', 'org-a', 'Dee')")
        .unwrap();
    assert!(matches!(
        session.execute("UPDATE patients SET org_id = 'org-b' WHERE id = 'p4'"),
        Err(SqlError::BicDb(BicDbError::Authorization(_)))
    ));
    assert_eq!(
        session
            .execute("DELETE FROM patients WHERE id = 'p4'")
            .unwrap()
            .command_tag,
        Some("DELETE 1".to_string())
    );

    drop(session);
    let removal = reconcile_embedded_table_row_policy(&mut db, "patients", None).unwrap();
    let removed = load_schema(&db, "patients").unwrap().unwrap();
    assert!(removed.rls_enabled);
    assert!(!removed.rls_forced);
    assert_eq!(
        removed
            .policies
            .iter()
            .map(|policy| policy.name.as_str())
            .collect::<Vec<_>>(),
        vec!["outside_open"]
    );
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows
            .len(),
        3
    );
    removal.rollback(&mut db).unwrap();
    let external = SecurityContext::new("external-1", "org-a")
        .with_roles(["external_practitioner"])
        .with_policy_attributes(BTreeMap::from([(
            "email".to_string(),
            "alice@example.test".to_string(),
        )]));
    assert_eq!(
        SqlSession::new_secure(&mut db, external)
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );
}

#[test]
fn create_table_classifies_explicit_nextval_defaults_for_bulk_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE SEQUENCE bulk_default_seq;
             CREATE TABLE bulk_default_rows (
                 id text PRIMARY KEY,
                 sequence_value bigint
                     DEFAULT pg_catalog.nextval('bulk_default_seq')
             )",
        )
        .unwrap();

    let schema = load_schema(&db, "bulk_default_rows").unwrap().unwrap();
    let column = schema.column("sequence_value").unwrap();
    assert_eq!(column.default_sequence.as_deref(), Some("bulk_default_seq"));
    assert_eq!(column.default_expr, None);
}

#[test]
fn array_concatenation_projects_the_array_type() {
    let statements = Parser::parse_sql(
        &PostgreSqlDialect {},
        "SELECT ARRAY[1,2] || ARRAY[3,4], 0 || ARRAY[1,2]",
    )
    .unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected SELECT query")
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT body")
    };
    for item in &select.projection {
        let SelectItem::UnnamedExpr(expr) = item else {
            panic!("expected expression")
        };
        assert_eq!(
            projected_expr_pg_type(expr, None).as_deref(),
            Some("int4[]"),
            "{expr:?}"
        );
    }

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let result = SqlSession::new(&mut db)
        .execute("SELECT ARRAY[1,2] || ARRAY[3,4], 0 || ARRAY[1,2]")
        .unwrap();
    assert_eq!(
        result.column_types,
        vec![Some("int4[]".to_string()), Some("int4[]".to_string())]
    );
    assert_eq!(
        infer_query_result_types(&db, "SELECT ARRAY[1,2] || ARRAY[3,4], 0 || ARRAY[1,2]").unwrap(),
        Some(vec![Some("int4[]".to_string()), Some("int4[]".to_string())])
    );
}

#[test]
fn update_array_assignment_rewrite_preserves_other_assignments_and_literals() {
    let sql = "UPDATE array_rows SET \"values\"[0:1] = ARRAY[8,9], note = 'where, from, returning' WHERE id = 1";
    let rewritten = rewrite_postgres_parse_compat(sql).expect("array target must be lowered");
    assert!(rewritten.contains("\"values\" = bicdb_array_assign(\"values\""));
    assert!(rewritten.contains("note = 'where, from, returning'"));
    assert!(rewritten.ends_with("WHERE id = 1"));
    Parser::parse_sql(&PostgreSqlDialect {}, &rewritten).unwrap();
}

#[test]
fn current_date_uses_transaction_start_and_session_timezone() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("BEGIN").unwrap();
    session.transaction_timestamp_seconds = Some(0);
    session
        .execute("SET TIME ZONE 'America/Los_Angeles'")
        .unwrap();
    let pacific = session
        .execute("SELECT CURRENT_DATE, now(), transaction_timestamp()")
        .unwrap();
    assert_eq!(
        pacific.rows,
        vec![vec![
            SqlValue::String("1969-12-31".to_string()),
            SqlValue::String("1969-12-31 16:00:00-08".to_string()),
            SqlValue::String("1969-12-31 16:00:00-08".to_string()),
        ]]
    );

    session.execute("SET TIME ZONE 'Asia/Tokyo'").unwrap();
    let tokyo = session
        .execute("SELECT CURRENT_DATE, now(), transaction_timestamp()")
        .unwrap();
    assert_eq!(
        tokyo.rows,
        vec![vec![
            SqlValue::String("1970-01-01".to_string()),
            SqlValue::String("1970-01-01 09:00:00+09".to_string()),
            SqlValue::String("1970-01-01 09:00:00+09".to_string()),
        ]]
    );
    session.execute("ROLLBACK").unwrap();

    let error = session
        .execute("SET TIME ZONE 'Mars/Olympus_Mons'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
}

#[test]
fn legacy_sequence_metadata_defaults_to_bigint_without_rewriting() {
    let sequence: SequenceSchema = serde_json::from_value(json!({
        "name": "legacy_seq",
        "increment_by": 1,
        "min_value": 1,
        "max_value": 9223372036854775807i64,
        "start_value": 1,
        "cache_size": 1,
        "cycle": false,
        "last_value": 4,
        "is_called": true,
        "owner": "bicdb"
    }))
    .unwrap();

    assert_eq!(sequence.data_type, "int8");
    assert_eq!(sequence.last_value, 4);
}

#[test]
fn result_columns_preserve_postgres_row_description_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE row_description_probe (
                id INTEGER PRIMARY KEY,
                amount NUMERIC(10, 2),
                label VARCHAR(12)
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO row_description_probe (id, amount, label)
             VALUES (1, 12.34, 'ready')",
        )
        .unwrap();

    let result = session
        .execute("SELECT id, amount AS total, label, id + 1 AS computed FROM row_description_probe")
        .unwrap();

    assert_eq!(result.column_metadata.len(), 4);
    let table_oid = result.column_metadata[0].table_oid;
    assert!(table_oid > 0);
    assert_eq!(
        result.column_metadata,
        vec![
            SqlColumnMetadata {
                table_oid,
                attribute_number: 1,
                type_modifier: -1,
            },
            SqlColumnMetadata {
                table_oid,
                attribute_number: 2,
                type_modifier: ((10_i32 << 16) | 2) + 4,
            },
            SqlColumnMetadata {
                table_oid,
                attribute_number: 3,
                type_modifier: 16,
            },
            SqlColumnMetadata::default(),
        ]
    );
}

#[test]
fn postgres_type_error_helpers_have_stable_sqlstates_and_fields() {
    let cases = [
        (
            SqlError::invalid_text_representation("uuid", "bad value"),
            "22P02",
        ),
        (
            SqlError::numeric_value_out_of_range("numeric field overflow"),
            "22003",
        ),
        (
            SqlError::string_data_right_truncation("value too long"),
            "22001",
        ),
        (SqlError::invalid_datetime_format("invalid date"), "22007"),
        (
            SqlError::invalid_parameter_value("invalid precision"),
            "22023",
        ),
        (SqlError::undefined_type("missing_type"), "42704"),
    ];
    for (error, sqlstate) in cases {
        assert_eq!(error.sqlstate(), sqlstate);
    }

    assert_eq!(
        SqlError::invalid_text_representation("uuid", "bad value").fields(),
        vec![SqlErrorField::DataType("uuid".to_string())]
    );
}

#[test]
fn routine_statement_profile_kinds_follow_generic_ast_forms() {
    let cases = [
        ("local_value := 1", "ROUTINE_ASSIGNMENT"),
        (
            "SELECT answer_value INTO local_value FROM arbitrary_rows",
            "ROUTINE_SELECT_INTO",
        ),
        (
            "UPDATE arbitrary_rows SET answer_value = answer_value + 1 WHERE row_key = local_value",
            "ROUTINE_SQL_UPDATE",
        ),
        (
            "WITH changed_rows AS (
                    DELETE FROM arbitrary_rows
                    WHERE row_key = local_value
                    RETURNING row_key
                 )
                 SELECT array_agg(row_key) FROM changed_rows INTO local_keys",
            "ROUTINE_SELECT_INTO",
        ),
        (
            "IF local_value > 0 THEN local_value := local_value - 1; ELSE local_value := 0; END IF",
            "ROUTINE_IF",
        ),
        (
            "FOR loop_value IN 1 .. 3 LOOP local_value := loop_value; END LOOP",
            "ROUTINE_FOR_LOOP",
        ),
        ("OPEN arbitrary_cursor", "ROUTINE_OPEN_CURSOR"),
        (
            "FETCH arbitrary_cursor INTO local_value",
            "ROUTINE_FETCH_CURSOR",
        ),
        ("CLOSE arbitrary_cursor", "ROUTINE_CLOSE_CURSOR"),
        ("RETURN local_value", "ROUTINE_RETURN"),
    ];

    for (source, expected) in cases {
        let statement = parse_routine_statement(source).unwrap();
        assert_eq!(routine_statement_profile_kind(&statement), expected);
    }
}

#[test]
fn routine_statement_profile_signature_avoids_nested_body_debug_dump() {
    let body = (0..512)
        .map(|value| format!("local_value := local_value + {value};"))
        .collect::<Vec<_>>()
        .join(" ");
    let statement = parse_routine_statement(&format!(
        "IF local_value > 0 THEN {body} ELSE local_value := 0; END IF"
    ))
    .unwrap();

    let debug_len = format!("{statement:?}").len();
    let signature = routine_statement_profile_signature(&statement);

    assert!(debug_len > 10_000);
    assert_eq!(signature, "IF local_value > 0");
}

#[test]
fn routine_ir_cache_reuses_unchanged_definition_and_recompiles_changes() {
    ROUTINE_IR_CACHE.with(|cache| cache.borrow_mut().clear());
    let routine = RoutineSchema {
        name: "arbitrary_cached_routine".to_string(),
        schema: "public".to_string(),
        kind: RoutineKind::Function,
        args: vec!["input_value INTEGER".to_string()],
        arg_types: Vec::new(),
        return_type: "integer".to_string(),
        return_type_modifier: None,
        return_type_declaration: None,
        returns_set: false,
        language: "plpgsql".to_string(),
        definition: "CREATE FUNCTION arbitrary_cached_routine(input_value INTEGER)
                         RETURNS INTEGER
                         AS $$
                         BEGIN
                             RETURN input_value + 1;
                         END;
                         $$
                         LANGUAGE plpgsql"
            .to_string(),
        internal_symbol: None,
        owner: Some(BOOTSTRAP_ROLE_NAME.to_string()),
        security_definer: false,
    };

    let first = compile_cached_routine_ir(&routine).unwrap();
    let second = compile_cached_routine_ir(&routine).unwrap();
    let mut changed = routine.clone();
    changed.definition = "CREATE FUNCTION arbitrary_cached_routine(input_value INTEGER)
                              RETURNS INTEGER
                              AS $$
                              BEGIN
                                  RETURN input_value + 2;
                              END;
                              $$
                              LANGUAGE plpgsql"
        .to_string();
    let third = compile_cached_routine_ir(&changed).unwrap();

    assert!(Arc::ptr_eq(&first, &second));
    assert!(!Arc::ptr_eq(&first, &third));
}

#[test]
fn routine_ir_binds_scalar_expressions_to_var_ids() {
    let routine = RoutineSchema {
        schema: "public".to_string(),
        name: "arbitrary_bound_scalar_routine".to_string(),
        kind: RoutineKind::Function,
        args: vec![
            "input_value INTEGER".to_string(),
            "output_value OUT INTEGER".to_string(),
        ],
        arg_types: Vec::new(),
        return_type: "integer".to_string(),
        return_type_modifier: None,
        return_type_declaration: None,
        returns_set: false,
        language: "plpgsql".to_string(),
        definition: "CREATE FUNCTION arbitrary_bound_scalar_routine(input_value INTEGER)
                         RETURNS INTEGER
                         AS $$
                         DECLARE
                             working_value INTEGER DEFAULT input_value + 1;
                         BEGIN
                             working_value := working_value + 2;
                             IF working_value > input_value THEN
                                 output_value := working_value;
                             END IF;
                             RETURN output_value;
                         END;
                         $$
                         LANGUAGE plpgsql"
            .to_string(),
        internal_symbol: None,
        owner: Some(BOOTSTRAP_ROLE_NAME.to_string()),
        security_definer: false,
    };

    let ir = compile_routine_ir(&routine).unwrap();

    assert_eq!(
        ir.symbol_names,
        vec!["$1", "input_value", "$2", "output_value", "working_value"]
    );
    let RoutineDecl::Variable {
        default_expr: Some(default_expr),
        ..
    } = &ir.declarations[0]
    else {
        panic!("expected variable declaration with default expression");
    };
    assert!(matches!(
        &default_expr.bound,
        Some(BoundExpr::Binary {
            left,
            right,
            ..
        }) if matches!(left.as_ref(), BoundExpr::Var(VarId(1)))
            && matches!(right.as_ref(), BoundExpr::Literal(SqlValue::Int(1)))
    ));

    let RoutineStmt::Assignment { expr, .. } = &ir.statements[0] else {
        panic!("expected assignment statement");
    };
    assert!(matches!(
        &expr.bound,
        Some(BoundExpr::Binary { left, .. })
            if matches!(left.as_ref(), BoundExpr::Var(VarId(4)))
    ));

    let RoutineStmt::If { condition, .. } = &ir.statements[1] else {
        panic!("expected IF statement");
    };
    assert!(matches!(
        &condition.bound,
        Some(BoundExpr::Compare { left, right, .. })
            if matches!(left.as_ref(), BoundExpr::Var(VarId(4)))
                && matches!(right.as_ref(), BoundExpr::Var(VarId(1)))
    ));

    let RoutineStmt::Return(Some(expr)) = &ir.statements[2] else {
        panic!("expected RETURN statement");
    };
    assert!(matches!(&expr.bound, Some(BoundExpr::Var(VarId(3)))));
}

#[test]
fn routine_array_assignment_target_is_compiled_once() {
    ROUTINE_IR_CACHE.with(|cache| cache.borrow_mut().clear());
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"
                CREATE PROCEDURE arbitrary_array_target_compile_probe(
                    loop_limit IN INTEGER,
                    total_value OUT INTEGER
                )
                LANGUAGE 'plpgsql'
                AS $$
                DECLARE
                    flexible_values INT[];
                BEGIN
                    total_value := 0;
                    FOR loop_index IN 1 .. loop_limit
                    LOOP
                        flexible_values[loop_index] := loop_index + 10;
                    END LOOP;

                    SELECT sum(item_value)
                    FROM UNNEST(flexible_values) AS arbitrary_items(item_value)
                    INTO total_value;
                END;
                $$
                "#,
        )
        .unwrap();

    session
        .execute("CALL arbitrary_array_target_compile_probe(3)")
        .unwrap();
    reset_sql_parse_statement_calls();
    let result = session
        .execute("CALL arbitrary_array_target_compile_probe(3)")
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(36)]]);
    assert_eq!(
        sql_parse_statement_calls(),
        0,
        "cached routine execution parses nothing: the outer CALL's template is cached by the \
         statement cache and array assignment targets inside the VM are compiled once"
    );
}

#[test]
fn routine_frame_array_element_assignment_preserves_generic_array_semantics() {
    let mut frame = RoutineFrame::new(&[], &[]).unwrap();

    frame
        .set_array_element("flexible_values", 1, SqlValue::Int(7))
        .unwrap();
    assert_eq!(
        frame.get("flexible_values"),
        SqlValue::Json(json!([null, 7]))
    );

    frame
        .set_array_element("flexible_values", 0, SqlValue::String("left".to_string()))
        .unwrap();
    assert_eq!(
        frame.get("flexible_values"),
        SqlValue::Json(json!(["left", 7]))
    );
}

#[test]
fn routine_into_transfers_text_and_array_allocations_to_slots() {
    let targets = vec!["result_text".to_string(), "result_array".to_string()];
    let mut frame = RoutineFrame::new_with_symbols(&[], &[], &targets).unwrap();
    let text = "owned text from an SQL result".to_string();
    let array = vec![json!({"nested": [1, 2, "payload"]}), json!(42)];
    let text_pointer = text.as_ptr();
    let array_pointer = array.as_ptr();
    let row = vec![
        SqlValue::String(text),
        SqlValue::Json(JsonValue::Array(array)),
    ];
    assign_routine_targets_with_columns(&mut frame, &targets, &[], Some(row)).unwrap();
    let stored_text = frame
        .slot_values()
        .iter()
        .find_map(|value| match value {
            SqlValue::String(text) => Some(text),
            _ => None,
        })
        .unwrap();
    let stored_array = frame
        .slot_values()
        .iter()
        .find_map(|value| match value {
            SqlValue::Json(JsonValue::Array(array)) => Some(array),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        stored_text.as_ptr(),
        text_pointer,
        "INTO must move SQL result text"
    );
    assert_eq!(
        stored_array.as_ptr(),
        array_pointer,
        "INTO must move SQL result arrays"
    );
}

#[test]
fn routine_frame_slot_array_assignment_mutates_without_cloning_existing_array() {
    let mut frame =
        RoutineFrame::new_with_symbols(&[], &[], &["flexible_values".to_string()]).unwrap();

    reset_sql_routine_slot_array_clones();
    for idx in 0..64 {
        frame
            .set_array_element("flexible_values", idx, SqlValue::Int(idx as i64))
            .unwrap();
    }

    assert_eq!(
        sql_routine_slot_array_clones(),
        0,
        "slot-backed routine array element assignment should mutate the frame slot in place"
    );
    assert_eq!(
        frame.get("flexible_values"),
        SqlValue::Json(JsonValue::Array(
            (0..64)
                .map(|idx| JsonValue::Number(serde_json::Number::from(idx)))
                .collect()
        ))
    );
}

#[test]
fn routine_frame_array_element_assignment_converts_pg_integer_vectors() {
    let mut frame = RoutineFrame::new(&[], &[]).unwrap();
    frame.set("arbitrary_vector", SqlValue::String("3 4 5".to_string()));

    frame
        .set_array_element("arbitrary_vector", 1, SqlValue::Int(9))
        .unwrap();

    assert_eq!(
        frame.get("arbitrary_vector"),
        SqlValue::Json(json!([3, 9, 5]))
    );
}

#[test]
fn routine_frame_array_element_assignment_rejects_non_array_variables() {
    let mut frame = RoutineFrame::new(&[], &[]).unwrap();
    frame.set("plain_value", SqlValue::Bool(true));

    let err = frame
        .set_array_element("plain_value", 0, SqlValue::Int(1))
        .unwrap_err();

    assert!(format!("{err}").contains("cannot assign array element on non-array"));
}

#[test]
fn routine_frame_resolves_arbitrary_mixed_case_symbols_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE PROCEDURE arbitrary_case_frame_probe(
                    MixedInput IN INTEGER,
                    MixedOutput INOUT INTEGER
                )
                AS $$
                DECLARE
                    MixedLocal INTEGER;
                BEGIN
                    MixedLocal := MixedInput + 5;
                    SELECT MixedLocal + $1 INTO MixedOutput;
                END;
                $$
                LANGUAGE 'plpgsql'",
        )
        .unwrap();

    let result = session
        .execute("CALL arbitrary_case_frame_probe(7, 0)")
        .unwrap();

    assert_eq!(result.columns, vec!["mixedoutput"]);
    assert_eq!(result.rows, vec![vec![SqlValue::Int(19)]]);
}

#[test]
fn routine_slot_bound_assignments_avoid_string_map_syncs_until_sql_needs_vars() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE PROCEDURE arbitrary_slot_frame_probe(
                    arbitrary_input IN INTEGER,
                    arbitrary_output INOUT INTEGER
                )
                AS $$
                DECLARE
                    arbitrary_total INTEGER;
                    arbitrary_step INTEGER;
                BEGIN
                    arbitrary_total := arbitrary_input;
                    FOR arbitrary_step IN 1 .. 25
                    LOOP
                        arbitrary_total := arbitrary_total + arbitrary_step;
                    END LOOP;
                    arbitrary_output := arbitrary_total;
                END;
                $$
                LANGUAGE 'plpgsql'",
        )
        .unwrap();

    reset_sql_routine_frame_map_syncs();
    let result = session
        .execute("CALL arbitrary_slot_frame_probe(7, 0)")
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(332)]]);
    assert_eq!(
        sql_routine_frame_map_syncs(),
        0,
        "slot-bound routine assignment loops should not sync the string-keyed variable map"
    );
}

#[test]
fn primary_key_unique_validation_uses_record_id_lookup_without_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE pk_validation_fast (id INT PRIMARY KEY, label TEXT)")
            .unwrap();
        session
            .execute("INSERT INTO pk_validation_fast (id, label) VALUES (1, 'existing')")
            .unwrap();
    }
    let schema = load_schema(&db, "pk_validation_fast").unwrap().unwrap();
    let record = record_from_fields(
        "pk_validation_fast",
        Some(&schema),
        BTreeMap::from([
            ("id".to_string(), SqlValue::Int(2)),
            ("label".to_string(), SqlValue::String("new".to_string())),
        ]),
    )
    .unwrap();

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    validate_records_for_write(
        &db,
        None,
        "pk_validation_fast",
        &schema,
        std::slice::from_ref(&record),
        false,
    )
    .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(stats.full_scan_count, 0);
}

#[test]
fn primary_key_upserts_use_record_id_lookups_without_full_scans() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE pk_upsert_fast (id TEXT PRIMARY KEY, label TEXT);
             INSERT INTO pk_upsert_fast VALUES ('existing', 'before');
             BEGIN",
        )
        .unwrap();

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    session
        .execute(
            "INSERT INTO pk_upsert_fast VALUES ('new-a', 'a'), ('new-b', 'b')
             ON CONFLICT (id) DO UPDATE SET label = excluded.label",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO pk_upsert_fast VALUES ('existing', 'after'), ('new-a', 'updated')
             ON CONFLICT (id) DO UPDATE SET label = excluded.label",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    session.execute("COMMIT").unwrap();

    let result = session
        .execute("SELECT id, label FROM pk_upsert_fast ORDER BY id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("existing".into()),
                SqlValue::String("after".into())
            ],
            vec![
                SqlValue::String("new-a".into()),
                SqlValue::String("updated".into())
            ],
            vec![
                SqlValue::String("new-b".into()),
                SqlValue::String("b".into())
            ],
        ]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count >= 4);
}

#[test]
fn non_primary_unique_upserts_use_index_without_full_scans_and_see_pending_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE unique_upsert_fast (
                 id UUID PRIMARY KEY,
                 scan_id UUID UNIQUE,
                 payload TEXT
             );
             INSERT INTO unique_upsert_fast VALUES (
                 '00000000-0000-0000-0000-000000000001',
                 '10000000-0000-0000-0000-000000000001',
                 'before'
             );
             BEGIN",
        )
        .unwrap();

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    session
        .execute(
            "INSERT INTO unique_upsert_fast VALUES (
                 '00000000-0000-0000-0000-000000000002',
                 '10000000-0000-0000-0000-000000000002',
                 'pending-before'
             ) ON CONFLICT (scan_id) DO UPDATE SET payload = excluded.payload",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO unique_upsert_fast VALUES (
                 '00000000-0000-0000-0000-000000000003',
                 '10000000-0000-0000-0000-000000000001',
                 'after-committed'
             ) ON CONFLICT (scan_id) DO UPDATE SET payload = excluded.payload",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO unique_upsert_fast VALUES (
                 '00000000-0000-0000-0000-000000000004',
                 '10000000-0000-0000-0000-000000000002',
                 'after-pending'
             ) ON CONFLICT (scan_id) DO UPDATE SET payload = excluded.payload",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    session.execute("COMMIT").unwrap();

    let result = session
        .execute("SELECT payload FROM unique_upsert_fast ORDER BY payload")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("after-committed".into())],
            vec![SqlValue::String("after-pending".into())],
        ]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count >= 3);
}

#[test]
fn row_scans_reuse_loaded_schema_for_rls_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_schema_cached_scan (
                        row_id INT PRIMARY KEY,
                        payload TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_schema_cached_scan (row_id, payload)
                     VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let records = SqlEngine::new(&db)
        .scan_records("arbitrary_schema_cached_scan")
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(records.len(), 4);
    assert_eq!(stats.schema_loads, 1);
}

#[test]
fn routine_statements_reuse_schema_cache_across_nested_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_routine_schema_cache_rows (
                        row_key INT PRIMARY KEY,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_routine_schema_cache_rows (row_key, payload_value)
                     VALUES (7, 10)",
            )
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE arbitrary_routine_schema_cache_probe(
                        selected_key IN INTEGER,
                        observed_value INOUT INTEGER
                    )
                    AS $$
                    BEGIN
                        SELECT payload_value
                        INTO observed_value
                        FROM arbitrary_routine_schema_cache_rows
                        WHERE row_key = selected_key;

                        UPDATE arbitrary_routine_schema_cache_rows
                        SET payload_value = payload_value + 5
                        WHERE row_key = selected_key;

                        SELECT payload_value
                        INTO observed_value
                        FROM arbitrary_routine_schema_cache_rows
                        WHERE row_key = selected_key;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_routine_schema_cache_probe(7, 0)")
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(15)]]);
    assert!(
        stats.schema_loads <= 2,
        "routine statement execution should cache table schema loads within the outer CALL, got {}",
        stats.schema_loads
    );
}

#[test]
fn session_reuses_schema_cache_across_repeated_calls_and_invalidates_after_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute(
                "CREATE TABLE arbitrary_session_schema_cache_rows (
                        row_key INT PRIMARY KEY,
                        payload_value INT
                    )",
            )
            .unwrap();
        setup
            .execute(
                "INSERT INTO arbitrary_session_schema_cache_rows (row_key, payload_value)
                     VALUES (3, 11)",
            )
            .unwrap();
        setup
            .execute(
                "CREATE PROCEDURE arbitrary_session_schema_cache_probe(
                        selected_key IN INTEGER,
                        observed_value INOUT INTEGER
                    )
                    AS $$
                    BEGIN
                        SELECT payload_value
                        INTO observed_value
                        FROM arbitrary_session_schema_cache_rows
                        WHERE row_key = selected_key;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let first = session
        .execute("CALL arbitrary_session_schema_cache_probe(3, 0)")
        .unwrap();
    let first_stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    assert_eq!(first.rows, vec![vec![SqlValue::Int(11)]]);
    assert!(
        first_stats.schema_loads > 0,
        "first call should populate the session schema cache"
    );

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let second = session
        .execute("CALL arbitrary_session_schema_cache_probe(3, 0)")
        .unwrap();
    let second_stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    assert_eq!(second.rows, vec![vec![SqlValue::Int(11)]]);
    assert_eq!(
        second_stats.schema_loads, 0,
        "stable schemas should stay cached across repeated statements in one session"
    );

    session
        .execute("ALTER TABLE arbitrary_session_schema_cache_rows ADD COLUMN extra_value INT")
        .unwrap();
    session
        .execute(
            "INSERT INTO arbitrary_session_schema_cache_rows
                 (row_key, payload_value, extra_value)
                 VALUES (4, 12, 99)",
        )
        .unwrap();
    let after_ddl = session
        .execute(
            "SELECT extra_value
                 FROM arbitrary_session_schema_cache_rows
                 WHERE row_key = 4",
        )
        .unwrap();
    assert_eq!(after_ddl.rows, vec![vec![SqlValue::Int(99)]]);
}

#[test]
fn schema_list_cache_reuses_raw_loads_and_invalidates_after_schema_change() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_schema_list_alpha (
                        row_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_schema_list_beta (
                        row_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
    }

    reset_sql_schema_list_raw_loads();
    let _scope = SqlSchemaCacheScope::new();
    assert_eq!(list_schemas(&db).unwrap().len(), 2);
    assert_eq!(list_schemas(&db).unwrap().len(), 2);
    assert_eq!(
        sql_schema_list_raw_loads(),
        1,
        "schema list should deserialize once within a cache scope"
    );

    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_schema_list_gamma (
                        row_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
    }

    let schemas = list_schemas(&db).unwrap();
    assert!(
        schemas
            .iter()
            .any(|schema| schema.name == "arbitrary_schema_list_gamma"),
        "schema-list cache must be invalidated by schema writes"
    );
    assert_eq!(
        sql_schema_list_raw_loads(),
        2,
        "schema change should force one fresh raw schema-list load"
    );
}

#[test]
fn row_query_table_factor_uses_index_for_routine_bound_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_bound_lookup_rows (
                        row_id INT PRIMARY KEY,
                        lookup_key INT,
                        score INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_bound_lookup_idx
                     ON arbitrary_bound_lookup_rows (lookup_key)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_bound_lookup_rows (row_id, lookup_key, score)
                     VALUES (1, 1, 50), (2, 2, 40), (3, 3, 30), (4, 3, 20)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .with_routine_vars(BTreeMap::from([(
            "outer_lookup".to_string(),
            SqlValue::Int(3),
        )]))
        .execute(
            "SELECT MIN(row_source.score)
                 FROM arbitrary_bound_lookup_rows AS row_source
                 WHERE row_source.lookup_key = outer_lookup",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(20)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(stats.rows_materialized <= 2);
}

#[test]
fn single_part_row_lookup_avoids_compound_key_build() {
    let mut row = SqlRow::default();
    row.insert("customer_id".to_string(), SqlValue::Int(11));
    row.insert("c_id".to_string(), SqlValue::Int(7));
    row.insert("arbitrary_alias.c_id".to_string(), SqlValue::Int(13));

    reset_sql_row_lookup_compound_joins();
    assert_eq!(
        row_value_from_parts_opt(&row, &[String::from("c_id")]),
        Some(SqlValue::Int(7))
    );
    assert_eq!(sql_row_lookup_compound_joins(), 0);

    assert_eq!(
        row_value_from_parts_opt(
            &row,
            &[String::from("arbitrary_alias"), String::from("c_id")]
        ),
        Some(SqlValue::Int(13))
    );
    assert_eq!(sql_row_lookup_compound_joins(), 1);
}

#[test]
fn bound_expr_binds_columns_and_vars_to_stable_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    let columns = vec![
        "source_alpha.metric_value".to_string(),
        "source_alpha.offset_value".to_string(),
    ];
    let vars = vec!["threshold_value".to_string()];
    let scope = BoundExprScope::new(&columns, &vars);
    let expr = scope
        .bind(
            &parse_routine_expr("source_alpha.metric_value + threshold_value - offset_value")
                .unwrap(),
        )
        .unwrap();

    let value = expr
        .eval(&BoundExprFrame {
            user_calls: &[],
            db: &db,
            columns: BoundExprColumns::Values(&[SqlValue::Int(10), SqlValue::Int(3)]),
            vars: &[SqlValue::Int(7)],
        })
        .unwrap();

    assert_eq!(value, SqlValue::Int(14));
    match expr {
        BoundExpr::Binary { left, .. } => match *left {
            BoundExpr::Binary { left, right, .. } => {
                assert!(matches!(*left, BoundExpr::Column(ColumnId(0))));
                assert!(matches!(*right, BoundExpr::Var(VarId(0))));
            }
            other => panic!("expected nested binary expression, got {other:?}"),
        },
        other => panic!("expected binary expression, got {other:?}"),
    }
}

#[test]
fn bound_expr_rejects_ambiguous_unqualified_columns() {
    let columns = vec![
        "left_alias.shared_value".to_string(),
        "right_alias.shared_value".to_string(),
    ];
    let scope = BoundExprScope::new(&columns, &[]);

    assert!(scope
        .bind(&parse_routine_expr("shared_value").unwrap())
        .is_none());
    assert!(matches!(
        scope.bind(&parse_routine_expr("right_alias.shared_value").unwrap()),
        Some(BoundExpr::Column(ColumnId(1)))
    ));
}

#[test]
fn bound_expr_rejects_binary_operators_without_bound_semantics() {
    let columns = vec!["items.embedding_value".to_string()];
    let scope = BoundExprScope::new(&columns, &[]);

    assert!(scope
        .bind(&parse_routine_expr("items.embedding_value <=> '[1,2,3]'").unwrap())
        .is_none());
}

#[test]
fn bound_expr_truth_uses_positional_values_after_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    let columns = vec!["arbitrary_row.quantity_value".to_string()];
    let vars = vec!["limit_value".to_string()];
    let scope = BoundExprScope::new(&columns, &vars);
    let expr = scope
        .bind(
            &parse_routine_expr(
                "arbitrary_row.quantity_value < limit_value AND limit_value IS NOT NULL",
            )
            .unwrap(),
        )
        .unwrap();

    let matched = expr
        .eval_truth(&BoundExprFrame {
            user_calls: &[],
            db: &db,
            columns: BoundExprColumns::Values(&[SqlValue::Int(9)]),
            vars: &[SqlValue::Int(10)],
        })
        .unwrap();

    assert_eq!(matched, Some(true));
}

#[test]
fn row_query_binds_selection_projection_and_order_expressions_to_column_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_bound_rows (
                        scope_key INT,
                        row_key INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, row_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_bound_rows
                     (scope_key, row_key, payload_value)
                     VALUES (7, 1, 40), (7, 2, 30), (8, 1, 99)",
            )
            .unwrap();
    }

    reset_sql_row_lookup_compound_joins();
    reset_sql_row_from_record_calls();
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "selected_scope".to_string(),
            SqlValue::Int(7),
        )]));
        session
            .execute(
                "SELECT arbitrary_bound_rows.payload_value + 1 AS shifted_value
                     FROM arbitrary_bound_rows AS chosen_row
                     WHERE chosen_row.scope_key = selected_scope
                       AND arbitrary_bound_rows.payload_value >= 30
                     ORDER BY chosen_row.payload_value",
            )
            .unwrap()
    };

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(31)], vec![SqlValue::Int(41)]]
    );
    assert_eq!(
            sql_row_lookup_compound_joins(),
            0,
            "bound row expressions should not resolve qualified identifiers through the name lookup hot path"
        );
    assert_eq!(
            sql_row_from_record_calls(),
            0,
            "row-set execution should materialize table records directly into slot rows, not per-row name maps"
        );
}

#[test]
fn row_query_fallback_expressions_read_slot_rows_without_name_maps() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_slot_eval_rows (
                        row_key INT PRIMARY KEY,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_slot_eval_rows (row_key, payload_value)
                     VALUES (1, 5), (2, 20), (3, 30), (4, 100)",
            )
            .unwrap();
    }

    reset_sql_slot_row_to_map_calls();
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "SELECT
                        CASE
                            WHEN slot_alias.payload_value > 10
                            THEN slot_alias.payload_value
                            ELSE 0
                        END AS display_value
                     FROM arbitrary_slot_eval_rows AS slot_alias
                     WHERE slot_alias.row_key IN (1, 2, 3)
                     ORDER BY
                        CASE
                            WHEN slot_alias.row_key = 2 THEN 0
                            WHEN slot_alias.row_key = 1 THEN 1
                            ELSE 2
                        END",
            )
            .unwrap()
    };

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(20)],
            vec![SqlValue::Int(0)],
            vec![SqlValue::Int(30)]
        ]
    );
    assert_eq!(
            sql_slot_row_to_map_calls(),
            0,
            "fallback row expressions should read slot rows through the plan lookup instead of building per-row name maps"
        );
}

#[test]
fn row_aggregates_bind_arguments_to_column_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_bound_metric_rows (
                        scope_key INT,
                        row_key INT,
                        metric_value INT,
                        PRIMARY KEY (scope_key, row_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_bound_metric_rows
                     (scope_key, row_key, metric_value)
                     VALUES (3, 1, 5), (3, 2, 7), (4, 1, 100)",
            )
            .unwrap();
    }

    reset_sql_row_lookup_compound_joins();
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "wanted_scope".to_string(),
            SqlValue::Int(3),
        )]));
        session
                .execute(
                    "SELECT COUNT(metric_row.metric_value), SUM(arbitrary_bound_metric_rows.metric_value)
                     FROM arbitrary_bound_metric_rows AS metric_row
                     WHERE metric_row.scope_key = wanted_scope",
                )
                .unwrap()
    };

    assert_eq!(result.rows, vec![vec![SqlValue::Int(2), SqlValue::Int(12)]]);
    assert_eq!(
            sql_row_lookup_compound_joins(),
            0,
            "bound row aggregates should not resolve qualified arguments through the name lookup hot path"
        );
}

#[test]
fn row_aggregates_apply_distinct_from_parsed_function_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_distinct_metric_rows (
                        scope_key INT,
                        row_key INT,
                        item_key INT,
                        metric_value INT,
                        PRIMARY KEY (scope_key, row_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_distinct_metric_rows
                     (scope_key, row_key, item_key, metric_value)
                     VALUES
                     (5, 1, 11, 7),
                     (5, 2, 11, 7),
                     (5, 3, 12, 9),
                     (5, 4, NULL, 100),
                     (6, 1, 99, 1000)",
            )
            .unwrap();
    }

    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "wanted_scope".to_string(),
            SqlValue::Int(5),
        )]));
        session
            .execute(
                "SELECT COUNT(DISTINCT metric_row.item_key),
                            SUM(DISTINCT arbitrary_distinct_metric_rows.metric_value)
                     FROM arbitrary_distinct_metric_rows AS metric_row
                     WHERE metric_row.scope_key = wanted_scope",
            )
            .unwrap()
    };

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(2), SqlValue::Int(116)]]
    );
}

#[test]
fn derived_query_engines_reuse_shared_routine_var_context() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    let shared_vars = Arc::new(BTreeMap::from([(
        "arbitrary_scope_value".to_string(),
        SqlValue::Int(42),
    )]));
    let engine = SqlEngine::new(&db).with_shared_routine_vars(shared_vars.clone());

    assert!(Arc::ptr_eq(&engine.routine_vars, &shared_vars));

    let derived = engine
        .inherit_transaction(SqlEngine::with_ctes_and_context(
            &db,
            SqlSettings::default(),
            BTreeMap::new(),
            None,
            Arc::new(HashMap::new()),
        ))
        .with_shared_routine_vars(engine.routine_vars.clone());

    assert!(Arc::ptr_eq(&derived.routine_vars, &shared_vars));

    let result = engine
        .execute(
            "WITH arbitrary_cte AS (
                    SELECT arbitrary_scope_value AS derived_value
                 )
                 SELECT derived_value FROM arbitrary_cte",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(42)]]);
}

#[test]
fn routine_count_and_cursor_use_indexes_for_bound_predicates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=5000)
        .map(|row_id| {
            let bucket_key = if row_id <= 4 { 7 } else { row_id };
            let label_value = if row_id <= 4 { "target" } else { "other" };
            let rank_value = 10 - row_id.min(4);
            format!("({row_id}, {bucket_key}, '{label_value}', {rank_value}, {row_id})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_routine_index_rows (
                        row_id INT PRIMARY KEY,
                        bucket_key INT,
                        label_value TEXT,
                        rank_value INT,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_routine_index_lookup
                     ON arbitrary_routine_index_rows (bucket_key, label_value, rank_value)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_routine_index_rows
                     (row_id, bucket_key, label_value, rank_value, payload_value)
                     VALUES {values}"
            ))
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE arbitrary_routine_index_probe(
                        chosen_bucket IN INTEGER,
                        chosen_label IN VARCHAR,
                        matched_count INOUT INTEGER,
                        first_payload INOUT INTEGER
                    )
                    AS $$
                    DECLARE
                        ordered_matches CURSOR FOR
                            SELECT payload_value
                            FROM arbitrary_routine_index_rows
                            WHERE bucket_key = chosen_bucket
                              AND label_value = chosen_label
                            ORDER BY rank_value;
                    BEGIN
                        SELECT count(row_id)
                        INTO matched_count
                        FROM arbitrary_routine_index_rows
                        WHERE label_value = chosen_label
                          AND bucket_key = chosen_bucket;

                        OPEN ordered_matches;
                        FETCH ordered_matches INTO first_payload;
                        CLOSE ordered_matches;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_routine_index_probe(7, 'target', 0, 0)")
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(4), SqlValue::Int(4)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count >= 2);
    assert!(
        stats.rows_materialized <= 8,
        "routine count and cursor lookup should not materialize the whole table, got {} rows",
        stats.rows_materialized
    );
}

#[test]
fn pending_transaction_index_lookup_materializes_only_matching_delta_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pending_lookup_rows (
                        row_id INT PRIMARY KEY,
                        bucket_key INT,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_pending_lookup_idx
                     ON arbitrary_pending_lookup_rows (bucket_key)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_pending_lookup_rows
                     (row_id, bucket_key, payload_value)
                     VALUES (1, 7, 10), (2, 8, 20)",
            )
            .unwrap();
    }

    let pending_values = (10..=80)
        .map(|row_id| {
            let bucket_key = if row_id == 42 { 7 } else { 99 };
            format!("({row_id}, {bucket_key}, {row_id})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pending_lookup_rows
                     (row_id, bucket_key, payload_value)
                     VALUES {pending_values}"
            ))
            .unwrap();

        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let result = session
            .execute(
                "SELECT COUNT(*), SUM(payload_value)
                     FROM arbitrary_pending_lookup_rows AS arbitrary_alias
                     WHERE arbitrary_alias.bucket_key = 7",
            )
            .unwrap();
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

        assert_eq!(stats.full_scan_count, 0);
        assert!(stats.index_lookup_count > 0);
        assert!(
            stats.rows_materialized <= 2,
            "pending index lookup should merge only matching transaction delta rows, got {}",
            stats.rows_materialized
        );
        result
    };

    assert_eq!(result.rows, vec![vec![SqlValue::Int(2), SqlValue::Int(52)]]);
}

#[test]
fn pending_transaction_index_lookup_skips_merge_for_unmodified_collection() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pending_unrelated_writes (
                        row_id INT PRIMARY KEY,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_unmodified_index_rows (
                        scope_key INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_pending_unrelated_writes
                     (row_id, payload_value)
                     VALUES (1, 10)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_unmodified_index_rows
                     (scope_key, item_key, payload_value)
                     VALUES (7, 11, 110), (7, 12, 120), (8, 11, 210)",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "UPDATE arbitrary_pending_unrelated_writes
                 SET payload_value = payload_value + 1
                 WHERE row_id = 1",
        )
        .unwrap();

    reset_sql_pending_id_merges();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = session
        .execute(
            "SELECT item_key
                 FROM arbitrary_unmodified_index_rows AS lookup_row
                 WHERE lookup_row.scope_key = 7
                 ORDER BY item_key",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(11)], vec![SqlValue::Int(12)]]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(
        sql_pending_id_merges(),
        0,
        "pending writes in other collections should not force index id merge sets"
    );
}

#[test]
fn pending_transaction_index_range_materializes_only_matching_delta_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pending_range_rows (
                        row_id INT PRIMARY KEY,
                        bucket_key INT,
                        rank_value INT,
                        marker_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_pending_range_idx
                     ON arbitrary_pending_range_rows (bucket_key, rank_value, marker_value)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_pending_range_rows
                     (row_id, bucket_key, rank_value, marker_value)
                     VALUES (1, 7, 5, 1), (2, 7, 30, 2), (3, 8, 15, 3)",
            )
            .unwrap();
    }

    let pending_values = (10..=90)
        .map(|row_id| {
            let rank_value = if row_id == 42 { 15 } else { 100 + row_id };
            format!("({row_id}, 7, {rank_value}, {row_id})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pending_range_rows
                     (row_id, bucket_key, rank_value, marker_value)
                     VALUES {pending_values}"
            ))
            .unwrap();

        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let result = session
            .execute(
                "SELECT COUNT(*), SUM(marker_value)
                     FROM arbitrary_pending_range_rows AS arbitrary_alias
                     WHERE arbitrary_alias.bucket_key = 7
                       AND arbitrary_alias.rank_value >= 10
                       AND arbitrary_alias.rank_value <= 20",
            )
            .unwrap();
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

        assert_eq!(stats.full_scan_count, 0);
        assert!(stats.index_lookup_count > 0);
        assert!(
            stats.rows_materialized <= 1,
            "pending index range should merge only matching transaction delta rows, got {}",
            stats.rows_materialized
        );
        result
    };

    assert_eq!(result.rows, vec![vec![SqlValue::Int(1), SqlValue::Int(42)]]);
}

#[test]
fn pending_transaction_index_update_removes_stale_committed_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pending_update_rows (
                        row_id INT PRIMARY KEY,
                        bucket_key INT,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_pending_update_idx
                     ON arbitrary_pending_update_rows (bucket_key)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_pending_update_rows
                     (row_id, bucket_key, payload_value)
                     VALUES (1, 7, 10), (2, 8, 20)",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "UPDATE arbitrary_pending_update_rows
                 SET bucket_key = 8, payload_value = 30
                 WHERE row_id = 1",
        )
        .unwrap();

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let stale_lookup = session
        .execute(
            "SELECT row_id
                 FROM arbitrary_pending_update_rows AS arbitrary_alias
                 WHERE arbitrary_alias.bucket_key = 7",
        )
        .unwrap();
    let stale_stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert!(stale_lookup.rows.is_empty());
    assert_eq!(stale_stats.full_scan_count, 0);
    assert!(stale_stats.index_lookup_count > 0);
    assert_eq!(
        stale_stats.rows_materialized, 0,
        "updated pending rows should be removed from stale committed index candidates"
    );

    let current_lookup = session
        .execute(
            "SELECT row_id, payload_value
                 FROM arbitrary_pending_update_rows AS arbitrary_alias
                 WHERE arbitrary_alias.bucket_key = 8
                 ORDER BY row_id",
        )
        .unwrap();
    assert_eq!(
        current_lookup.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(30)],
            vec![SqlValue::Int(2), SqlValue::Int(20)],
        ]
    );
}

#[test]
fn routine_query_for_loop_uses_primary_key_prefix_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=5000)
        .map(|row_id| {
            let order_key = if row_id <= 5 { 19 } else { row_id };
            let tenant_key = if row_id <= 5 { 7 } else { 8 };
            let shard_key = if row_id <= 5 { 3 } else { 4 };
            let line_key = row_id;
            format!("({order_key}, {tenant_key}, {shard_key}, {line_key}, {row_id})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_loop_key_rows (
                        order_key INT,
                        tenant_key INT,
                        shard_key INT,
                        line_key INT,
                        payload_value INT,
                        PRIMARY KEY (order_key, tenant_key, shard_key, line_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_loop_key_rows
                     (order_key, tenant_key, shard_key, line_key, payload_value)
                     VALUES {values}"
            ))
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE arbitrary_loop_key_probe(
                        chosen_order IN INTEGER,
                        chosen_tenant IN INTEGER,
                        chosen_shard IN INTEGER,
                        seen_count INOUT INTEGER
                    )
                    AS $$
                    DECLARE
                        observed_row RECORD;
                    BEGIN
                        FOR observed_row IN
                            SELECT payload_value, chosen_tenant
                            FROM arbitrary_loop_key_rows
                            WHERE order_key = chosen_order
                              AND shard_key = chosen_shard
                              AND tenant_key = chosen_tenant
                        LOOP
                            seen_count := seen_count + 1;
                        END LOOP;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_loop_key_probe(19, 7, 3, 0)")
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(5)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.record_id_prefix_scans, 0);
    assert!(
        stats.rows_materialized <= 5,
        "routine query loop should use the primary key prefix, got {} materialized rows",
        stats.rows_materialized
    );
}

#[test]
fn routine_query_for_loop_with_null_key_avoids_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=5000)
        .map(|row_id| format!("({row_id}, 7, {row_id}, {row_id})"))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_null_key_rows (
                        order_key INT,
                        tenant_key INT,
                        line_key INT,
                        payload_value INT,
                        PRIMARY KEY (order_key, tenant_key, line_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_null_key_rows
                     (order_key, tenant_key, line_key, payload_value)
                     VALUES {values}"
            ))
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE arbitrary_null_key_probe(
                        maybe_order IN INTEGER,
                        chosen_tenant IN INTEGER,
                        seen_count INOUT INTEGER
                    )
                    AS $$
                    DECLARE
                        observed_row RECORD;
                    BEGIN
                        FOR observed_row IN
                            SELECT payload_value
                            FROM arbitrary_null_key_rows
                            WHERE order_key = maybe_order
                              AND tenant_key = chosen_tenant
                        LOOP
                            seen_count := seen_count + 1;
                        END LOOP;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_null_key_probe(NULL, 7, 0)")
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert_eq!(stats.rows_materialized, 0);
}

#[test]
fn indexed_select_reuses_target_schema_for_candidate_rls_checks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=32)
        .map(|row_id| format!("({row_id}, 17, 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_cached_select_rows (
                        row_id INT PRIMARY KEY,
                        lookup_key INT,
                        payload INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_cached_select_lookup_idx
                     ON arbitrary_cached_select_rows (lookup_key)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_cached_select_rows (row_id, lookup_key, payload)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT SUM(payload)
                 FROM arbitrary_cached_select_rows
                 WHERE lookup_key = 17",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(32)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.schema_loads <= 3,
        "indexed SELECT should reuse statement schema, got {} schema loads",
        stats.schema_loads
    );
}

#[test]
fn index_predicates_match_unquoted_identifier_case_insensitively() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_case_index_rows (
                        row_id INT PRIMARY KEY,
                        lookup_key INT,
                        sort_key INT,
                        payload TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_case_lookup_idx
                     ON arbitrary_case_index_rows (LOOKUP_KEY, SORT_KEY)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_case_index_rows (row_id, lookup_key, sort_key, payload)
                     VALUES (1, 7, 1, 'one'), (2, 7, 2, 'two'), (3, 8, 2, 'three')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT row_id
                 FROM arbitrary_case_index_rows
                 WHERE lookup_key = 7 AND sort_key = 2",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(2)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(stats.rows_materialized <= 1);
}

#[test]
fn declared_composite_primary_key_exact_predicate_uses_record_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_exact_key_rows (
                        tenant_key INT,
                        item_key INT,
                        shard_key INT,
                        payload TEXT,
                        PRIMARY KEY (tenant_key, item_key, shard_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_exact_key_rows
                     (tenant_key, item_key, shard_key, payload)
                     VALUES
                     (1, 1, 1, 'outside'),
                     (7, 42, 3, 'target'),
                     (7, 43, 3, 'neighbor')",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_exact_key_payload_idx
                     ON arbitrary_exact_key_rows (payload)",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_exact_key_item_idx
                     ON arbitrary_exact_key_rows (item_key)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT payload
                 FROM arbitrary_exact_key_rows
                 WHERE tenant_key = 7
                   AND item_key = 42
                   AND shard_key = 3",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("target".to_string())]]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.index_catalog_entries_considered, 0);
    assert!(stats.rows_materialized <= 1);
}

#[test]
fn declared_composite_primary_key_prefix_filters_row_queries_without_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_prefix_key_rows (
                        tenant_key INT,
                        sequence_key INT,
                        shard_key INT,
                        metric_value INT,
                        PRIMARY KEY (tenant_key, sequence_key, shard_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_prefix_key_rows
                     (tenant_key, sequence_key, shard_key, metric_value)
                     VALUES
                     (1, 1, 1, 100),
                     (1, 2, 2, 200),
                     (2, 1, 1, 70),
                     (2, 2, 2, 30),
                     (2, 3, 2, 20),
                     (3, 1, 2, 10)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .with_routine_vars(BTreeMap::from([
            ("chosen_tenant".to_string(), SqlValue::Int(2)),
            ("chosen_shard".to_string(), SqlValue::Int(2)),
        ]))
        .execute(
            "SELECT MIN(row_source.sequence_key)
                 FROM arbitrary_prefix_key_rows AS row_source
                 WHERE row_source.tenant_key = chosen_tenant
                   AND row_source.shard_key = chosen_shard",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(2)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.record_id_prefix_scans, 0);
    assert!(stats.rows_materialized <= 3);
}

#[test]
fn delete_using_primary_key_predicate_uses_candidate_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_direct_delete_rows (
                        row_id INT PRIMARY KEY,
                        payload TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_direct_delete_rows (row_id, payload)
                     VALUES (1, 'remove-a'), (2, 'keep'), (3, 'remove-b'), (4, 'other')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let direct_result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "WITH removed_rows AS (
                        DELETE FROM arbitrary_direct_delete_rows AS gone
                        USING UNNEST(ARRAY[1, 3]) AS wanted(row_id)
                        WHERE gone.row_id = wanted.row_id
                        RETURNING gone.row_id
                     )
                     SELECT array_agg(row_id)
                     FROM removed_rows",
            )
            .unwrap()
    };
    let direct_stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        direct_result.rows,
        vec![vec![SqlValue::Json(json!([1, 3]))]]
    );
    assert_eq!(direct_stats.full_scan_count, 0);
    assert!(direct_stats.index_lookup_count > 0);

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_delete_key_rows (
                        tenant_key INT,
                        sequence_key INT,
                        shard_key INT,
                        payload TEXT,
                        PRIMARY KEY (tenant_key, sequence_key, shard_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_delete_key_rows
                     (tenant_key, sequence_key, shard_key, payload)
                     VALUES
                     (7, 1, 1, 'remove-a'),
                     (7, 2, 1, 'keep-a'),
                     (7, 1, 2, 'remove-b'),
                     (7, 2, 2, 'keep-b'),
                     (8, 1, 1, 'other')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "WITH removed_rows AS (
                        DELETE FROM arbitrary_delete_key_rows AS gone
                        USING UNNEST(ARRAY[1, 2]) AS wanted(shard_key)
                        WHERE gone.tenant_key = 7
                          AND gone.shard_key = wanted.shard_key
                          AND gone.sequence_key = (
                              SELECT MIN(probe.sequence_key)
                              FROM arbitrary_delete_key_rows AS probe
                              WHERE probe.tenant_key = 7
                                AND probe.shard_key = wanted.shard_key
                          )
                        RETURNING gone.sequence_key
                     )
                     SELECT array_agg(sequence_key)
                     FROM removed_rows",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Json(json!([1, 1]))]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
}

#[test]
fn multi_row_delete_batches_record_writes_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_delete_batch_rows (
                        row_key INT PRIMARY KEY,
                        payload_text TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_delete_batch_rows (row_key, payload_text)
                     VALUES
                     (1, 'one'), (2, 'two'), (3, 'three'),
                     (4, 'four'), (5, 'five'), (6, 'six')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "DELETE FROM arbitrary_delete_batch_rows AS target_row
                     USING UNNEST(ARRAY[1, 2, 3, 4]) AS requested(row_key)
                     WHERE target_row.row_key = requested.row_key",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let remaining = SqlEngine::new(&db)
        .execute(
            "SELECT row_key
                 FROM arbitrary_delete_batch_rows
                 ORDER BY row_key",
        )
        .unwrap();
    assert_eq!(
        remaining.rows,
        vec![vec![SqlValue::Int(5)], vec![SqlValue::Int(6)]]
    );
    assert_eq!(stats.write_rows, 4);
    assert_eq!(
        stats.write_batches, 1,
        "multi-row DELETE should append record deletes as one batch, not one write per row"
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
}

#[test]
fn multi_row_delete_without_inbound_foreign_keys_checks_schema_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_delete_unreferenced_rows (
                        row_key INT PRIMARY KEY,
                        payload_text TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_other_parent_rows (
                        other_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_other_child_rows (
                        child_key INT PRIMARY KEY,
                        other_parent_key INT REFERENCES arbitrary_other_parent_rows(other_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_delete_unreferenced_rows (row_key, payload_text)
                     VALUES
                     (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "DELETE FROM arbitrary_delete_unreferenced_rows AS target_row
                     USING UNNEST(ARRAY[1, 2, 3]) AS requested(row_key)
                     WHERE target_row.row_key = requested.row_key",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(stats.write_rows, 3);
    assert_eq!(stats.write_batches, 1);
    assert_eq!(stats.foreign_key_parent_delete_schema_scans, 1);
    assert_eq!(stats.foreign_key_child_scans, 0);
}

#[test]
fn batched_parent_delete_restrict_scans_child_table_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_restrict_parent_rows (
                        parent_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_restrict_child_rows (
                        child_key INT PRIMARY KEY,
                        parent_ref INT REFERENCES arbitrary_restrict_parent_rows(parent_key)
                            ON DELETE RESTRICT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_restrict_parent_rows (parent_key)
                     VALUES (1), (2), (3)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_restrict_child_rows (child_key, parent_ref)
                     VALUES (10, 2)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "DELETE FROM arbitrary_restrict_parent_rows AS target_row
                     USING UNNEST(ARRAY[1, 2]) AS requested(parent_key)
                     WHERE target_row.parent_key = requested.parent_key",
            )
            .unwrap_err()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert!(
        error.to_string().contains("foreign key"),
        "expected a foreign-key violation, got {error}"
    );
    assert_eq!(stats.foreign_key_parent_delete_schema_scans, 1);
    assert_eq!(stats.foreign_key_child_scans, 1);
    let remaining = SqlEngine::new(&db)
        .execute(
            "SELECT COUNT(*)
                 FROM arbitrary_restrict_parent_rows",
        )
        .unwrap();
    assert_eq!(remaining.rows, vec![vec![SqlValue::Int(3)]]);
}

#[test]
fn batched_parent_delete_checks_restrict_before_cascade_actions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_mixed_parent_rows (
                        parent_key INT PRIMARY KEY
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_mixed_cascade_rows (
                        child_key INT PRIMARY KEY,
                        parent_ref INT REFERENCES arbitrary_mixed_parent_rows(parent_key)
                            ON DELETE CASCADE
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_mixed_restrict_rows (
                        child_key INT PRIMARY KEY,
                        parent_ref INT REFERENCES arbitrary_mixed_parent_rows(parent_key)
                            ON DELETE RESTRICT
                    )",
            )
            .unwrap();
        session
            .execute("INSERT INTO arbitrary_mixed_parent_rows (parent_key) VALUES (1)")
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_mixed_cascade_rows (child_key, parent_ref)
                     VALUES (10, 1)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_mixed_restrict_rows (child_key, parent_ref)
                     VALUES (20, 1)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("DELETE FROM arbitrary_mixed_parent_rows WHERE parent_key = 1")
            .unwrap_err()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert!(
        error.to_string().contains("foreign key"),
        "expected a foreign-key violation, got {error}"
    );
    assert_eq!(stats.foreign_key_child_scans, 1);
    let parent_count = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM arbitrary_mixed_parent_rows")
        .unwrap();
    let cascade_count = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM arbitrary_mixed_cascade_rows")
        .unwrap();
    let restrict_count = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM arbitrary_mixed_restrict_rows")
        .unwrap();
    assert_eq!(parent_count.rows, vec![vec![SqlValue::Int(1)]]);
    assert_eq!(cascade_count.rows, vec![vec![SqlValue::Int(1)]]);
    assert_eq!(restrict_count.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn batched_parent_update_restrict_scans_child_table_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_update_parent_rows (
                        row_key INT PRIMARY KEY,
                        parent_code INT UNIQUE
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_update_restrict_rows (
                        child_key INT PRIMARY KEY,
                        parent_ref INT REFERENCES arbitrary_update_parent_rows(parent_code)
                            ON UPDATE RESTRICT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_update_parent_rows (row_key, parent_code)
                     VALUES (1, 10), (2, 20), (3, 30)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_update_restrict_rows (child_key, parent_ref)
                     VALUES (10, 20)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_update_parent_rows AS target_row
                     SET parent_code = target_row.parent_code + 100
                     FROM UNNEST(ARRAY[10, 20]) AS requested(parent_code)
                     WHERE target_row.parent_code = requested.parent_code",
            )
            .unwrap_err()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert!(
        error.to_string().contains("foreign key"),
        "expected a foreign-key violation, got {error}"
    );
    assert_eq!(stats.foreign_key_child_scans, 1);
    let remaining = SqlEngine::new(&db)
        .execute(
            "SELECT parent_code
                 FROM arbitrary_update_parent_rows
                 ORDER BY parent_code",
        )
        .unwrap();
    assert_eq!(
        remaining.rows,
        vec![
            vec![SqlValue::Int(10)],
            vec![SqlValue::Int(20)],
            vec![SqlValue::Int(30)]
        ]
    );
}

#[test]
fn batched_parent_update_cascade_updates_children_generically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_cascade_parent_rows (
                        row_key INT PRIMARY KEY,
                        parent_code INT UNIQUE
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_cascade_child_rows (
                        child_key INT PRIMARY KEY,
                        parent_ref INT REFERENCES arbitrary_cascade_parent_rows(parent_code)
                            ON UPDATE CASCADE
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_cascade_parent_rows (row_key, parent_code)
                     VALUES (1, 10), (2, 20), (3, 30)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_cascade_child_rows (child_key, parent_ref)
                     VALUES (10, 10), (20, 20), (30, 30)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_cascade_parent_rows AS target_row
                     SET parent_code = target_row.parent_code + 100
                     FROM UNNEST(ARRAY[10, 20]) AS requested(parent_code)
                     WHERE target_row.parent_code = requested.parent_code",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(stats.foreign_key_child_scans, 1);
    let children = SqlEngine::new(&db)
        .execute(
            "SELECT child_key, parent_ref
                 FROM arbitrary_cascade_child_rows
                 ORDER BY child_key",
        )
        .unwrap();
    assert_eq!(
        children.rows,
        vec![
            vec![SqlValue::Int(10), SqlValue::Int(110)],
            vec![SqlValue::Int(20), SqlValue::Int(120)],
            vec![SqlValue::Int(30), SqlValue::Int(30)]
        ]
    );
}

#[test]
fn delete_using_correlated_scalar_subquery_binds_full_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=20)
        .flat_map(|seq_key| {
            [
                format!("(7, {seq_key}, 1, 'first')"),
                format!("(7, {seq_key}, 2, 'second')"),
            ]
        })
        .chain((1..=20).map(|seq_key| format!("(8, {seq_key}, 1, 'other')")))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_scalar_key_delete_rows (
                        group_key INT,
                        sequence_key INT,
                        bucket_key INT,
                        payload TEXT,
                        PRIMARY KEY (group_key, sequence_key, bucket_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_scalar_key_delete_rows
                     (group_key, sequence_key, bucket_key, payload)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "WITH removed_rows AS (
                        DELETE FROM arbitrary_scalar_key_delete_rows AS target_row
                        USING UNNEST(ARRAY[1, 2]) AS wanted(bucket_key)
                        WHERE target_row.group_key = 7
                          AND target_row.bucket_key = wanted.bucket_key
                          AND target_row.sequence_key = (
                              SELECT MIN(probe.sequence_key)
                              FROM arbitrary_scalar_key_delete_rows AS probe
                              WHERE probe.group_key = 7
                                AND probe.bucket_key = wanted.bucket_key
                          )
                        RETURNING target_row.bucket_key
                     )
                     SELECT array_agg(bucket_key)
                     FROM removed_rows",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let remaining = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM arbitrary_scalar_key_delete_rows WHERE group_key = 7")
        .unwrap();
    assert_eq!(remaining.rows, vec![vec![SqlValue::Int(38)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.record_id_prefix_scans, 0);
    assert!(
            stats.scalar_subqueries <= 2,
            "exact primary-key candidate lookup should not re-run scalar subqueries during covered predicate checks, got {} scalar subqueries",
            stats.scalar_subqueries
        );
    assert!(
        stats.rows_materialized <= 12,
        "correlated scalar key lookup should avoid repeated prefix materialization, got {} rows",
        stats.rows_materialized
    );
    assert_eq!(
            sql_slot_row_to_map_calls(),
            0,
            "correlated scalar subqueries should carry outer rows as slot rows instead of rebuilding name maps"
        );
}

#[test]
fn update_from_primary_key_predicate_uses_candidate_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_update_key_rows (
                        row_id INT PRIMARY KEY,
                        payload INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_update_key_rows (row_id, payload)
                     VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_update_key_rows AS target
                     SET payload = target.payload + patch.delta
                     FROM UNNEST(ARRAY[1, 3], ARRAY[100, 300]) AS patch(row_id, delta)
                     WHERE target.row_id = patch.row_id",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let result = SqlEngine::new(&db)
        .execute("SELECT row_id, payload FROM arbitrary_update_key_rows ORDER BY row_id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(110)],
            vec![SqlValue::Int(2), SqlValue::Int(20)],
            vec![SqlValue::Int(3), SqlValue::Int(330)],
            vec![SqlValue::Int(4), SqlValue::Int(40)],
        ]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
}

#[test]
fn update_without_from_skips_predicate_when_exact_primary_key_covers_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_direct_exact_update_rows (
                        tenant_key INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_direct_exact_update_rows
                     (tenant_key, item_key, payload_value)
                     VALUES (7, 42, 100), (7, 43, 200), (8, 42, 300)",
            )
            .unwrap();
    }

    reset_sql_row_lookup_compound_joins();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_direct_exact_update_rows AS target_row
                     SET payload_value = payload_value + 5
                     WHERE target_row.tenant_key = 7
                       AND target_row.item_key = 42",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let result = SqlEngine::new(&db)
        .execute(
            "SELECT tenant_key, item_key, payload_value
                 FROM arbitrary_direct_exact_update_rows
                 ORDER BY tenant_key, item_key",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(7), SqlValue::Int(42), SqlValue::Int(105)],
            vec![SqlValue::Int(7), SqlValue::Int(43), SqlValue::Int(200)],
            vec![SqlValue::Int(8), SqlValue::Int(42), SqlValue::Int(300)],
        ]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(
        sql_row_lookup_compound_joins(),
        0,
        "covered exact primary-key UPDATE should not evaluate the WHERE row predicate"
    );
}

#[test]
fn routine_update_without_from_uses_bound_composite_key_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=50)
        .flat_map(|tenant_key| {
            (1..=100).map(move |item_key| {
                let marker = if tenant_key == 7 && item_key == 42 {
                    1
                } else {
                    0
                };
                format!("({tenant_key}, {item_key}, 100, {marker})")
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_direct_key_update_rows (
                        tenant_key INT,
                        item_key INT,
                        balance_value INT,
                        marker_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_direct_key_update_rows
                     (tenant_key, item_key, balance_value, marker_value)
                     VALUES {values}"
            ))
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE arbitrary_direct_key_update_probe(
                        chosen_tenant IN INTEGER,
                        chosen_item IN INTEGER,
                        delta_value IN INTEGER,
                        observed_balance INOUT INTEGER
                    )
                    AS $$
                    BEGIN
                        UPDATE arbitrary_direct_key_update_rows AS target_row
                        SET balance_value = target_row.balance_value + delta_value
                        WHERE target_row.item_key = chosen_item
                          AND target_row.tenant_key = chosen_tenant
                        RETURNING balance_value INTO observed_balance;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_direct_key_update_probe(7, 42, 5, 0)")
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(105)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert_eq!(
            stats.index_lookup_count, 1,
            "routine-bound UPDATE should use only the candidate key lookup when unique keys are unchanged"
        );
    assert!(
        stats.rows_materialized <= 2,
        "routine-bound UPDATE should use the declared composite key, got {} materialized rows",
        stats.rows_materialized
    );

    let rows = SqlEngine::new(&db)
        .execute(
            "SELECT COUNT(*), SUM(balance_value), SUM(marker_value)
                 FROM arbitrary_direct_key_update_rows
                 WHERE balance_value = 105",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Int(105), SqlValue::Int(1)]]
    );
}

#[test]
fn routine_update_returning_uses_canonical_schema_column_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_case_heads (
                        SCOPE_KEY INT,
                        GROUP_KEY INT,
                        NEXT_VALUE INT,
                        PRIMARY KEY (SCOPE_KEY, GROUP_KEY)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_case_items (
                        SCOPE_KEY INT,
                        GROUP_KEY INT,
                        ITEM_KEY INT,
                        PRIMARY KEY (SCOPE_KEY, GROUP_KEY, ITEM_KEY)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_case_heads
                     (SCOPE_KEY, GROUP_KEY, NEXT_VALUE)
                     VALUES (1, 1, 2)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_case_items
                     (SCOPE_KEY, GROUP_KEY, ITEM_KEY)
                     VALUES (1, 1, 1)",
            )
            .unwrap();
        session
            .execute(
                "CREATE OR REPLACE PROCEDURE arbitrary_case_make_item(
                        p_scope INT,
                        p_group INT,
                        made_key INOUT INT
                     )
                     AS $$
                     BEGIN
                         UPDATE arbitrary_case_heads
                         SET next_value = next_value + 1
                         WHERE scope_key = p_scope
                           AND group_key = p_group
                         RETURNING next_value - 1 INTO made_key;

                         INSERT INTO arbitrary_case_items
                         (scope_key, group_key, item_key)
                         VALUES (p_scope, p_group, made_key);
                     END;
                     $$
                     LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_case_make_item(1, 1, 0)")
            .unwrap()
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Int(2)]]);

    let items = SqlEngine::new(&db)
        .execute(
            "SELECT item_key
                 FROM arbitrary_case_items
                 WHERE scope_key = 1 AND group_key = 1
                 ORDER BY item_key",
        )
        .unwrap();
    assert_eq!(
        items.rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );
}

#[test]
fn routine_update_assignment_casts_to_target_column_type() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_typed_update_rows (
                        ROW_KEY INT PRIMARY KEY,
                        SMALL_VALUE SMALLINT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_typed_update_rows
                     (ROW_KEY, SMALL_VALUE)
                     VALUES (1, 3)",
            )
            .unwrap();
        session
            .execute(
                "CREATE OR REPLACE PROCEDURE arbitrary_typed_update_probe(
                        observed_value INOUT INT
                     )
                     AS $$
                     BEGIN
                         UPDATE arbitrary_typed_update_rows
                         SET small_value = 7.0
                         WHERE row_key = 1
                         RETURNING small_value INTO observed_value;
                     END;
                     $$
                     LANGUAGE 'plpgsql'",
            )
            .unwrap();
    }

    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CALL arbitrary_typed_update_probe(0)")
            .unwrap()
    };
    assert_eq!(result.rows, vec![vec![SqlValue::Int(7)]]);

    let stored = SqlEngine::new(&db)
        .execute("SELECT small_value FROM arbitrary_typed_update_rows WHERE row_key = 1")
        .unwrap();
    assert_eq!(stored.rows, vec![vec![SqlValue::Int(7)]]);
}

#[test]
fn multi_row_update_validates_before_writing_any_candidate_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_batch_update_guard (
                        row_key INT PRIMARY KEY,
                        unique_value INT UNIQUE
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_batch_update_guard (row_key, unique_value)
                     VALUES (1, 10), (2, 20)",
            )
            .unwrap();
    }

    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("UPDATE arbitrary_batch_update_guard SET unique_value = 99")
            .unwrap_err()
    };
    assert_eq!(error.sqlstate(), "23505");

    let rows = SqlEngine::new(&db)
        .execute(
            "SELECT row_key, unique_value
                 FROM arbitrary_batch_update_guard
                 ORDER BY row_key",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(10)],
            vec![SqlValue::Int(2), SqlValue::Int(20)],
        ]
    );
}

#[test]
fn update_from_routine_bound_unnest_uses_composite_key_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=50)
        .flat_map(|bucket_key| {
            (1..=100).map(move |item_key| format!("({bucket_key}, {item_key}, 500)"))
        })
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_composite_patch_rows (
                        bucket_key INT,
                        item_key INT,
                        quantity INT,
                        PRIMARY KEY (bucket_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_composite_patch_rows
                     (bucket_key, item_key, quantity)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([
            (
                "chosen_items".to_string(),
                SqlValue::Json(json!([11, 12, 13])),
            ),
            (
                "chosen_buckets".to_string(),
                SqlValue::Json(json!([7, 7, 8])),
            ),
            ("deltas".to_string(), SqlValue::Json(json!([5, 6, 7]))),
        ]));
        session
            .execute(
                "WITH changed_rows AS (
                        UPDATE arbitrary_composite_patch_rows AS target_row
                        SET quantity =
                            CASE
                                WHEN target_row.quantity < (source_row.delta_value + 10)
                                THEN target_row.quantity + 91
                                ELSE target_row.quantity
                            END - source_row.delta_value
                        FROM UNNEST(chosen_items, chosen_buckets, deltas)
                             AS source_row(item_key, bucket_key, delta_value)
                        WHERE target_row.item_key = source_row.item_key
                          AND target_row.bucket_key = source_row.bucket_key
                          AND target_row.bucket_key = ANY(chosen_buckets)
                        RETURNING target_row.bucket_key,
                                  target_row.item_key,
                                  target_row.quantity
                     )
                     SELECT array_agg(quantity)
                     FROM changed_rows",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Json(json!([495, 494, 493]))]]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.rows_materialized <= 16,
        "routine-bound UNNEST update should not materialize the target table, got {} rows",
        stats.rows_materialized
    );
    assert!(
        stats.join_candidate_pairs <= 16,
        "routine-bound UNNEST update should avoid target/source cross joins, got {} pairs",
        stats.join_candidate_pairs
    );
}

#[test]
fn simple_returning_projection_resolves_arbitrary_aliases_directly() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_returning_source (
                        scope_key INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_returning_source
                     (scope_key, item_key, payload_value)
                     VALUES (7, 11, 20), (7, 12, 30), (8, 11, 40)",
            )
            .unwrap();
    }

    reset_sql_row_from_record_calls();
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_returning_source AS changed_row
                     SET payload_value = changed_row.payload_value + 5
                     WHERE changed_row.scope_key = 7
                       AND changed_row.item_key = 11
                     RETURNING changed_row.item_key AS returned_item,
                               (changed_row.payload_value) AS returned_payload",
            )
            .unwrap()
    };

    assert_eq!(
        result.columns,
        vec!["returned_item".to_string(), "returned_payload".to_string()]
    );
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(25)]]
    );

    let rows = SqlEngine::new(&db)
        .execute(
            "SELECT item_key, payload_value
                 FROM arbitrary_returning_source
                 WHERE scope_key = 7
                 ORDER BY item_key",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(11), SqlValue::Int(25)],
            vec![SqlValue::Int(12), SqlValue::Int(30)],
        ]
    );
}

#[test]
fn returning_projection_evaluates_target_expressions_without_row_maps() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_returning_expression_source (
                        scope_key INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
    }

    reset_sql_row_from_record_calls();
    let inserted = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "INSERT INTO arbitrary_returning_expression_source
                     (scope_key, item_key, payload_value)
                     VALUES (7, 11, 20)
                     RETURNING arbitrary_returning_expression_source.item_key AS returned_item,
                               payload_value - 1 AS previous_payload,
                               (payload_value * 2)::INT AS doubled_payload",
            )
            .unwrap()
    };

    assert_eq!(
        inserted.rows,
        vec![vec![
            SqlValue::Int(11),
            SqlValue::Int(19),
            SqlValue::Int(40),
        ]]
    );
    assert_eq!(
        sql_row_from_record_calls(),
        0,
        "target-only RETURNING expressions should read records directly"
    );

    reset_sql_row_from_record_calls();
    let updated = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_returning_expression_source AS changed_row
                     SET payload_value = changed_row.payload_value + 5
                     WHERE changed_row.scope_key = 7
                       AND changed_row.item_key = 11
                     RETURNING changed_row.item_key AS returned_item,
                               changed_row.payload_value - 1 AS previous_payload",
            )
            .unwrap()
    };

    assert_eq!(
        updated.rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(24)]]
    );
    assert_eq!(
        sql_row_from_record_calls(),
        0,
        "UPDATE assignment and RETURNING should read target values from slot rows"
    );
}

#[test]
fn update_from_returning_projects_source_aliases_and_updated_target_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_update_returning_targets (
                        row_key INT PRIMARY KEY,
                        quantity_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_update_returning_targets
                     (row_key, quantity_value)
                     VALUES (1, 10)",
            )
            .unwrap();
    }

    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_update_returning_targets AS target_row
                     SET quantity_value = target_row.quantity_value + source_row.delta_value
                     FROM UNNEST(ARRAY[1], ARRAY[5], ARRAY[30])
                          AS source_row(row_key, delta_value, price_value)
                     WHERE target_row.row_key = source_row.row_key
                     RETURNING source_row.delta_value AS returned_delta,
                               source_row.price_value AS returned_price,
                               target_row.quantity_value AS returned_quantity,
                               source_row.delta_value + source_row.price_value AS returned_total",
            )
            .unwrap()
    };

    assert_eq!(
        result.columns,
        vec![
            "returned_delta".to_string(),
            "returned_price".to_string(),
            "returned_quantity".to_string(),
            "returned_total".to_string(),
        ]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(5),
            SqlValue::Int(30),
            SqlValue::Int(15),
            SqlValue::Int(35),
        ]]
    );
    assert_eq!(
        sql_row_from_record_calls(),
        0,
        "UPDATE FROM assignment and RETURNING should read target/source values from slot rows"
    );
    assert_eq!(
        sql_slot_row_to_map_calls(),
        0,
        "UPDATE FROM should carry source rows as slots instead of rebuilding name maps"
    );
}

#[test]
fn update_unchanged_unique_keys_skips_redundant_unique_validation_lookups() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_unique_payload_rows (
                        scope_key INT,
                        row_key INT,
                        external_code INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, row_key),
                        UNIQUE (scope_key, external_code)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_unique_payload_rows
                     (scope_key, row_key, external_code, payload_value)
                     VALUES
                     (7, 11, 101, 10),
                     (7, 12, 102, 20),
                     (7, 13, 103, 30)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_unique_payload_rows AS target_row
                     SET payload_value = target_row.payload_value + requested.delta_value
                     FROM UNNEST(ARRAY[11, 12, 13], ARRAY[5, 6, 7])
                          AS requested(row_key, delta_value)
                     WHERE target_row.scope_key = 7
                       AND target_row.row_key = requested.row_key",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(stats.full_scan_count, 0);
    assert!(
        stats.index_lookup_count <= 6,
        "unchanged unique keys should not add per-row validation lookups, got {}",
        stats.index_lookup_count
    );
    let rows = SqlEngine::new(&db)
        .execute(
            "SELECT row_key, external_code, payload_value
                 FROM arbitrary_unique_payload_rows
                 ORDER BY row_key",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(11), SqlValue::Int(101), SqlValue::Int(15)],
            vec![SqlValue::Int(12), SqlValue::Int(102), SqlValue::Int(26)],
            vec![SqlValue::Int(13), SqlValue::Int(103), SqlValue::Int(37)],
        ]
    );
}

#[test]
fn update_changed_unique_keys_still_detects_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_unique_rekey_rows (
                        scope_key INT,
                        row_key INT,
                        external_code INT,
                        PRIMARY KEY (scope_key, row_key),
                        UNIQUE (scope_key, external_code)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_unique_rekey_rows
                     (scope_key, row_key, external_code)
                     VALUES (7, 11, 101), (7, 12, 102)",
            )
            .unwrap();
    }

    let error = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_unique_rekey_rows
                     SET external_code = 102
                     WHERE scope_key = 7
                       AND row_key = 11",
            )
            .unwrap_err()
    };

    assert!(
        error.to_string().contains("duplicate key"),
        "expected a unique violation, got {error}"
    );
}

#[test]
fn materialized_cte_clones_share_row_storage() {
    let cte = CteResult::new(
        "arbitrary_shared_rows".to_string(),
        vec!["row_key".to_string()],
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]],
    );
    let cloned = cte.clone();

    assert!(Arc::ptr_eq(&cte.rows, &cloned.rows));
    assert_eq!(cloned.rows.len(), 2);
}

#[test]
fn row_value_key_capacity_accounts_for_qualified_aliases() {
    assert_eq!(
        row_value_key_capacity("arbitrary_rows", "arbitrary_rows", 4),
        8
    );
    assert_eq!(
        row_value_key_capacity("arbitrary_rows", "changed_rows", 4),
        12
    );
}

#[test]
fn schema_bound_row_materialization_preserves_record_fields() {
    fn column(name: &str, pg_type: &str, primary_key: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            pg_type: pg_type.to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        }
    }

    let schema = TableSchema {
        name: "arbitrary_slot_rows".to_string(),
        schema_name: "public".to_string(),
        row_type_oid: None,
        row_array_type_oid: None,
        columns: vec![
            column("row_key", "int4", true),
            column("ordinary_value", "int4", false),
            column("metadata", "jsonb", false),
            column("timestamp", "int8", false),
            column("payload", "jsonb", false),
            column("embedding", "vector", false),
        ],
        primary_key_name: None,
        indexes: Vec::new(),
        constraints: Vec::new(),
        rls_enabled: false,
        rls_forced: false,
        policies: Vec::new(),
        owner: None,
        partitioning: None,
        partition_of: None,
    };
    let record = Record::new("42")
        .with_metadata(json!({
            "ordinary_value": 7,
            "metadata": {"inner": true}
        }))
        .with_timestamp(123)
        .with_payload(vec![1, 2])
        .with_vector(vec![0.5, 1.5]);

    let row = row_from_record("arbitrary_slot_rows", "slot_alias", Some(&schema), &record).unwrap();

    assert_eq!(row.get("slot_alias.row_key"), Some(&SqlValue::Int(42)));
    assert_eq!(row.get("row_key"), Some(&SqlValue::Int(42)));
    assert_eq!(
        row.get("slot_alias.ordinary_value"),
        Some(&SqlValue::Int(7))
    );
    assert_eq!(
        row.get("slot_alias.metadata"),
        Some(&SqlValue::Json(json!({"inner": true})))
    );
    assert_eq!(row.get("slot_alias.timestamp"), Some(&SqlValue::Int(123)));
    assert_eq!(
        row.get("slot_alias.payload"),
        Some(&SqlValue::Json(json!([1, 2])))
    );
    assert_eq!(
        row.get("slot_alias.embedding"),
        Some(&SqlValue::Json(json!([0.5, 1.5])))
    );
}

#[test]
fn select_projection_binds_schema_columns_to_direct_record_slots() {
    fn column(name: &str, pg_type: &str, primary_key: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            pg_type: pg_type.to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        }
    }

    let schema = TableSchema {
        name: "arbitrary_projection_rows".to_string(),
        schema_name: "public".to_string(),
        row_type_oid: None,
        row_array_type_oid: None,
        columns: vec![
            column("row_key", "int4", true),
            column("payload_value", "int4", false),
        ],
        primary_key_name: None,
        indexes: Vec::new(),
        constraints: Vec::new(),
        rls_enabled: false,
        rls_forced: false,
        policies: Vec::new(),
        owner: None,
        partitioning: None,
        partition_of: None,
    };
    let query = parse_single_query(
        "SELECT row_key AS projected_key, payload_value
             FROM arbitrary_projection_rows",
    )
    .unwrap();
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select query");
    };

    let projection = Projection::from_select_items(&select.projection, Some(&schema)).unwrap();

    assert_eq!(
        projection.columns,
        vec!["projected_key".to_string(), "payload_value".to_string()]
    );
    match &projection.fields[..] {
        [ProjectedRecordField::SchemaColumn {
            column: key_column,
            use_record_id_for_primary_key: true,
        }, ProjectedRecordField::SchemaColumn {
            column: value_column,
            use_record_id_for_primary_key: false,
        }] => {
            assert_eq!(key_column.name, "row_key");
            assert_eq!(value_column.name, "payload_value");
        }
        fields => panic!("expected schema-column projection slots, got {fields:?}"),
    }

    let record = Record::new("42").with_metadata(json!({
        "payload_value": 7
    }));
    assert_eq!(
        projection.row(&record).unwrap(),
        vec![SqlValue::Int(42), SqlValue::Int(7)]
    );
}

#[test]
fn data_modifying_cte_grouped_aggregate_with_correlated_lookup_is_generic() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_group_lookup_headers (
                        scope_key INT,
                        group_key INT,
                        parent_key INT,
                        owner_ref INT,
                        PRIMARY KEY (scope_key, group_key, parent_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_group_lookup_lines (
                        scope_key INT,
                        group_key INT,
                        parent_key INT,
                        line_key INT,
                        amount_value INT,
                        PRIMARY KEY (scope_key, group_key, parent_key, line_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_group_lookup_headers
                     (scope_key, group_key, parent_key, owner_ref)
                     VALUES (1, 10, 100, 1000), (1, 20, 200, 2000)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_group_lookup_lines
                     (scope_key, group_key, parent_key, line_key, amount_value)
                     VALUES
                     (1, 10, 100, 1, 5),
                     (1, 10, 100, 2, 2),
                     (1, 20, 200, 1, 7)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "WITH changed_rows AS (
                        UPDATE arbitrary_group_lookup_lines AS target_row
                        SET amount_value = target_row.amount_value + 1
                        FROM UNNEST(ARRAY[10, 20], ARRAY[100, 200])
                             AS requested(group_key, parent_key)
                        WHERE target_row.scope_key = 1
                          AND target_row.group_key = requested.group_key
                          AND target_row.parent_key = requested.parent_key
                        RETURNING target_row.group_key,
                                  target_row.parent_key,
                                  target_row.amount_value
                     )
                     SELECT array_agg(group_key),
                            array_agg(owner_ref),
                            array_agg(total_amount)
                     FROM (
                        SELECT group_key,
                               (
                                   SELECT DISTINCT owner_row.owner_ref
                                   FROM arbitrary_group_lookup_headers AS owner_row
                                   WHERE owner_row.scope_key = 1
                                     AND owner_row.group_key = changed_rows.group_key
                                     AND owner_row.parent_key = changed_rows.parent_key
                               ) AS owner_ref,
                               sum(amount_value) AS total_amount
                        FROM changed_rows
                        GROUP BY group_key, parent_key
                     ) AS grouped_rows",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Json(json!([10, 20])),
            SqlValue::Json(json!([1000, 2000])),
            SqlValue::Json(json!([9, 8])),
        ]]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
}

#[test]
fn indexed_right_join_filters_residual_predicates_before_row_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_join_payloads (
                        scope_key INT,
                        item_key INT,
                        quantity_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_join_payloads
                     (scope_key, item_key, quantity_value)
                     VALUES (7, 11, 50), (7, 12, 60), (7, 13, 70)",
            )
            .unwrap();
    }

    reset_sql_row_from_record_calls();
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT COUNT(*)
                 FROM UNNEST(ARRAY[11, 12, 13]) AS driver_row(item_key)
                 JOIN arbitrary_join_payloads AS payload_row
                   ON payload_row.scope_key = 7
                  AND payload_row.item_key = driver_row.item_key
                  AND payload_row.quantity_value < 10",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(
            sql_row_from_record_calls(),
            0,
            "residual predicates on indexed right joins should reject records before building right-side row maps"
        );
}

#[test]
fn left_join_from_derived_rows_uses_right_primary_key_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=5000)
        .map(|item_key| format!("({item_key}, {})", item_key * 10))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_join_lookup_items (
                        item_key INT PRIMARY KEY,
                        price_value INT
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_join_lookup_items (item_key, price_value)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "requested_keys".to_string(),
            SqlValue::Json(json!([11, 12, 9999])),
        )]));
        session
            .execute(
                "SELECT array_agg(catalog_row.price_value)
                     FROM UNNEST(requested_keys) AS requested(item_key)
                     LEFT JOIN arbitrary_join_lookup_items AS catalog_row
                       ON catalog_row.item_key = requested.item_key",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Json(json!([110, 120, null]))]]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.rows_materialized <= 8,
        "indexed left join should not materialize the right table, got {} rows",
        stats.rows_materialized
    );
    assert!(
        stats.join_candidate_pairs <= 3,
        "indexed left join should evaluate only matching candidate pairs, got {}",
        stats.join_candidate_pairs
    );
}

#[test]
fn indexed_join_qualified_predicate_uses_outer_row_without_predicate_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_join_outer_rows (
                        tenant_key INT,
                        item_key INT,
                        label_text TEXT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_join_inner_rows (
                        tenant_key INT,
                        item_key INT,
                        quantity_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_join_outer_rows
                     (tenant_key, item_key, label_text)
                     VALUES (7, 11, 'kept-a'), (7, 12, 'kept-b'), (8, 11, 'other')",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_join_inner_rows
                     (tenant_key, item_key, quantity_value)
                     VALUES (7, 11, 4), (7, 12, 6), (8, 11, 3)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT left_row.label_text, right_row.quantity_value
                 FROM arbitrary_join_outer_rows AS left_row
                 JOIN arbitrary_join_inner_rows AS right_row
                   ON right_row.tenant_key = left_row.tenant_key
                  AND right_row.item_key = left_row.item_key
                  AND right_row.quantity_value > 3
                 WHERE left_row.tenant_key = 7
                 ORDER BY left_row.item_key",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::String("kept-a".to_string()), SqlValue::Int(4)],
            vec![SqlValue::String("kept-b".to_string()), SqlValue::Int(6)],
        ]
    );
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.join_predicate_row_merges, 0);
}

#[test]
fn indexed_join_reuses_bound_context_for_routine_vars_and_outer_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let left_values = (1..=96)
        .map(|item_key| format!("(7, {item_key}, {})", item_key % 5))
        .chain((1..=96).map(|item_key| format!("(8, {item_key}, {})", item_key % 5)))
        .collect::<Vec<_>>()
        .join(", ");
    let right_values = (1..=96)
        .map(|item_key| format!("(7, {item_key}, {})", item_key % 7))
        .chain((1..=96).map(|item_key| format!("(8, {item_key}, {})", item_key % 7)))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_context_left_rows (
                        scope_key INT,
                        item_key INT,
                        limit_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_context_right_rows (
                        scope_key INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (scope_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_context_left_rows
                     (scope_key, item_key, limit_value)
                     VALUES {left_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_context_right_rows
                     (scope_key, item_key, payload_value)
                     VALUES {right_values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "chosen_scope".to_string(),
            SqlValue::Int(7),
        )]));
        session
            .execute(
                "SELECT COUNT(*)
                     FROM arbitrary_context_left_rows AS outer_row
                     JOIN arbitrary_context_right_rows AS inner_row
                       ON inner_row.scope_key = chosen_scope
                      AND inner_row.item_key = outer_row.item_key
                      AND inner_row.payload_value >= outer_row.limit_value
                     WHERE outer_row.scope_key = chosen_scope",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(68)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.join_predicate_row_merges, 0);
}

#[test]
fn repeated_index_prefix_lookups_share_statement_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let left_values = (1..=32)
        .map(|row_id| format!("(7, {row_id}, {})", row_id % 5))
        .collect::<Vec<_>>()
        .join(", ");
    let right_values = (1..=96)
        .map(|row_id| format!("({row_id}, 7, {})", row_id % 9))
        .chain((97..=192).map(|row_id| format!("({row_id}, 8, {})", row_id % 9)))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_cached_prefix_left_rows (
                        partition_value INT,
                        row_id INT,
                        minimum_payload INT,
                        PRIMARY KEY (partition_value, row_id)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_cached_prefix_right_rows (
                        right_row_id INT PRIMARY KEY,
                        partition_value INT,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_cached_prefix_right_idx
                     ON arbitrary_cached_prefix_right_rows (partition_value)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_cached_prefix_left_rows
                     (partition_value, row_id, minimum_payload)
                     VALUES {left_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_cached_prefix_right_rows
                     (right_row_id, partition_value, payload_value)
                     VALUES {right_values}"
            ))
            .unwrap();
    }

    reset_sql_index_storage_lookup_calls();
    reset_sql_record_storage_get_calls();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "chosen_partition".to_string(),
            SqlValue::Int(7),
        )]));
        session
            .execute(
                "SELECT COUNT(*)
                     FROM arbitrary_cached_prefix_left_rows AS left_side
                     JOIN arbitrary_cached_prefix_right_rows AS right_side
                       ON right_side.partition_value = chosen_partition
                      AND right_side.payload_value >= left_side.minimum_payload
                     WHERE left_side.partition_value = chosen_partition",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(2405)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(
        stats.index_lookup_count > sql_index_storage_lookup_calls(),
        "logical index probes should exceed raw storage walks when prefixes repeat"
    );
    assert_eq!(
        sql_index_storage_lookup_calls(),
        2,
        "the statement should read the left filter once and the repeated right-side prefix once"
    );
    assert!(
        sql_record_storage_get_calls() <= 160,
        "repeated right-side ids should be materialized once per statement, got {} storage gets",
        sql_record_storage_get_calls()
    );
}

#[test]
fn indexed_right_join_prepares_primary_key_bindings_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let left_values = (1..=64)
        .map(|item_key| format!("(7, {item_key}, {item_key})"))
        .chain((1..=64).map(|item_key| format!("(8, {item_key}, {item_key})")))
        .collect::<Vec<_>>()
        .join(", ");
    let right_values = (1..=64)
        .map(|item_key| format!("(7, {item_key}, {})", item_key * 10))
        .chain((1..=512).map(|item_key| format!("(8, {item_key}, {})", item_key * 10)))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pk_join_left_rows (
                        partition_value INT,
                        item_key INT,
                        payload_floor INT,
                        PRIMARY KEY (partition_value, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_pk_join_right_rows (
                        partition_value INT,
                        item_key INT,
                        payload_value INT,
                        PRIMARY KEY (partition_value, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pk_join_left_rows
                     (partition_value, item_key, payload_floor)
                     VALUES {left_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pk_join_right_rows
                     (partition_value, item_key, payload_value)
                     VALUES {right_values}"
            ))
            .unwrap();
    }

    reset_sql_indexed_selection_plan_calls();
    reset_sql_join_constraint_eval_calls();
    reset_sql_records_for_ids_calls();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "SELECT COUNT(arbitrary_right.payload_value)
                     FROM arbitrary_pk_join_left_rows AS arbitrary_left
                     JOIN arbitrary_pk_join_right_rows AS arbitrary_right
                       ON arbitrary_right.partition_value = arbitrary_left.partition_value
                      AND arbitrary_right.item_key = arbitrary_left.item_key
                     WHERE arbitrary_left.partition_value = 7",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(64)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(
            sql_indexed_selection_plan_calls() <= 2,
            "primary-key right join should prepare dynamic bindings once, got {} indexed selection plans",
            sql_indexed_selection_plan_calls()
        );
    assert_eq!(
        sql_join_constraint_eval_calls(),
        0,
        "primary-key right join should not re-evaluate equality terms already proven by lookup"
    );
    assert!(
            sql_records_for_ids_calls() <= 3,
            "primary-key right join should batch right-side record loads, got {} record-id materialization calls",
            sql_records_for_ids_calls()
        );
}

#[test]
fn indexed_right_join_compiles_secondary_index_bindings_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let left_values = (1..=64)
        .map(|item_key| format!("(7, {item_key}, {})", item_key % 4))
        .chain((1..=64).map(|item_key| format!("(8, {item_key}, {})", item_key % 4)))
        .collect::<Vec<_>>()
        .join(", ");
    let right_values = (1..=64)
        .map(|item_key| format!("({}, 7, {item_key}, {})", item_key, item_key % 6))
        .chain(
            (1..=64)
                .map(|item_key| format!("({}, 8, {item_key}, {})", item_key + 1000, item_key % 6)),
        )
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_compiled_left_rows (
                        partition_value INT,
                        lookup_value INT,
                        minimum_payload INT,
                        PRIMARY KEY (partition_value, lookup_value)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_compiled_right_rows (
                        right_row_id INT PRIMARY KEY,
                        partition_value INT,
                        lookup_value INT,
                        payload_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_compiled_right_lookup
                     ON arbitrary_compiled_right_rows (partition_value, lookup_value)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_compiled_left_rows
                     (partition_value, lookup_value, minimum_payload)
                     VALUES {left_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_compiled_right_rows
                     (right_row_id, partition_value, lookup_value, payload_value)
                     VALUES {right_values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    reset_sql_merge_rows_calls();
    let result = {
        let mut session = SqlSession::new(&mut db);
        session.routine_vars = Arc::new(BTreeMap::from([(
            "chosen_partition".to_string(),
            SqlValue::Int(7),
        )]));
        session
            .execute(
                "SELECT COUNT(*)
                     FROM arbitrary_compiled_left_rows AS left_side
                     JOIN arbitrary_compiled_right_rows AS right_side
                       ON right_side.partition_value = chosen_partition
                      AND right_side.lookup_value = left_side.lookup_value
                      AND right_side.payload_value >= left_side.minimum_payload
                     WHERE left_side.partition_value = chosen_partition",
            )
            .unwrap()
    };
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(54)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert_eq!(stats.join_predicate_row_merges, 0);
    assert_eq!(
        sql_merge_rows_calls(),
        0,
        "indexed join output should concatenate slot rows without materializing merged name maps"
    );
    assert!(
        stats.index_catalog_entries_considered <= 1,
        "right-side secondary index bindings should be planned once, got {} catalog walks",
        stats.index_catalog_entries_considered
    );
}

#[test]
fn indexed_join_unqualified_conflict_keeps_merged_predicate_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_conflict_left_rows (
                        lookup_key INT PRIMARY KEY,
                        shared_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_conflict_right_rows (
                        lookup_key INT PRIMARY KEY,
                        shared_value INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_conflict_left_rows
                     (lookup_key, shared_value)
                     VALUES (1, 20)",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_conflict_right_rows
                     (lookup_key, shared_value)
                     VALUES (1, 10)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .execute(
            "SELECT left_row.lookup_key
                 FROM arbitrary_conflict_left_rows AS left_row
                 JOIN arbitrary_conflict_right_rows AS right_row
                   ON right_row.lookup_key = left_row.lookup_key
                  AND shared_value = 20",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.join_predicate_row_merges > 0,
        "unqualified conflicting column references must keep merged-row predicate semantics"
    );
}

#[test]
fn comma_join_where_equalities_drive_indexed_right_side_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let line_values = (1..=2000)
        .map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(7, 3, {seq_key}, {seq_key}, {item_key})")
        })
        .chain((1..=2000).map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(8, 3, {seq_key}, {seq_key}, {item_key})")
        }))
        .collect::<Vec<_>>()
        .join(", ");
    let stock_values = (1..=1000)
        .map(|item_key| format!("(7, {item_key}, 5)"))
        .chain((1..=1000).map(|item_key| format!("(8, {item_key}, 5)")))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_join_lines (
                        tenant_key INT,
                        shard_key INT,
                        sequence_key INT,
                        line_key INT,
                        item_key INT,
                        PRIMARY KEY (tenant_key, shard_key, sequence_key, line_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_join_stock (
                        tenant_key INT,
                        item_key INT,
                        quantity_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_join_counters (
                        tenant_key INT,
                        shard_key INT,
                        next_sequence INT,
                        PRIMARY KEY (tenant_key, shard_key)
                    )",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_join_lines
                     (tenant_key, shard_key, sequence_key, line_key, item_key)
                     VALUES {line_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_join_stock
                     (tenant_key, item_key, quantity_value)
                     VALUES {stock_values}"
            ))
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_join_counters
                     (tenant_key, shard_key, next_sequence)
                     VALUES (7, 3, 1990), (8, 3, 1990)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .with_routine_vars(BTreeMap::from([
            ("chosen_tenant".to_string(), SqlValue::Int(7)),
            ("chosen_shard".to_string(), SqlValue::Int(3)),
            ("quantity_limit".to_string(), SqlValue::Int(10)),
        ]))
        .execute(
            "SELECT COUNT(DISTINCT (item.item_key))
                 FROM arbitrary_join_lines line_item,
                      arbitrary_join_stock item,
                      arbitrary_join_counters counter_row
                 WHERE line_item.tenant_key = chosen_tenant
                   AND line_item.shard_key = chosen_shard
                   AND counter_row.tenant_key = chosen_tenant
                   AND counter_row.shard_key = chosen_shard
                   AND line_item.sequence_key < counter_row.next_sequence
                   AND line_item.sequence_key >= (counter_row.next_sequence - 20)
                   AND item.tenant_key = chosen_tenant
                   AND item.item_key = line_item.item_key
                   AND item.quantity_value < quantity_limit",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(20)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert_eq!(
        stats.planner_record_count_scans, 0,
        "join planning must use cheap collection cardinality, not scan records for row estimates"
    );
    assert!(stats.index_lookup_count > 0);
    assert!(
            stats.join_candidate_pairs <= 2500,
            "comma join WHERE equalities should apply the bounded prefix before later joins, got {} candidate pairs",
            stats.join_candidate_pairs
        );
}

#[test]
fn comma_join_dynamic_range_uses_btree_index_without_table_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let line_values = (1..=2000)
        .map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(7, 3, {seq_key}, {seq_key}, {item_key})")
        })
        .chain((1..=2000).map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(8, 3, {seq_key}, {seq_key}, {item_key})")
        }))
        .collect::<Vec<_>>()
        .join(", ");
    let stock_values = (1..=1000)
        .map(|item_key| format!("(7, {item_key}, 5)"))
        .chain((1..=1000).map(|item_key| format!("(8, {item_key}, 5)")))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_range_lines (
                        tenant_key INT,
                        shard_key INT,
                        sequence_key INT,
                        line_key INT,
                        item_key INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE UNIQUE INDEX arbitrary_range_lines_idx
                     ON arbitrary_range_lines (sequence_key, tenant_key, shard_key, line_key)",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_range_stock (
                        tenant_key INT,
                        item_key INT,
                        quantity_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_range_counters (
                        tenant_key INT,
                        shard_key INT,
                        next_sequence INT,
                        PRIMARY KEY (tenant_key, shard_key)
                    )",
            )
            .unwrap();
        session.execute("ANALYZE arbitrary_range_lines").unwrap();
        session.execute("ANALYZE arbitrary_range_stock").unwrap();
        session.execute("ANALYZE arbitrary_range_counters").unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_range_lines
                     (tenant_key, shard_key, sequence_key, line_key, item_key)
                     VALUES {line_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_range_stock
                     (tenant_key, item_key, quantity_value)
                     VALUES {stock_values}"
            ))
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_range_counters
                     (tenant_key, shard_key, next_sequence)
                     VALUES (7, 3, 1990), (8, 3, 1990)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .with_routine_vars(BTreeMap::from([
            ("chosen_tenant".to_string(), SqlValue::Int(7)),
            ("chosen_shard".to_string(), SqlValue::Int(3)),
            ("quantity_limit".to_string(), SqlValue::Int(10)),
        ]))
        .execute(
            "SELECT COUNT(DISTINCT (item.item_key))
                 FROM arbitrary_range_lines line_item,
                      arbitrary_range_stock item,
                      arbitrary_range_counters counter_row
                 WHERE line_item.tenant_key = chosen_tenant
                   AND line_item.shard_key = chosen_shard
                   AND counter_row.tenant_key = chosen_tenant
                   AND counter_row.shard_key = chosen_shard
                   AND line_item.sequence_key < counter_row.next_sequence
                   AND line_item.sequence_key >= (counter_row.next_sequence - 20)
                   AND item.tenant_key = chosen_tenant
                   AND item.item_key = line_item.item_key
                   AND item.quantity_value < quantity_limit",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(20)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
            stats.join_candidate_pairs <= 2500,
            "dynamic range bounds on a joined btree index should keep join candidates bounded, got {} candidate pairs",
            stats.join_candidate_pairs
        );
}

#[test]
fn primary_key_index_backfill_restores_executable_btree_for_existing_schema() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_existing_keys (
                        tenant_key INT,
                        sequence_key INT,
                        payload_text TEXT,
                        PRIMARY KEY (tenant_key, sequence_key)
                    )",
            )
            .unwrap();
    }

    let index_name = "arbitrary_existing_keys_pkey";
    assert!(db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(index_name)));
    db.drop_index(index_name).unwrap();
    assert!(!db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(index_name)));

    ensure_primary_key_indexes(&mut db).unwrap();
    let index = db
        .index_definitions()
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .unwrap();
    assert!(index.unique);
    assert!(index_field_lists_match(
        &index.fields,
        &[
            IndexField::MetadataPath(vec!["tenant_key".to_string()]),
            IndexField::MetadataPath(vec!["sequence_key".to_string()])
        ]
    ));
}

#[test]
fn unique_constraint_indexes_are_created_backfilled_and_follow_constraint_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_unique_keys (
                     id UUID PRIMARY KEY,
                     tenant_id UUID,
                     scan_id UUID,
                     CONSTRAINT arbitrary_scan_key UNIQUE (tenant_id, scan_id)
                 )",
            )
            .unwrap();
    }

    let index = db
        .index_definitions()
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case("arbitrary_scan_key"))
        .unwrap();
    assert!(index.unique);
    assert_eq!(index.kind, IndexKind::BTree);
    assert!(index_field_lists_match(
        &index.fields,
        &[
            IndexField::MetadataPath(vec!["tenant_id".to_string()]),
            IndexField::MetadataPath(vec!["scan_id".to_string()]),
        ]
    ));

    db.drop_index("arbitrary_scan_key").unwrap();
    ensure_unique_constraint_indexes(&mut db).unwrap();
    assert!(db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("arbitrary_scan_key")));

    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "ALTER TABLE arbitrary_unique_keys
                     RENAME CONSTRAINT arbitrary_scan_key TO renamed_scan_key",
            )
            .unwrap();
    }
    assert!(!db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("arbitrary_scan_key")));
    assert!(db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("renamed_scan_key")));

    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "ALTER TABLE arbitrary_unique_keys DROP CONSTRAINT renamed_scan_key;
                 ALTER TABLE arbitrary_unique_keys
                   ADD CONSTRAINT replacement_scan_key UNIQUE (scan_id)",
            )
            .unwrap();
    }
    assert!(!db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("renamed_scan_key")));
    assert!(db
        .index_definitions()
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case("replacement_scan_key")));
}

#[test]
fn comma_join_dynamic_range_uses_primary_key_btree_without_table_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let line_values = (1..=2000)
        .map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(7, 3, {seq_key}, {seq_key}, {item_key})")
        })
        .chain((1..=2000).map(|seq_key| {
            let item_key = (seq_key % 1000) + 1;
            format!("(8, 3, {seq_key}, {seq_key}, {item_key})")
        }))
        .collect::<Vec<_>>()
        .join(", ");
    let stock_values = (1..=1000)
        .map(|item_key| format!("(7, {item_key}, 5)"))
        .chain((1..=1000).map(|item_key| format!("(8, {item_key}, 5)")))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_pk_range_lines (
                        tenant_key INT,
                        shard_key INT,
                        sequence_key INT,
                        line_key INT,
                        item_key INT,
                        PRIMARY KEY (sequence_key, tenant_key, shard_key, line_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_pk_range_stock (
                        tenant_key INT,
                        item_key INT,
                        quantity_value INT,
                        PRIMARY KEY (tenant_key, item_key)
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE TABLE arbitrary_pk_range_counters (
                        tenant_key INT,
                        shard_key INT,
                        next_sequence INT,
                        PRIMARY KEY (tenant_key, shard_key)
                    )",
            )
            .unwrap();
        session.execute("ANALYZE arbitrary_pk_range_lines").unwrap();
        session.execute("ANALYZE arbitrary_pk_range_stock").unwrap();
        session
            .execute("ANALYZE arbitrary_pk_range_counters")
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pk_range_lines
                     (tenant_key, shard_key, sequence_key, line_key, item_key)
                     VALUES {line_values}"
            ))
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_pk_range_stock
                     (tenant_key, item_key, quantity_value)
                     VALUES {stock_values}"
            ))
            .unwrap();
        session
            .execute(
                "INSERT INTO arbitrary_pk_range_counters
                     (tenant_key, shard_key, next_sequence)
                     VALUES (7, 3, 1990), (8, 3, 1990)",
            )
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = SqlEngine::new(&db)
        .with_routine_vars(BTreeMap::from([
            ("chosen_tenant".to_string(), SqlValue::Int(7)),
            ("chosen_shard".to_string(), SqlValue::Int(3)),
            ("quantity_limit".to_string(), SqlValue::Int(10)),
        ]))
        .execute(
            "SELECT COUNT(DISTINCT (item.item_key))
                 FROM arbitrary_pk_range_lines line_item,
                      arbitrary_pk_range_stock item,
                      arbitrary_pk_range_counters counter_row
                 WHERE line_item.tenant_key = chosen_tenant
                   AND line_item.shard_key = chosen_shard
                   AND counter_row.tenant_key = chosen_tenant
                   AND counter_row.shard_key = chosen_shard
                   AND line_item.sequence_key < counter_row.next_sequence
                   AND line_item.sequence_key >= (counter_row.next_sequence - 20)
                   AND item.tenant_key = chosen_tenant
                   AND item.item_key = line_item.item_key
                   AND item.quantity_value < quantity_limit",
        )
        .unwrap();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    assert_eq!(result.rows, vec![vec![SqlValue::Int(20)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
            stats.join_candidate_pairs <= 50,
            "dynamic range bounds on a joined primary key should apply later key equalities before materialization, got {} candidate pairs",
            stats.join_candidate_pairs
        );
}

#[test]
fn indexed_update_reuses_target_schema_for_candidate_and_rls_checks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=32)
        .map(|row_id| format!("({row_id}, 7, 0)"))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_cached_update_rows (
                        row_id INT PRIMARY KEY,
                        lookup_key INT,
                        payload INT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_cached_update_lookup_idx
                     ON arbitrary_cached_update_rows (lookup_key)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_cached_update_rows (row_id, lookup_key, payload)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "UPDATE arbitrary_cached_update_rows
                     SET payload = payload + 1
                     WHERE lookup_key = 7",
            )
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let result = SqlEngine::new(&db)
        .execute("SELECT SUM(payload) FROM arbitrary_cached_update_rows")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(32)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.schema_loads <= 3,
        "indexed UPDATE should reuse statement schema, got {} schema loads",
        stats.schema_loads
    );
}

#[test]
fn indexed_delete_reuses_target_schema_for_candidate_checks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let values = (1..=32)
        .map(|row_id| format!("({row_id}, 11, 'remove')"))
        .chain((33..=64).map(|row_id| format!("({row_id}, 12, 'keep')")))
        .collect::<Vec<_>>()
        .join(", ");
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE arbitrary_cached_delete_rows (
                        row_id INT PRIMARY KEY,
                        lookup_key INT,
                        payload TEXT
                    )",
            )
            .unwrap();
        session
            .execute(
                "CREATE INDEX arbitrary_cached_delete_lookup_idx
                     ON arbitrary_cached_delete_rows (lookup_key)",
            )
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO arbitrary_cached_delete_rows (row_id, lookup_key, payload)
                     VALUES {values}"
            ))
            .unwrap();
    }

    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("DELETE FROM arbitrary_cached_delete_rows WHERE lookup_key = 11")
            .unwrap();
    }
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());

    let result = SqlEngine::new(&db)
        .execute("SELECT COUNT(*) FROM arbitrary_cached_delete_rows")
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(32)]]);
    assert_eq!(stats.full_scan_count, 0);
    assert!(stats.index_lookup_count > 0);
    assert!(
        stats.schema_loads <= 3,
        "indexed DELETE should reuse statement schema, got {} schema loads",
        stats.schema_loads
    );
}

#[test]
fn pg_class_pushdown_follows_pg_index_join_to_filtered_table_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE catalog_noise_{idx} (id INT PRIMARY KEY, value INT)"
                ))
                .unwrap();
            session
                .execute(&format!(
                    "CREATE INDEX idx_catalog_noise_{idx}_value ON catalog_noise_{idx}(value)"
                ))
                .unwrap();
        }
        session
            .execute("CREATE TABLE catalog_target (id INT PRIMARY KEY, first INT, second INT)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_catalog_target_first ON catalog_target(first)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_catalog_target_second ON catalog_target(second)")
            .unwrap();
    }

    let selection = select_selection_with_join_constraints(
        "SELECT DISTINCT i.relname
             FROM pg_class t
             INNER JOIN pg_index d ON t.oid = d.indrelid
             INNER JOIN pg_class i ON d.indexrelid = i.oid
             LEFT JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE i.relkind IN ('i', 'I')
               AND d.indisprimary = 'f'
               AND t.relname = 'catalog_target'
               AND n.nspname = 'public'",
    );
    let rows = virtual_rows_with_selection(&db, "pg_class", "i", Some(&selection)).unwrap();
    let names = rows
        .iter()
        .map(|row| match virtual_cell(row, "relname") {
            SqlValue::String(value) => value,
            value => panic!("expected relname string, got {value:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "idx_catalog_target_first".to_string(),
            "idx_catalog_target_second".to_string(),
        ]
    );
}

#[test]
fn view_filters_fold_concat_current_schema_constants() {
    let selection = select_selection_with_join_constraints(
        "SELECT 1
             FROM postgres_partitions
             WHERE identifier = concat(current_schema(), '.', 'catalog_target')",
    );
    let filters = view_string_filters_from_selection(
        Some(&selection),
        "postgres_partitions",
        "postgres_partitions",
        &["identifier"],
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        filters,
        ["public.catalog_target".to_string()]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
}

#[test]
fn pg_constraint_pushdown_follows_pg_class_join_to_filtered_relation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                    .execute(&format!(
                        "CREATE TABLE constraint_noise_{idx} (id INT PRIMARY KEY, value INT CHECK (value >= 0))"
                    ))
                    .unwrap();
        }
        session
            .execute(
                "CREATE TABLE constraint_target (
                        id INT PRIMARY KEY,
                        value INT,
                        CONSTRAINT check_constraint_target_value CHECK (value >= 0)
                    )",
            )
            .unwrap();
    }

    let selection = select_selection_with_join_constraints(
        "SELECT COUNT(*)
             FROM pg_catalog.pg_constraint con
             INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
             INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
             WHERE con.contype = 'c'
               AND con.conname = 'check_constraint_target_value'
               AND nsp.nspname = 'public'
               AND rel.relname = 'constraint_target'",
    );
    let rows = virtual_rows_with_selection(&db, "pg_constraint", "con", Some(&selection)).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        virtual_cell(&rows[0], "conname"),
        SqlValue::String("check_constraint_target_value".to_string())
    );
}

#[test]
fn active_record_foreign_key_catalog_join_filters_before_materializing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE fk_parent_target (id INT PRIMARY KEY)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE fk_child_target (
                        id INT PRIMARY KEY,
                        parent_id INT,
                        CONSTRAINT fk_child_target_parent
                            FOREIGN KEY (parent_id) REFERENCES fk_parent_target(id)
                    )",
            )
            .unwrap();
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE fk_parent_noise_{idx} (id INT PRIMARY KEY)"
                ))
                .unwrap();
            session
                .execute(&format!(
                    "CREATE TABLE fk_child_noise_{idx} (
                            id INT PRIMARY KEY,
                            parent_id INT,
                            CONSTRAINT fk_child_noise_{idx}_parent
                                FOREIGN KEY (parent_id) REFERENCES fk_parent_noise_{idx}(id)
                        )"
                ))
                .unwrap();
        }
    }

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT t2.oid::regclass::text AS to_table,
                        a1.attname AS column,
                        a2.attname AS primary_key,
                        c.conname AS name,
                        c.confupdtype AS on_update,
                        c.confdeltype AS on_delete,
                        c.convalidated AS valid,
                        c.condeferrable AS deferrable,
                        c.condeferred AS deferred,
                        c.conkey,
                        c.confkey,
                        c.conrelid,
                        c.confrelid
                 FROM pg_constraint c
                 JOIN pg_class t1 ON c.conrelid = t1.oid
                 JOIN pg_class t2 ON c.confrelid = t2.oid
                 JOIN pg_attribute a1 ON a1.attnum = c.conkey[1] AND a1.attrelid = t1.oid
                 JOIN pg_attribute a2 ON a2.attnum = c.confkey[1] AND a2.attrelid = t2.oid
                 JOIN pg_namespace t3 ON c.connamespace = t3.oid
                 WHERE c.contype = 'f'
                   AND t1.relname = 'fk_child_target'
                   AND t3.nspname = 'public'
                 ORDER BY c.conname",
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        SqlValue::String("fk_parent_target".to_string())
    );
    assert_eq!(result.rows[0][1], SqlValue::String("parent_id".to_string()));
    assert_eq!(result.rows[0][2], SqlValue::String("id".to_string()));
    assert_eq!(
        result.rows[0][3],
        SqlValue::String("fk_child_target_parent".to_string())
    );
}

#[test]
fn postgres_triggers_view_filters_catalog_rows_before_materializing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE FUNCTION trigger_fast_path_fn() RETURNS trigger LANGUAGE plpgsql AS $$
                     BEGIN
                       RETURN NULL;
                     END
                     $$",
            )
            .unwrap();
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE trigger_noise_{idx} (id INT PRIMARY KEY)"
                ))
                .unwrap();
            session
                .execute(&format!(
                    "CREATE TRIGGER trigger_noise_{idx}_after_delete
                         AFTER DELETE ON trigger_noise_{idx}
                         FOR EACH ROW
                         EXECUTE FUNCTION trigger_fast_path_fn()"
                ))
                .unwrap();
        }
        session
            .execute("CREATE TABLE trigger_target (id INT PRIMARY KEY)")
            .unwrap();
        session
            .execute(
                "CREATE TRIGGER trigger_target_after_delete
                     AFTER DELETE ON trigger_target
                     FOR EACH ROW
                     EXECUTE FUNCTION trigger_fast_path_fn()",
            )
            .unwrap();
        session
            .execute(
                "CREATE OR REPLACE VIEW postgres_triggers AS
                     SELECT
                       CONCAT(nsp.nspname, '.', rel.relname, '.', trgr.tgname) AS identifier,
                       trgr.tgname AS trigger_name,
                       rel.relname AS table_name,
                       nsp.nspname AS schema_name,
                       proc.proname AS function_name
                     FROM pg_catalog.pg_trigger trgr
                       INNER JOIN pg_catalog.pg_class rel
                         ON trgr.tgrelid = rel.oid
                       INNER JOIN pg_catalog.pg_namespace nsp
                         ON nsp.oid = rel.relnamespace
                       LEFT JOIN pg_catalog.pg_proc proc
                         ON trgr.tgfoid = proc.oid
                     WHERE NOT trgr.tgisinternal
                       AND nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    let exists = session
        .execute(
            "SELECT 1 AS one
                 FROM \"postgres_triggers\"
                 WHERE \"postgres_triggers\".\"table_name\" = 'trigger_target'
                   AND \"postgres_triggers\".\"trigger_name\" = 'trigger_target_after_delete'
                   AND \"postgres_triggers\".\"schema_name\" = 'public'
                 LIMIT 1",
        )
        .unwrap();
    assert_eq!(exists.rows, vec![vec![SqlValue::Int(1)]]);

    let functions = session
        .execute(
            "SELECT DISTINCT \"postgres_triggers\".\"function_name\"
                 FROM \"postgres_triggers\"
                 WHERE \"postgres_triggers\".\"table_name\" = 'trigger_target'
                   AND (schema_name = current_schema())",
        )
        .unwrap();
    assert_eq!(
        functions.rows,
        vec![vec![SqlValue::String("trigger_fast_path_fn".to_string())]]
    );
}

#[test]
fn check_constraint_catalog_count_join_filters_to_target_relation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE check_noise_{idx} (
                            id INT PRIMARY KEY,
                            value INT,
                            CONSTRAINT check_noise_{idx}_value CHECK (value >= 0)
                        )"
                ))
                .unwrap();
        }
        session
            .execute(
                "CREATE TABLE check_target (
                        id INT PRIMARY KEY,
                        value INT,
                        CONSTRAINT check_target_value CHECK (value >= 0)
                    )",
            )
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT COUNT(*)
                 FROM pg_catalog.pg_constraint con
                 INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
                 INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
                 WHERE con.contype = 'c'
                   AND con.conname = 'check_target_value'
                   AND nsp.nspname = 'public'
                   AND rel.relname = 'check_target'",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Int(1)]]);
}

#[test]
fn index_validity_catalog_join_filters_by_index_relation_name() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE invalid_index_noise_{idx} (id INT PRIMARY KEY, value INT)"
                ))
                .unwrap();
            session
                    .execute(&format!(
                        "CREATE INDEX idx_invalid_index_noise_{idx}_value ON invalid_index_noise_{idx}(value)"
                    ))
                    .unwrap();
        }
        session
            .execute("CREATE TABLE invalid_index_target (id INT PRIMARY KEY, value INT)")
            .unwrap();
        session
            .execute("CREATE INDEX idx_invalid_index_target_value ON invalid_index_target(value)")
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT NOT i.indisvalid
                 FROM pg_class c
                 INNER JOIN pg_index i
                   ON c.oid = i.indexrelid
                 INNER JOIN pg_namespace n
                   ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema()
                   AND c.relname = 'idx_invalid_index_target_value'",
        )
        .unwrap();

    assert_eq!(result.rows, vec![vec![SqlValue::Bool(false)]]);
}

#[test]
fn active_record_tables_catalog_join_lists_public_relations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE table_list_noise_{idx} (id INT PRIMARY KEY)"
                ))
                .unwrap();
            session
                .execute(&format!(
                    "CREATE INDEX idx_table_list_noise_{idx}_id ON table_list_noise_{idx}(id)"
                ))
                .unwrap();
        }
        session
            .execute("CREATE TABLE table_list_target (id INT PRIMARY KEY)")
            .unwrap();
    }

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT c.relname
                 FROM pg_class c
                 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = ANY (current_schemas(false))
                   AND c.relkind IN ('r','p')
                 ORDER BY c.relname",
        )
        .unwrap();
    let names = result
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::String(value) => value.clone(),
            value => panic!("expected table name, got {value:?}"),
        })
        .collect::<Vec<_>>();

    assert!(names.contains(&"table_list_target".to_string()));
    assert!(!names.contains(&"idx_table_list_noise_0_id".to_string()));
}

#[test]
fn string_filters_extract_active_record_current_schemas_any() {
    let selection = select_selection_with_join_constraints(
        "SELECT c.relname
             FROM pg_class c
             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = ANY (current_schemas(false))
               AND c.relkind IN ('r','v','m','p','f')",
    );

    let schema_names =
        string_filter_values_from_selection(Some(&selection), "n", "pg_namespace", &["nspname"])
            .unwrap();
    assert_eq!(schema_names, Some(BTreeSet::from(["public".to_string()])));

    let relkinds =
        string_filter_values_from_selection(Some(&selection), "c", "pg_class", &["relkind"])
            .unwrap()
            .unwrap();
    assert!(relkinds.contains("r"));
    assert!(relkinds.contains("v"));
    assert!(relkinds.contains("m"));
    assert!(relkinds.contains("p"));
    assert!(relkinds.contains("f"));
}

#[test]
fn string_filters_do_not_push_down_not_in_as_positive_filter() {
    let selection = select_selection_with_join_constraints(
        "SELECT c.relname
             FROM pg_class c
             WHERE c.relkind NOT IN ('i')",
    );

    let relkinds =
        string_filter_values_from_selection(Some(&selection), "c", "pg_class", &["relkind"])
            .unwrap();

    assert!(relkinds.is_none());
}

#[test]
fn pg_locks_class_join_returns_empty_without_scanning_catalog_relations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE lock_noise_{idx} (id INT PRIMARY KEY)"
                ))
                .unwrap();
        }
    }

    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT DISTINCT relation::regclass AS table_name
                 FROM pg_locks
                 JOIN pg_class ON pg_locks.relation = pg_class.oid
                 WHERE relation IS NOT NULL
                   AND pg_class.relkind IN ('r', 'p')
                   AND pid = pg_backend_pid()
                   AND relation::regclass::text NOT LIKE 'pg_%'
                   AND relation::regclass::text NOT LIKE 'information_schema.%'
                   AND relation::regclass::text NOT IN ('schema_migrations', 'ar_internal_metadata')
                   AND mode NOT IN ('RowShareLock', 'AccessShareLock')",
        )
        .unwrap();

    assert_eq!(result.rows, Vec::<Vec<SqlValue>>::new());
}

#[test]
fn pg_class_pushdown_follows_joined_relid_to_regclass_filter() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for idx in 0..24 {
            session
                .execute(&format!(
                    "CREATE TABLE stat_noise_{idx} (id INT PRIMARY KEY, value INT)"
                ))
                .unwrap();
        }
        session
            .execute("CREATE TABLE stat_target (id INT PRIMARY KEY, value INT)")
            .unwrap();
    }

    let selection = select_selection_with_join_constraints(
        "SELECT pg_class.*
             FROM pg_class
             LEFT JOIN pg_stat_user_tables
               ON pg_stat_user_tables.relid = pg_class.oid
             WHERE pg_stat_user_tables.relid = to_regclass('stat_target')",
    );
    let rows = virtual_rows_with_selection(&db, "pg_class", "pg_class", Some(&selection)).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        virtual_cell(&rows[0], "relname"),
        SqlValue::String("stat_target".to_string())
    );
}

fn select_selection_with_join_constraints(sql: &str) -> Expr {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query");
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected select");
    };
    let mut selection = select.selection.clone().unwrap();
    for from in &select.from {
        for join in &from.joins {
            if let Some(constraint) = join_operator_constraint(&join.join_operator) {
                if let Some(combined) = selection_with_join_constraint(Some(&selection), constraint)
                {
                    selection = combined;
                }
            }
        }
    }
    selection
}

// ---------------------------------------------------------------------------
// Bound-plan cache (BICDB_PLAN_CACHE) differential tests.
//
// The fused `execute_row_query` path is the ORACLE. For each query shape we run
// it with the plan-cache gate OFF and ON (twice on, to exercise build-then-hit)
// and assert byte-identical `SqlResult`s. The fast path must either reproduce
// the fused result exactly or fall back to it — never return a different result.
// ---------------------------------------------------------------------------

#[test]
fn plan_cache_matches_fused_path_across_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE customer (\
               id BIGINT PRIMARY KEY, \
               name TEXT, \
               bucket INT, \
               balance DECIMAL(12,2));",
        )
        .unwrap();
    // Composite primary key.
    session
        .execute(
            "CREATE TABLE district (\
               w_id BIGINT, \
               d_id BIGINT, \
               tax DECIMAL(6,4), \
               PRIMARY KEY (w_id, d_id));",
        )
        .unwrap();
    // Text primary key.
    session
        .execute("CREATE TABLE item (sku TEXT PRIMARY KEY, price DECIMAL(8,2));")
        .unwrap();
    // Secondary (non-PK) index — point lookups on `code` must FALL BACK (not be
    // cached), but still match the fused path.
    session
        .execute("CREATE TABLE widget (id BIGINT PRIMARY KEY, code INT, label TEXT);")
        .unwrap();
    session
        .execute("CREATE INDEX widget_code_idx ON widget (code);")
        .unwrap();

    session
        .execute(
            "INSERT INTO customer (id, name, bucket, balance) VALUES \
               (1, 'alice', 5, 100.25), \
               (2, 'bob', 5, 200.50), \
               (3, NULL, 7, 300.00);",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO district (w_id, d_id, tax) VALUES \
               (1, 1, 0.0500), (1, 2, 0.0600), (2, 1, 0.0700);",
        )
        .unwrap();
    session
        .execute("INSERT INTO item (sku, price) VALUES ('A-1', 9.99), ('B-2', 19.99);")
        .unwrap();
    session
        .execute(
            "INSERT INTO widget (id, code, label) VALUES \
               (10, 100, 'x'), (11, 100, 'y'), (12, 200, 'z');",
        )
        .unwrap();

    let cases = [
        // (label, sql, expected_to_be_cached)
        (
            "point lookup hit",
            "SELECT id, name, balance FROM customer WHERE id = 2",
            true,
        ),
        (
            "point lookup no-match",
            "SELECT id, name FROM customer WHERE id = 9999",
            true,
        ),
        (
            "point lookup null col",
            "SELECT id, name FROM customer WHERE id = 3",
            true,
        ),
        (
            "select star pk",
            "SELECT * FROM customer WHERE id = 1",
            true,
        ),
        (
            "expr projection pk",
            "SELECT id, balance + 1 AS b1 FROM customer WHERE id = 1",
            true,
        ),
        (
            "multi-column pk",
            "SELECT w_id, d_id, tax FROM district WHERE w_id = 1 AND d_id = 2",
            true,
        ),
        (
            "multi-column pk reversed",
            "SELECT tax FROM district WHERE d_id = 1 AND w_id = 2",
            true,
        ),
        (
            "text pk",
            "SELECT sku, price FROM item WHERE sku = 'B-2'",
            true,
        ),
        // Shapes that must FALL BACK (fused path is the oracle either way):
        (
            "partial composite pk",
            "SELECT tax FROM district WHERE w_id = 1",
            false,
        ),
        (
            "pk with residual",
            "SELECT name FROM customer WHERE id = 2 AND bucket = 5",
            false,
        ),
        (
            "range predicate",
            "SELECT id FROM customer WHERE id > 1",
            false,
        ),
        (
            "secondary index",
            "SELECT id, label FROM widget WHERE code = 100",
            false,
        ),
        (
            "non-key predicate",
            "SELECT id FROM customer WHERE bucket = 5",
            false,
        ),
        (
            "order by + limit pk",
            "SELECT id, name FROM customer WHERE id = 1 ORDER BY name LIMIT 1",
            true,
        ),
    ];

    for (label, sql, _expect_cached) in cases {
        crate::plan_cache::set_test_override(Some(false));
        let oracle = session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{label}: oracle err {e:?}"));

        crate::plan_cache::set_test_override(Some(true));
        let cached_build = session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{label}: build err {e:?}"));
        let cached_hit = session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{label}: hit err {e:?}"));
        crate::plan_cache::set_test_override(None);

        assert_eq!(
            oracle.columns, cached_build.columns,
            "{label}: build columns"
        );
        assert_eq!(oracle.rows, cached_build.rows, "{label}: build rows");
        assert_eq!(oracle.columns, cached_hit.columns, "{label}: hit columns");
        assert_eq!(oracle.rows, cached_hit.rows, "{label}: hit rows");
    }
}

#[test]
fn plan_cache_invalidates_after_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    crate::plan_cache::set_test_override(Some(true));

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, a INT);")
        .unwrap();
    session
        .execute("INSERT INTO t (id, a) VALUES (1, 10);")
        .unwrap();

    let r1 = session.execute("SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(r1.rows, vec![vec![SqlValue::Int(1), SqlValue::Int(10)]]);

    // Add a column: the schema generation bumps, the cached plan (and its embedded
    // schema / output columns) must be discarded so the new column appears.
    session.execute("ALTER TABLE t ADD COLUMN b INT;").unwrap();
    session
        .execute("UPDATE t SET b = 99 WHERE id = 1;")
        .unwrap();

    let r2 = session.execute("SELECT * FROM t WHERE id = 1").unwrap();
    crate::plan_cache::set_test_override(None);
    assert_eq!(
        r2.columns.len(),
        3,
        "post-ALTER projection must include the new column: {:?}",
        r2.columns
    );
    assert_eq!(
        r2.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Int(10), SqlValue::Int(99)]]
    );
}

#[test]
fn limit_offset_coerce_string_float_and_null_like_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE lt (id INT PRIMARY KEY);")
        .unwrap();
    for id in 1..=5 {
        session
            .execute(&format!("INSERT INTO lt (id) VALUES ({id});"))
            .unwrap();
    }

    // Plain integer literal still works.
    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT 2")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]);

    // String literal LIMIT/OFFSET — what a text/unknown-typed bind parameter
    // (`LIMIT $1` = 500) is substituted to. PostgreSQL coerces these.
    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT '2'")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]);

    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT '2' OFFSET '1'")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(3)]]);

    let r = session
        .execute("SELECT id FROM lt ORDER BY id OFFSET '3'")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(4)], vec![SqlValue::Int(5)]]);

    // A LIMIT larger than the row count returns everything (the reported "500" case).
    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT '500'")
        .unwrap();
    assert_eq!(r.rows.len(), 5);

    // NULL LIMIT is unbounded; NULL OFFSET is zero (PostgreSQL semantics).
    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT NULL")
        .unwrap();
    assert_eq!(r.rows.len(), 5);

    let r = session
        .execute("SELECT id FROM lt ORDER BY id OFFSET NULL")
        .unwrap();
    assert_eq!(r.rows.len(), 5);

    // Float literal rounds (PostgreSQL coerces to bigint).
    let r = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT 2.0")
        .unwrap();
    assert_eq!(r.rows.len(), 2);

    // A non-numeric string is still rejected.
    let err = session
        .execute("SELECT id FROM lt ORDER BY id LIMIT 'abc'")
        .unwrap_err();
    assert!(
        matches!(err, SqlError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
}

#[test]
fn column_compression_matches_postgres_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE compression_target (plain_text text, pglz_text text COMPRESSION pglz, lz4_text text COMPRESSION lz4)",
            )
            .unwrap();

        let schema = load_schema(session.db_ref(), "compression_target")
            .unwrap()
            .unwrap();
        assert_eq!(schema.column("plain_text").unwrap().compression, None);
        assert_eq!(schema.column("pglz_text").unwrap().compression, Some('p'));
        assert_eq!(schema.column("lz4_text").unwrap().compression, Some('l'));

        session
            .execute("ALTER TABLE compression_target ALTER COLUMN pglz_text SET COMPRESSION lz4")
            .unwrap();
        session
            .execute("ALTER TABLE compression_target ALTER COLUMN lz4_text SET COMPRESSION default")
            .unwrap();
    }

    let db = BicDb::open(dir.path()).unwrap();
    let schema = load_schema(&db, "compression_target").unwrap().unwrap();
    assert_eq!(schema.column("pglz_text").unwrap().compression, Some('l'));
    assert_eq!(schema.column("lz4_text").unwrap().compression, None);
    let rows = pg_attribute_rows_for_columns(42, schema.columns);
    let compression = rows
        .into_iter()
        .map(|row| (row["attname"].clone(), row["attcompression"].clone()))
        .collect::<Vec<_>>();
    assert!(compression.contains(&(
        SqlValue::String("pglz_text".to_string()),
        SqlValue::String("l".to_string())
    )));
    assert!(compression.contains(&(
        SqlValue::String("lz4_text".to_string()),
        SqlValue::String(String::new())
    )));
}

#[test]
fn column_compression_rejects_unsupported_types_and_methods() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let error = session
        .execute("CREATE TABLE compression_fixed (value int4 COMPRESSION pglz)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");

    let error = session
        .execute("CREATE TABLE compression_unknown (value text COMPRESSION unknown_method)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");

    session
        .execute("CREATE TABLE compression_alter_fixed (value int4)")
        .unwrap();
    let error = session
        .execute("ALTER TABLE compression_alter_fixed ALTER COLUMN value SET COMPRESSION lz4")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
}

#[test]
fn anonymous_do_applies_independent_constraint_guards() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE delegation_do_guards (id INT PRIMARY KEY)")
        .unwrap();
    let sql = r#"DO $constraints$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'delegation_positive') THEN
        ALTER TABLE delegation_do_guards ADD CONSTRAINT delegation_positive CHECK (id > 0);
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'delegation_small') THEN
        ALTER TABLE delegation_do_guards ADD CONSTRAINT delegation_small CHECK (CASE WHEN id < 10 THEN true ELSE false END);
      END IF;
    END $constraints$"#;
    session.execute(sql).unwrap();
    session.execute(sql).unwrap();
    session
        .execute("INSERT INTO delegation_do_guards VALUES (1)")
        .unwrap();
    assert!(session
        .execute("INSERT INTO delegation_do_guards VALUES (0)")
        .is_err());
    assert!(session
        .execute("INSERT INTO delegation_do_guards VALUES (11)")
        .is_err());
}

#[test]
fn anonymous_do_raise_using_preserves_message_and_sqlstate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute(
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = '100% denied'; END $$",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    assert!(error.to_string().contains("100% denied"));
    assert!(session
        .execute("DO $$ BEGIN RAISE EXCEPTION 'first' USING MESSAGE = 'second'; END $$")
        .is_err());
}

#[test]
fn membership_options_separate_inherited_privilege_from_set_role() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE membership_worker NOLOGIN NOINHERIT",
        "CREATE ROLE membership_app LOGIN INHERIT",
        "CREATE TABLE membership_secret (id INT)",
        "INSERT INTO membership_secret VALUES (1)",
        "ALTER TABLE membership_secret OWNER TO membership_worker",
        "GRANT membership_worker TO membership_app WITH INHERIT FALSE, SET TRUE",
        "SET SESSION AUTHORIZATION membership_app",
    ] {
        session.execute(sql).unwrap();
    }
    assert!(session.execute("SELECT * FROM membership_secret").is_err());
    assert!(session
        .execute("ALTER TABLE membership_secret ADD COLUMN forbidden INT")
        .is_err());
    let capabilities = session.execute("SELECT pg_has_role('membership_worker', 'MEMBER'), pg_has_role('membership_worker', 'USAGE'), pg_has_role('membership_worker', 'SET')").unwrap();
    assert_eq!(
        capabilities.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true)
        ]]
    );
    session.execute("SET ROLE membership_worker").unwrap();
    assert_eq!(
        session
            .execute("SELECT * FROM membership_secret")
            .unwrap()
            .rows
            .len(),
        1
    );
    session.execute("RESET ROLE").unwrap();
    assert!(session.execute("SELECT * FROM membership_secret").is_err());
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT membership_worker TO membership_app WITH INHERIT TRUE, SET FALSE")
        .unwrap();
    // Repeating a grant without options preserves explicit restrictions.
    session
        .execute("GRANT membership_worker TO membership_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION membership_app")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT * FROM membership_secret")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(session.execute("SET ROLE membership_worker").is_err());
    let capabilities = session.execute("SELECT pg_has_role('membership_worker', 'MEMBER'), pg_has_role('membership_worker', 'USAGE'), pg_has_role('membership_worker', 'SET')").unwrap();
    assert_eq!(
        capabilities.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false)
        ]]
    );
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    let rows = session
        .execute("SELECT inherit_option, set_option FROM pg_auth_members")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
    session
        .execute("CREATE ROLE membership_leaf NOLOGIN")
        .unwrap();
    session
        .execute("GRANT membership_leaf TO membership_worker WITH SET TRUE, INHERIT TRUE")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION membership_app")
        .unwrap();
    assert!(session.execute("SET ROLE membership_leaf").is_err());
    let capabilities = session.execute("SELECT pg_has_role('membership_leaf', 'MEMBER'), pg_has_role('membership_leaf', 'USAGE'), pg_has_role('membership_leaf', 'SET')").unwrap();
    assert_eq!(
        capabilities.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false)
        ]]
    );
}

#[test]
fn membership_identity_keywords_resolve_the_effective_grantor_and_member() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE membership_admin LOGIN SUPERUSER",
        "CREATE ROLE membership_executor NOLOGIN",
        "SET SESSION AUTHORIZATION membership_admin",
        "GRANT membership_executor TO CURRENT_USER WITH INHERIT FALSE, SET TRUE",
    ] {
        session.execute(sql).unwrap();
    }
    let rows = session.execute("SELECT pg_get_userbyid(member), pg_get_userbyid(grantor), inherit_option, set_option FROM pg_auth_members").unwrap().rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::String("membership_admin".into()),
            SqlValue::String("bicdb".into()),
            SqlValue::Bool(false),
            SqlValue::Bool(true)
        ]]
    );
    assert!(session
        .execute("GRANT membership_executor TO CURRENT_USER WITH SET TRUE, SET FALSE")
        .is_err());
    assert!(session
        .execute("GRANT membership_executor TO CURRENT_USER WITH INHERIT banana")
        .is_err());
}

#[test]
fn function_grants_resolve_session_user_and_require_grantor_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE function_grant_admin LOGIN SUPERUSER",
        "CREATE ROLE function_grant_app LOGIN",
        "CREATE FUNCTION grant_probe() RETURNS INT LANGUAGE SQL AS $$ SELECT 42 $$",
        "REVOKE EXECUTE ON FUNCTION grant_probe() FROM PUBLIC",
        "SET SESSION AUTHORIZATION function_grant_admin",
        "GRANT EXECUTE ON FUNCTION grant_probe() TO SESSION_USER WITH GRANT OPTION",
    ] {
        session.execute(sql).unwrap();
    }
    assert!(session
        .execute("GRANT EXECUTE ON FUNCTION grant_probe() TO function_grant_app WITH GRANT OPTION")
        .is_err());
    session
        .execute("SET SESSION AUTHORIZATION function_grant_app")
        .unwrap();
    assert!(session.execute("SELECT grant_probe()").is_err());
    assert!(session
        .execute("GRANT EXECUTE ON FUNCTION grant_probe() TO CURRENT_USER")
        .is_err());
    assert!(session.execute("DO $$ BEGIN EXECUTE 'GRANT EXECUTE ON FUNCTION grant_probe() TO CURRENT_USER WITH GRANT OPTION'; END $$").is_err());
    assert!(session
        .execute("DO $$ BEGIN EXECUTE 'REVOKE ALL ON FUNCTION grant_probe() FROM PUBLIC'; END $$")
        .is_err());
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION grant_probe() TO function_grant_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION function_grant_app")
        .unwrap();
    assert_eq!(
        session.execute("SELECT grant_probe()").unwrap().rows,
        vec![vec![SqlValue::Int(42)]]
    );
}

#[test]
fn dollar_quoted_literals_preserve_text_and_dynamic_constraint_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let value = session
        .execute(r"SELECT $text$don't \ escape$text$")
        .unwrap();
    assert_eq!(
        value.rows,
        vec![vec![SqlValue::String(r"don't \ escape".into())]]
    );
    session
        .execute("CREATE TABLE dollar_guard (id INT)")
        .unwrap();
    session.execute("DO $$ DECLARE clause TEXT := $guard$id > 0$guard$; BEGIN EXECUTE format('ALTER TABLE dollar_guard ADD CHECK (%s)', clause); END $$").unwrap();
    session
        .execute("INSERT INTO dollar_guard VALUES (1)")
        .unwrap();
    assert!(session
        .execute("INSERT INTO dollar_guard VALUES (0)")
        .is_err());
}

#[test]
fn format_regprocedure_uses_the_function_identity_in_dynamic_grants() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE signature_reader LOGIN",
        "CREATE SCHEMA signature_private",
        "CREATE FUNCTION signature_private.signature_probe(x INT) RETURNS INT LANGUAGE SQL AS $$ SELECT x $$",
        "DO $$ BEGIN EXECUTE 'REVOKE ALL ON FUNCTION signature_private.signature_probe(INT) FROM PUBLIC'; END $$",
        "DO $$ BEGIN EXECUTE 'GRANT EXECUTE ON FUNCTION signature_private.signature_probe(INT) TO SESSION_USER WITH GRANT OPTION'; END $$",
        "DO $$ DECLARE f RECORD; BEGIN FOR f IN SELECT oid FROM pg_proc WHERE proname = 'signature_probe' LOOP EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO signature_reader', f.oid::regprocedure); END LOOP; END $$",
        "SET SESSION AUTHORIZATION signature_reader",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert_eq!(
        session
            .execute("SELECT signature_private.signature_probe(42)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(42)]]
    );
}

#[test]
fn column_update_grants_do_not_expand_to_table_access() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE column_writer LOGIN",
        "CREATE TABLE column_guard (id INT PRIMARY KEY, title TEXT, secret TEXT)",
        "INSERT INTO column_guard VALUES (1, 'old', 'private')",
        "GRANT SELECT, INSERT ON column_guard TO column_writer",
        "GRANT UPDATE(title) ON column_guard TO column_writer",
        "SET SESSION AUTHORIZATION column_writer",
        "UPDATE column_guard SET title = 'new' WHERE id = 1",
        "INSERT INTO column_guard VALUES (1, 'upsert', 'hidden') ON CONFLICT(id) DO UPDATE SET title = excluded.title",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    for sql in [
        "UPDATE column_guard SET secret = 'exposed' WHERE id = 1",
        "UPDATE column_guard SET title = 'bad', secret = 'exposed' WHERE id = 1",
        "INSERT INTO column_guard VALUES (1, 'bad', 'exposed') ON CONFLICT(id) DO UPDATE SET secret = excluded.secret",
        "GRANT UPDATE(secret) ON column_guard TO column_writer",
    ] { assert!(session.execute(sql).is_err(), "{sql}"); }
    assert_eq!(
        session
            .execute("SELECT has_table_privilege('column_writer', 'column_guard', 'UPDATE')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    assert_eq!(
        session
            .execute("SELECT title, secret FROM column_guard")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("upsert".into()),
            SqlValue::String("private".into())
        ]]
    );
    for sql in [
        "RESET SESSION AUTHORIZATION",
        "ALTER TABLE column_guard RENAME COLUMN title TO label",
        "SET SESSION AUTHORIZATION column_writer",
        "UPDATE column_guard SET label = 'renamed' WHERE id = 1",
        "RESET SESSION AUTHORIZATION",
        "REVOKE UPDATE(label) ON column_guard FROM column_writer",
        "SET SESSION AUTHORIZATION column_writer",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(session
        .execute("UPDATE column_guard SET label = 'denied'")
        .is_err());
    for sql in [
        "RESET SESSION AUTHORIZATION",
        "GRANT UPDATE(label) ON column_guard TO column_writer",
        "ALTER TABLE column_guard DROP COLUMN label",
        "ALTER TABLE column_guard ADD COLUMN label TEXT",
        "SET SESSION AUTHORIZATION column_writer",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(session
        .execute("UPDATE column_guard SET label = 'denied'")
        .is_err());
}

#[test]
fn column_grants_follow_table_ddl_and_transactional_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE column_ddl_writer LOGIN",
        "CREATE TABLE column_ddl_guard (id INT, title TEXT)",
        "INSERT INTO column_ddl_guard VALUES (1, 'old')",
        "GRANT UPDATE(title) ON column_ddl_guard TO column_ddl_writer",
        "BEGIN",
        "REVOKE UPDATE ON column_ddl_guard FROM column_ddl_writer",
        "ROLLBACK",
        "ALTER TABLE column_ddl_guard RENAME TO renamed_column_guard",
        "SET SESSION AUTHORIZATION column_ddl_writer",
        "UPDATE renamed_column_guard SET title = 'allowed'",
        "RESET SESSION AUTHORIZATION",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let acl = session.execute("SELECT attacl FROM pg_attribute WHERE attrelid = 'renamed_column_guard'::regclass AND attname = 'title'").unwrap();
    assert_eq!(acl.rows.len(), 1);
    assert!(acl.rows[0][0].to_cell().contains("column_ddl_writer=w"));
    session
        .execute("REVOKE UPDATE ON renamed_column_guard FROM column_ddl_writer")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION column_ddl_writer")
        .unwrap();
    assert!(session
        .execute("UPDATE renamed_column_guard SET title = 'denied'")
        .is_err());
    for sql in [
        "RESET SESSION AUTHORIZATION",
        "GRANT UPDATE(title) ON renamed_column_guard TO column_ddl_writer",
        "DROP TABLE renamed_column_guard",
        "CREATE TABLE renamed_column_guard (id INT, title TEXT)",
        "SET SESSION AUTHORIZATION column_ddl_writer",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(session
        .execute("UPDATE renamed_column_guard SET title = 'denied'")
        .is_err());
}

#[test]
fn raise_using_preserves_dynamic_message_and_detail() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let error = session.execute("DO $$ DECLARE dependency TEXT := 'missing_provider'; BEGIN RAISE EXCEPTION USING ERRCODE = '42883', MESSAGE = '100% missing ' || dependency, DETAIL = dependency; END $$").unwrap_err();
    assert_eq!(error.sqlstate(), "42883");
    assert_eq!(error.to_string(), "100% missing missing_provider");
    assert_eq!(
        error.fields(),
        vec![SqlErrorField::Detail("missing_provider".into())]
    );
}

#[test]
fn role_provisioning_and_memberships_roll_back_with_migrations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE rollback_member LOGIN",
        "CREATE ROLE rollback_worker NOLOGIN",
        "BEGIN",
        "CREATE ROLE temporary_worker NOLOGIN",
        "GRANT rollback_worker TO rollback_member WITH INHERIT FALSE, SET TRUE",
        "ALTER ROLE rollback_member SUPERUSER",
        "ROLLBACK",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        session
            .execute("SELECT count(*) FROM pg_roles WHERE rolname = 'temporary_worker'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT rolsuper FROM pg_roles WHERE rolname = 'rollback_member'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    assert_eq!(
        session
            .execute("SELECT pg_has_role('rollback_member', 'rollback_worker', 'MEMBER')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    for sql in [
        "GRANT rollback_worker TO rollback_member WITH INHERIT FALSE, SET TRUE",
        "BEGIN",
        "SAVEPOINT role_settings",
        "GRANT rollback_worker TO rollback_member WITH INHERIT TRUE, SET FALSE",
        "ROLLBACK TO role_settings",
        "COMMIT",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(session.execute("SELECT pg_has_role('rollback_member', 'rollback_worker', 'SET'), pg_has_role('rollback_member', 'rollback_worker', 'USAGE')").unwrap().rows, vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]);
    for sql in ["BEGIN", "DROP ROLE rollback_worker", "ROLLBACK"] {
        session.execute(sql).unwrap();
    }
    assert_eq!(
        session
            .execute("SELECT pg_has_role('rollback_member', 'rollback_worker', 'SET')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn routine_oid_lookup_accepts_runtime_names_in_provisioning() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE FUNCTION oid_probe() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$")
        .unwrap();
    session.execute("DO $$ DECLARE function_name TEXT; function_oid OID; BEGIN FOREACH function_name IN ARRAY ARRAY['oid_probe'] LOOP function_oid := to_regproc('public.' || function_name); IF function_oid IS NULL THEN RAISE EXCEPTION 'missing function'; END IF; EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', function_oid::regprocedure); END LOOP; END $$").unwrap();
    assert_eq!(session.execute("SELECT to_regproc('missing_function'), to_regprocedure('missing_function()'), to_regproc(NULL)").unwrap().rows, vec![vec![SqlValue::Null, SqlValue::Null, SqlValue::Null]]);
    assert_eq!(
        session
            .execute("SELECT to_regprocedure('oid_probe()') IS NOT NULL")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn parsed_function_grants_use_the_same_authority_checks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE parsed_grant_admin LOGIN SUPERUSER",
        "CREATE ROLE parsed_grant_app LOGIN",
        "CREATE FUNCTION parsed_grant_probe() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
        "SET SESSION AUTHORIZATION parsed_grant_admin",
        "-- migration comment\nGRANT EXECUTE ON FUNCTION parsed_grant_probe() TO SESSION_USER WITH GRANT OPTION",
        "-- migration comment\nREVOKE ALL ON FUNCTION parsed_grant_probe() FROM PUBLIC",
        "SET SESSION AUTHORIZATION parsed_grant_app",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert!(session
        .execute(
            "-- migration comment\nGRANT EXECUTE ON FUNCTION parsed_grant_probe() TO CURRENT_USER"
        )
        .is_err());
    assert!(session.execute("SELECT parsed_grant_probe()").is_err());
}

#[test]
fn migration_can_replace_function_body_using_quoted_fragments() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION definition_probe() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'old)' $$",
        )
        .unwrap();
    session.execute("DO $migration$ DECLARE old_fragment TEXT := $old$'old)'$old$; new_fragment TEXT := $new$'new(' /* BEGIN */$new$; definition TEXT; BEGIN SELECT pg_get_functiondef(oid) INTO definition FROM pg_proc WHERE proname = 'definition_probe'; definition := replace(definition, old_fragment, new_fragment); EXECUTE definition; END $migration$").unwrap();
    assert_eq!(
        session.execute("SELECT definition_probe()").unwrap().rows,
        vec![vec![SqlValue::String("new(".into())]]
    );
    assert_eq!(
        find_top_level_keyword(
            "/* BEGIN /* nested */ */ \"BEGIN\" $tag$BEGIN)$tag$ BEGIN",
            "BEGIN"
        ),
        Some(50)
    );
}

#[test]
fn carrier_provisioning_privilege_changes_roll_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE carrier_app LOGIN",
        "CREATE TABLE provision_guard (id INT)",
        "GRANT SELECT ON provision_guard TO carrier_app",
        "CREATE FUNCTION provision_probe() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
        "REVOKE EXECUTE ON FUNCTION provision_probe() FROM PUBLIC",
        "BEGIN",
        "DO $carrier_revoke_relations$ BEGIN PERFORM 1 FROM pg_catalog.pg_class AS class; EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC, carrier_app', 'provision_guard'); END $carrier_revoke_relations$",
        "DO $grant_functions$ DECLARE implementation RECORD; BEGIN FOR implementation IN SELECT oid::regprocedure AS identity FROM pg_proc WHERE proname='provision_probe' LOOP EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO carrier_app', implementation.identity); END LOOP; END $grant_functions$",
        "ROLLBACK",
        "SET SESSION AUTHORIZATION carrier_app",
        "SELECT * FROM provision_guard",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert!(session.execute("SELECT provision_probe()").is_err());
}

#[test]
fn trigger_catalog_reports_event_timing_and_row_bits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE trigger_type_probe(id INT)",
        "CREATE FUNCTION trigger_type_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "CREATE TRIGGER before_row BEFORE INSERT OR UPDATE ON trigger_type_probe FOR EACH ROW EXECUTE FUNCTION trigger_type_fn()",
        "CREATE TRIGGER after_statement AFTER DELETE ON trigger_type_probe FOR EACH STATEMENT EXECUTE FUNCTION trigger_type_fn()",
    ] { session.execute(sql).unwrap(); }
    assert_eq!(
        session
            .execute("SELECT tgtype FROM pg_trigger WHERE tgname = 'before_row'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(23)]]
    );
    assert_eq!(
        session
            .execute("SELECT tgtype FROM pg_trigger WHERE tgname = 'after_statement'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(8)]]
    );
}

#[test]
fn routine_signatures_normalize_catalog_type_aliases_without_dropping_unknowns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION signature_types(a INTEGER, b BIGINT, c BOOLEAN, d DOUBLE PRECISION, e UUID, f TIMESTAMPTZ) RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$").unwrap();
    session
        .execute("CREATE FUNCTION zero_signature() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$")
        .unwrap();
    for signature in [
        "signature_types(integer,bigint,boolean,double precision,uuid,timestamp with time zone)",
        "signature_types(int4,int8,bool,float8,uuid,timestamptz)",
    ] {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT to_regprocedure('{signature}') IS NOT NULL"
                ))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(true)]]
        );
    }
    assert_eq!(session.execute("SELECT to_regprocedure('zero_signature(unknown_signature_type)'), to_regprocedure('signature_types(text,bigint,boolean,double precision,uuid,timestamptz)')").unwrap().rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
}

#[test]
fn plpgsql_set_functions_correlate_rows_and_accumulate_return_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE set_inputs (id BIGINT PRIMARY KEY, normalized BIGINT)",
        "INSERT INTO set_inputs VALUES (1, 0), (2, 0)",
        "CREATE FUNCTION set_expand(n BIGINT) RETURNS TABLE (value BIGINT) LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT n; RETURN QUERY SELECT n + 10; RETURN; RETURN QUERY SELECT 999; END $$",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    let result = session.execute("SELECT s.id, f.value FROM set_inputs s CROSS JOIN LATERAL set_expand(s.id) f WHERE f.value > 5 ORDER BY s.id").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(11)],
            vec![SqlValue::Int(2), SqlValue::Int(12)]
        ]
    );
    assert_eq!(
        session.execute("SELECT * FROM set_expand(3)").unwrap().rows,
        vec![vec![SqlValue::Int(3)], vec![SqlValue::Int(13)]]
    );
    session.execute("WITH normalized AS (SELECT s.id, f.value FROM set_inputs s CROSS JOIN LATERAL set_expand(s.id) f WHERE f.value > 10) UPDATE set_inputs SET normalized = normalized.value FROM normalized WHERE set_inputs.id = normalized.id").unwrap();
    assert_eq!(
        session
            .execute("SELECT normalized FROM set_inputs ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11)], vec![SqlValue::Int(12)]]
    );
}

#[test]
fn plpgsql_set_functions_preserve_transaction_and_execute_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE set_reader LOGIN",
        "CREATE TABLE set_writes (id BIGINT)",
        "CREATE FUNCTION set_write(n BIGINT) RETURNS TABLE (value BIGINT) LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN INSERT INTO set_writes VALUES (n); IF n < 0 THEN RAISE EXCEPTION 'negative'; END IF; RETURN QUERY SELECT n; END $$",
        "REVOKE ALL ON FUNCTION set_write(BIGINT) FROM PUBLIC",
        "SET SESSION AUTHORIZATION set_reader",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert!(session
        .execute("SELECT * FROM set_write(1)")
        .unwrap_err()
        .to_string()
        .contains("permission denied"));
    for sql in [
        "RESET SESSION AUTHORIZATION",
        "GRANT EXECUTE ON FUNCTION set_write(BIGINT) TO set_reader",
        "SET SESSION AUTHORIZATION set_reader",
        "BEGIN",
        "SELECT * FROM set_write(2)",
        "ROLLBACK",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(session.execute("SELECT * FROM set_write(-1)").is_err());
    assert_eq!(
        session.execute("SELECT current_user").unwrap().rows,
        vec![vec![SqlValue::String("set_reader".into())]]
    );
    assert!(session.execute("SELECT * FROM set_writes").is_err());
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM set_writes")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert!(session
        .execute("WITH changed AS (SELECT * FROM set_write(8)) SELECT value / 0 FROM changed")
        .is_err());
    assert_eq!(
        session
            .execute("SELECT count(*) FROM set_writes")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    session.execute("SELECT * FROM set_write(3)").unwrap();
    assert_eq!(
        session.execute("SELECT id FROM set_writes").unwrap().rows,
        vec![vec![SqlValue::Int(3)]]
    );
}

#[test]
fn plpgsql_set_functions_empty_left_join_and_result_shape() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE set_left (id BIGINT)",
        "INSERT INTO set_left VALUES (1), (2)",
        "CREATE FUNCTION set_empty(n BIGINT) RETURNS TABLE (value BIGINT) LANGUAGE plpgsql AS $$ BEGIN IF n = 1 THEN RETURN QUERY SELECT n; END IF; END $$",
        "CREATE FUNCTION set_bad() RETURNS TABLE (value BIGINT) LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY SELECT 1, 2; END $$",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert_eq!(session.execute("SELECT s.id, f.value FROM set_left s LEFT JOIN LATERAL set_empty(s.id) f ON TRUE ORDER BY s.id").unwrap().rows, vec![vec![SqlValue::Int(1), SqlValue::Int(1)], vec![SqlValue::Int(2), SqlValue::Null]]);
    assert!(session
        .execute("SELECT * FROM set_bad()")
        .unwrap_err()
        .to_string()
        .contains("column count"));
    assert_eq!(
        session
            .execute("SELECT * FROM set_empty(2)")
            .unwrap()
            .columns,
        vec!["value"]
    );
}

#[test]
fn plpgsql_set_functions_strict_lookup_rejects_missing_and_ambiguous_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE strict_lookup (id BIGINT, lookup_key BIGINT)",
        "INSERT INTO strict_lookup VALUES (1, 10), (2, 20), (3, 20)",
        "CREATE FUNCTION set_lookup(n BIGINT) RETURNS TABLE (value BIGINT) LANGUAGE plpgsql AS $$ DECLARE selected_id BIGINT; BEGIN SELECT s.id INTO STRICT selected_id FROM strict_lookup s WHERE s.lookup_key = n; RETURN QUERY SELECT selected_id; END $$",
        "CREATE FUNCTION set_lookup_handled(n BIGINT) RETURNS TABLE (value BIGINT) LANGUAGE plpgsql AS $$ DECLARE selected_id BIGINT; BEGIN SELECT s.id INTO STRICT selected_id FROM strict_lookup s WHERE s.lookup_key = n; RETURN QUERY SELECT selected_id; EXCEPTION WHEN NO_DATA_FOUND OR TOO_MANY_ROWS THEN RETURN QUERY SELECT -1; END $$",
    ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
    assert_eq!(
        session
            .execute("SELECT * FROM set_lookup(10)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute("SELECT * FROM set_lookup(30)")
            .unwrap_err()
            .sqlstate(),
        "P0002"
    );
    assert_eq!(
        session
            .execute("SELECT * FROM set_lookup(20)")
            .unwrap_err()
            .sqlstate(),
        "P0003"
    );
    for key in [20, 30] {
        assert_eq!(
            session
                .execute(&format!("SELECT * FROM set_lookup_handled({key})"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(-1)]]
        );
    }
}

#[test]
fn trigram_indexes_match_scans_across_patterns_updates_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let patterns = [
        "%alpha%",
        "%ALPHA%",
        "%pha%bet%",
        "%al_ha%",
        "%al%",
        "%λ%",
        "%kelvin%",
        "%a\\_b%",
        "%absent%",
    ];
    let queries = patterns.iter().flat_map(|pattern| ["LIKE", "ILIKE"].map(|op| format!("SELECT id FROM trigram_rows WHERE title {op} '{pattern}' AND archived = FALSE ORDER BY id"))).collect::<Vec<_>>();
    let expected;
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE trigram_rows (id BIGINT PRIMARY KEY, title TEXT, archived BOOLEAN)",
            )
            .unwrap();
        session.execute("INSERT INTO trigram_rows VALUES (1, 'alphabet soup', FALSE), (2, 'ALPHABET', FALSE), (3, 'alpha archived', TRUE), (4, 'λ alpha', FALSE), (5, 'Kelvin', FALSE), (6, 'a_b', FALSE), (7, NULL, FALSE), (8, 'unrelated', FALSE)").unwrap();
        expected = queries
            .iter()
            .map(|sql| session.execute(sql).unwrap().rows)
            .collect::<Vec<_>>();
        session.execute("CREATE INDEX trigram_title ON trigram_rows USING GIN (title gin_trgm_ops) WHERE archived = FALSE").unwrap();
        for (query, rows) in queries.iter().zip(&expected) {
            assert_eq!(&session.execute(query).unwrap().rows, rows, "{query}");
        }
        session.execute("BEGIN").unwrap();
        session
            .execute("UPDATE trigram_rows SET title = 'alpha new' WHERE id = 8")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT id FROM trigram_rows WHERE title LIKE '%alpha new%'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(8)]]
        );
        session.execute("ROLLBACK").unwrap();
        assert!(session
            .execute("SELECT id FROM trigram_rows WHERE title LIKE '%alpha new%'")
            .unwrap()
            .rows
            .is_empty());
        session
            .execute("UPDATE trigram_rows SET title = 'replacement token' WHERE id = 8")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT id FROM trigram_rows WHERE title LIKE '%replacement%'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(8)]]
        );
        session
            .execute("DELETE FROM trigram_rows WHERE id = 8")
            .unwrap();
        assert!(session
            .execute("SELECT id FROM trigram_rows WHERE title LIKE '%replacement%'")
            .unwrap()
            .rows
            .is_empty());
        assert!(session
            .execute(
                "CREATE UNIQUE INDEX invalid_trgm ON trigram_rows USING GIN (title gin_trgm_ops)"
            )
            .is_err());
        assert!(session
            .execute("CREATE INDEX wrong_trgm ON trigram_rows USING GIN (id gin_trgm_ops)")
            .is_err());
    }
    let schema = load_schema(&db, "trigram_rows").unwrap().unwrap();
    let expr = parse_routine_expr("title LIKE '%alphabet%'").unwrap();
    let candidate = SqlEngine::new(&db)
        .trigram_index_candidate("trigram_rows", "trigram_rows", &schema, &expr)
        .unwrap();
    assert!(candidate.is_some(), "schema: {:?}", schema.indexes);
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    assert_eq!(
        SqlEngine::new(&db)
            .execute("SELECT id FROM trigram_rows WHERE title LIKE '%alphabet%'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    assert!(
        stats.index_lookup_count > 0,
        "substring predicate must use real postings"
    );
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (query, rows) in queries.iter().zip(expected) {
        assert_eq!(
            session.execute(query).unwrap().rows,
            rows,
            "after restart: {query}"
        );
    }
}

#[test]
fn anonymous_do_duplicate_object_preserves_constraints_and_rolls_back_handler_block() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE duplicate_guard (id BIGINT PRIMARY KEY, value BIGINT CONSTRAINT positive_value CHECK (value > 0))").unwrap();
    let sql = "DO $$ BEGIN ALTER TABLE duplicate_guard ADD COLUMN discarded TEXT; ALTER TABLE duplicate_guard ADD CONSTRAINT positive_value CHECK (value > -100); EXCEPTION WHEN duplicate_object THEN NULL; END $$";
    session.execute(sql).unwrap();
    assert!(session
        .execute("SELECT discarded FROM duplicate_guard")
        .is_err());
    assert!(session
        .execute("INSERT INTO duplicate_guard VALUES (1, -1)")
        .is_err());
    session
        .execute("INSERT INTO duplicate_guard VALUES (2, 1)")
        .unwrap();
    assert!(session.execute("DO $$ BEGIN RAISE EXCEPTION 'unrelated failure'; EXCEPTION WHEN duplicate_object THEN NULL; END $$").is_err());
    session.execute("DO $$ BEGIN NULL; END $$").unwrap();
}

#[test]
fn carrier_current_context_is_served_from_the_trusted_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"CREATE SCHEMA carrier_private;
                   CREATE FUNCTION carrier_private.current_context()
                   RETURNS JSONB LANGUAGE SQL STABLE
                   AS $$ SELECT NULL::JSONB $$;
                   CREATE FUNCTION carrier_private.current_tenant()
                   RETURNS TEXT LANGUAGE SQL STABLE
                   AS $$ SELECT carrier_private.current_context() ->> 'tenant' $$;
                   CREATE FUNCTION carrier_private.current_roles()
                   RETURNS JSONB LANGUAGE SQL STABLE
                   AS $$ SELECT COALESCE(carrier_private.current_context() -> 'roles', '[]'::JSONB) $$;"#,
            )
            .unwrap();
        // An unbound login reads the authored body and stays fail-closed.
        assert_eq!(
            session
                .execute("SELECT carrier_private.current_tenant()")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Null]]
        );
    }

    let bound = SecurityContext::new("42", "org-a").with_roles(["member", "org_admin"]);
    let mut session = SqlSession::new_secure(&mut db, bound);
    assert_eq!(
        session
            .execute("SELECT carrier_private.current_tenant()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("org-a".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT carrier_private.current_context() ->> 'subject', \
                        carrier_private.current_context() -> 'user' ->> 'id', \
                        carrier_private.current_roles() ?| ARRAY['org_admin'], \
                        carrier_private.current_roles() ?| ARRAY['auditor']"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("42".to_string()),
            SqlValue::String("42".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
}

#[test]
fn carrier_operation_authority_trigger_runs_for_trusted_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"CREATE SCHEMA carrier_private;
                   CREATE TABLE posts (id TEXT PRIMARY KEY, org_id TEXT NOT NULL);
                   CREATE FUNCTION carrier_private.enforce_operation_context()
                   RETURNS TRIGGER LANGUAGE plpgsql
                   AS $$ BEGIN
                     RAISE EXCEPTION 'missing or invalid Carrier operation authority'
                       USING ERRCODE = '42501';
                   END $$;
                   CREATE TRIGGER carrier_guard_posts BEFORE INSERT OR UPDATE ON posts
                   FOR EACH ROW EXECUTE FUNCTION carrier_private.enforce_operation_context('id');"#,
            )
            .unwrap();
        // Without a bound identity the authored guard runs and rejects.
        assert!(session
            .execute("INSERT INTO posts (id, org_id) VALUES ('p0', 'org-a')")
            .is_err());
    }

    let bound = SecurityContext::new("42", "org-a").with_roles(["member"]);
    let mut session = SqlSession::new_secure(&mut db, bound);
    let error = session
        .execute("INSERT INTO posts (id, org_id) VALUES ('p1', 'org-a')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    assert!(error
        .to_string()
        .contains("missing or invalid Carrier operation authority"));
    assert_eq!(
        session.execute("SELECT count(*) FROM posts").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn carrier_runtime_role_block_provisions_the_role_in_one_step() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let block = r#"DO $carrier_runtime_role$
DECLARE
  target_role CONSTANT TEXT := 'carrier_app';
  attempts INTEGER := 0;
BEGIN
  LOOP
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
        EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT', target_role);
      END IF;
      EXIT;
    EXCEPTION
      WHEN duplicate_object OR unique_violation THEN
        NULL;
    END;
    attempts := attempts + 1;
    PERFORM pg_sleep(LEAST(attempts * 0.01, 0.1));
  END LOOP;
END
$carrier_runtime_role$"#;
    let mut session = SqlSession::new(&mut db);
    session.execute(block).unwrap();
    // Re-running takes the ALTER path and stays idempotent.
    session.execute(block).unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT rolcanlogin, rolsuper, rolbypassrls, rolinherit \
                 FROM pg_roles WHERE rolname = 'carrier_app'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );
}

#[test]
fn plpgsql_sis_migration_ddl_if_exists_is_not_control_flow() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE migration_boundary (id INT PRIMARY KEY); INSERT INTO migration_boundary VALUES (1)").unwrap();
    let migration = r#"DO $migration$ BEGIN
      IF true THEN
        ALTER TABLE migration_boundary ADD COLUMN IF NOT EXISTS tenant TEXT;
        DROP INDEX IF EXISTS migration_old_idx;
        -- IF FOR END IF END LOOP in a comment must not change nesting.
        IF false THEN
          RAISE EXCEPTION 'wrong nested branch';
        ELSE
          UPDATE migration_boundary SET tenant = 'λ IF; ELSE; END LOOP';
        END IF;
      ELSE
        RAISE EXCEPTION 'wrong outer branch';
      END IF;
    END $migration$"#;
    session.execute(migration).unwrap();
    session.execute(migration).unwrap();
    assert_eq!(
        session
            .execute("SELECT tenant FROM migration_boundary")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("λ IF; ELSE; END LOOP".into())]]
    );
    session.execute("DO $$ BEGIN IF false THEN ALTER TABLE migration_boundary ADD COLUMN IF NOT EXISTS forbidden INT; ELSIF true THEN ALTER TABLE migration_boundary ADD COLUMN IF NOT EXISTS allowed INT; ELSE RAISE EXCEPTION 'wrong elsif'; END IF; END $$").unwrap();
    assert!(session
        .execute("SELECT allowed FROM migration_boundary")
        .is_ok());
    assert!(session
        .execute("SELECT forbidden FROM migration_boundary")
        .is_err());
}

#[test]
fn plpgsql_sis_continue_skips_only_the_innermost_loop_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE loop_visits (v INT)").unwrap();
    session
        .execute(
            r#"DO $$ DECLARE item INT; row RECORD; BEGIN
      FOR outer_i IN 1..2 LOOP
        FOR inner_i IN 1..3 LOOP
          IF inner_i = 2 THEN CONTINUE; END IF;
          INSERT INTO loop_visits VALUES (outer_i * 10 + inner_i);
        END LOOP;
        INSERT INTO loop_visits VALUES (outer_i * 100);
      END LOOP;
      FOREACH item IN ARRAY ARRAY[1,2,3] LOOP
        CONTINUE WHEN item = 2;
        INSERT INTO loop_visits VALUES (item);
      END LOOP;
      FOR row IN SELECT v FROM loop_visits WHERE v < 10 ORDER BY v LOOP
        CONTINUE WHEN row.v = 1;
        INSERT INTO loop_visits VALUES (row.v * 1000);
      END LOOP;
    END $$"#,
        )
        .unwrap();
    let mut rows = session.execute("SELECT v FROM loop_visits").unwrap().rows;
    rows.sort_by_key(|row| sql_value_i64(&row[0]).unwrap());
    assert_eq!(
        rows,
        [1, 3, 11, 13, 21, 23, 100, 200, 3000].map(|v| vec![SqlValue::Int(v)])
    );
    for sql in [
        "DO $$ BEGIN CONTINUE; END $$",
        "DO $$ BEGIN IF false THEN CONTINUE; END IF; END $$",
        "DO $$ BEGIN FOR i IN 1..2 LOOP CONTINUE outer_loop; END LOOP; END $$",
    ] {
        assert!(session.execute(sql).is_err(), "must reject {sql}");
    }
    session.execute("BEGIN").unwrap();
    session.execute("DO $$ BEGIN FOR i IN 1..3 LOOP CONTINUE WHEN i = 2; INSERT INTO loop_visits VALUES (999); END LOOP; END $$").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert!(session
        .execute("SELECT v FROM loop_visits WHERE v = 999")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn plpgsql_sis_boundaries_ignore_sql_keywords_and_quoted_bodies() {
    let sql = "IF true THEN /* IF /* FOR */ END IF */ PERFORM 'λ'; EXECUTE $body$SELECT 'END IF; FOR; ELSE'$body$; ALTER TABLE x ADD COLUMN IF NOT EXISTS v TEXT; IF false THEN NULL; ELSE NULL; END IF; ELSE NULL; END IF";
    assert_eq!(matching_end_if(sql).unwrap(), sql.len());
    let body = "ALTER TABLE x ADD COLUMN IF NOT EXISTS v TEXT; IF false THEN NULL; ELSE NULL; END IF; ELSE NULL;";
    assert_eq!(
        find_plpgsql_if_branch(body).unwrap().unwrap().0,
        body.rfind("ELSE").unwrap()
    );
    let loop_sql = "FOR r IN SELECT id FROM x FOR UPDATE LOOP IF true THEN NULL; END IF; END LOOP";
    assert_eq!(matching_end_loop(loop_sql).unwrap(), loop_sql.len());
    assert!(
        matching_end_if("IF true THEN ALTER TABLE x ADD COLUMN IF NOT EXISTS v TEXT;").is_err()
    );
}
