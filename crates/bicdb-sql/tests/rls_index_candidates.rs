//! Necessary RLS constraints may narrow candidate reads; they never replace RLS.
use bicdb_core::{BicDb, DbConfig, SecurityContext};
use bicdb_sql::plan_diagnostics::{with_plan_diagnostics, DiagnosticsOptions};
use bicdb_sql::{SqlSession, SqlValue};

fn fixture() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for ddl in [
        "CREATE ROLE probe_reader NOSUPERUSER NOBYPASSRLS",
        "CREATE ROLE probe_bypass NOSUPERUSER BYPASSRLS",
        "CREATE TABLE orders(id TEXT PRIMARY KEY, org_id TEXT, number TEXT, visible BOOLEAN, UNIQUE(org_id,number))",
        "CREATE FUNCTION probe_tenant() RETURNS TEXT LANGUAGE SQL STABLE AS $$ SELECT current_setting('app.tenant',true) $$",
        "GRANT SELECT ON orders TO probe_reader,probe_bypass",
        "GRANT EXECUTE ON FUNCTION probe_tenant() TO probe_reader,probe_bypass",
    ] { sql.execute(ddl).unwrap(); }
    let values = ["own", "other", "third"]
        .into_iter()
        .flat_map(|org| (0..100).map(move |n| format!("('{org}-{n}','{org}','N{n}',{})", n != 43)))
        .collect::<Vec<_>>()
        .join(",");
    sql.execute(&format!("INSERT INTO orders VALUES {values}"))
        .unwrap();
    sql.execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .unwrap();
    sql.execute("CREATE POLICY tenant ON orders FOR SELECT TO probe_reader USING (org_id::text = NULLIF((SELECT probe_tenant()), '') AND visible)").unwrap();
    drop(sql);
    (dir, db)
}
fn reader(db: &mut BicDb) -> SqlSession<'_> {
    let mut sql = SqlSession::new(db);
    sql.execute("SET ROLE probe_reader").unwrap();
    sql
}
fn count(sql: &mut SqlSession<'_>, number: &str) -> i64 {
    let value = sql
        .execute(&format!(
            "SELECT count(*)::int FROM orders WHERE number='{number}'"
        ))
        .unwrap();
    match value.rows[0][0] {
        SqlValue::Int(value) => value,
        ref other => panic!("{other:?}"),
    }
}
fn plan(sql: &mut SqlSession<'_>, query: &str) -> String {
    sql.execute(&format!("EXPLAIN ANALYZE {query}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n")
}
fn assert_point_plan(sql: &mut SqlSession<'_>, query: &str) {
    let output = plan(sql, query);
    assert!(
        output.contains("IndexScan orders_org_id_number_key"),
        "{output}"
    );
    assert!(output.contains("PrefixKeys 2"), "{output}");
    // EXPLAIN ANALYZE does not show a policy reader its pre-policy fetch
    // count; the engine's own trace still proves the narrowed probe.
    assert!(!output.contains("ActualRowsFetched"), "{output}");
    let (result, decisions) =
        with_plan_diagnostics(DiagnosticsOptions::default(), || sql.execute(query));
    result.unwrap();
    let fetched = decisions
        .iter()
        .rev()
        .find_map(|decision| decision.rows_fetched);
    assert_eq!(fetched, Some(1), "{output}\n{decisions:?}");
}
#[test]
fn necessary_policy_equality_reuses_index_and_statement_context_without_dropping_residual_rls() {
    let (_dir, mut db) = fixture();
    let mut sql = reader(&mut db);
    assert_eq!(
        count(&mut sql, "N42"),
        0,
        "missing tenant yields NULL and denies rows"
    );
    for tenant in ["own", "other", "third", "own"] {
        sql.execute(&format!("SET app.tenant='{tenant}'")).unwrap();
        assert_eq!(count(&mut sql, "N42"), 1);
        assert_point_plan(
            &mut sql,
            "SELECT count(*)::int FROM orders WHERE number='N42'",
        );
        assert_eq!(
            count(&mut sql, "N43"),
            0,
            "residual visible policy remains enforced"
        );
    }
    sql.execute("SET app.tenant=''").unwrap();
    assert_eq!(count(&mut sql, "N42"), 0);
    sql.execute("RESET app.tenant").unwrap();
    assert_eq!(count(&mut sql, "N42"), 0);
}
#[test]
fn permissive_union_does_not_make_one_tenant_predicate_necessary() {
    let (_dir, mut db) = fixture();
    SqlSession::new(&mut db)
        .execute("CREATE POLICY shared ON orders FOR SELECT TO probe_reader USING (number='N42')")
        .unwrap();
    let mut sql = reader(&mut db);
    sql.execute("SET app.tenant=''").unwrap();
    assert_eq!(count(&mut sql, "N42"), 3);
    assert!(plan(
        &mut sql,
        "SELECT count(*)::int FROM orders WHERE number='N42'"
    )
    .contains("FullScan"));
}
#[test]
fn common_permissive_conjunct_and_restrictive_conjunct_can_narrow_without_changing_union() {
    let (_dir, mut db) = fixture();
    SqlSession::new(&mut db).execute("CREATE POLICY hidden ON orders FOR SELECT TO probe_reader USING (org_id::text = NULLIF((SELECT probe_tenant()), '') AND number='N43')").unwrap();
    {
        let mut sql = reader(&mut db);
        sql.execute("SET app.tenant='own'").unwrap();
        assert_eq!(count(&mut sql, "N43"), 1);
        assert_point_plan(
            &mut sql,
            "SELECT count(*)::int FROM orders WHERE number='N43'",
        );
    }
    let mut owner = SqlSession::new(&mut db);
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    owner.execute("DROP POLICY hidden ON orders").unwrap();
    owner
        .execute("CREATE POLICY all_rows ON orders FOR SELECT TO probe_reader USING (true)")
        .unwrap();
    owner.execute("CREATE POLICY tenant_restrictive ON orders AS RESTRICTIVE FOR SELECT TO probe_reader USING (org_id::text = NULLIF((SELECT probe_tenant()), ''))").unwrap();
    drop(owner);
    let mut sql = reader(&mut db);
    sql.execute("SET app.tenant='other'").unwrap();
    assert_eq!(count(&mut sql, "N42"), 1);
    assert_point_plan(
        &mut sql,
        "SELECT count(*)::int FROM orders WHERE number='N42'",
    );
}
#[test]
fn owner_bypass_role_bypass_forced_rls_and_trusted_bypass_are_preserved() {
    let (_dir, mut db) = fixture();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("SET app.tenant='own'").unwrap();
        assert_eq!(count(&mut owner, "N42"), 3);
        owner
            .execute("ALTER TABLE orders FORCE ROW LEVEL SECURITY")
            .unwrap();
        assert_eq!(
            count(&mut owner, "N42"),
            0,
            "no owner-applicable permissive policy"
        );
        owner.execute("CREATE POLICY owner_tenant ON orders FOR SELECT TO bicdb USING (org_id::text = NULLIF((SELECT probe_tenant()), ''))").unwrap();
        assert_eq!(count(&mut owner, "N42"), 1);
        assert_point_plan(
            &mut owner,
            "SELECT count(*)::int FROM orders WHERE number='N42'",
        );
        owner.execute("SET ROLE probe_bypass").unwrap();
        assert_eq!(count(&mut owner, "N42"), 3);
    }
    let context = SecurityContext::new("probe_reader", "irrelevant")
        .with_bypass_reason("test trusted host bypass");
    let mut trusted = SqlSession::new(&mut db).with_security_context(Some(context));
    assert_eq!(count(&mut trusted, "N42"), 3);
}
#[test]
fn direct_calls_and_volatile_init_plans_keep_residual_scan_semantics() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    // Unlike an explicit FROM-less InitPlan, a direct user call is per-row.
    owner.execute("CREATE POLICY direct_call ON orders FOR SELECT TO probe_reader USING (org_id=probe_tenant())").unwrap();
    drop(owner);
    let mut sql = reader(&mut db);
    sql.execute("SET app.tenant='own'").unwrap();
    assert_eq!(count(&mut sql, "N42"), 1);
    assert!(plan(
        &mut sql,
        "SELECT count(*)::int FROM orders WHERE number='N42'"
    )
    .contains("FullScan"));
}

#[test]
fn lossy_policy_casts_and_inherited_role_changes_do_not_reuse_wrong_constraints() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("CREATE ROLE probe_child NOSUPERUSER NOBYPASSRLS INHERIT")
        .unwrap();
    owner.execute("GRANT probe_reader TO probe_child").unwrap();
    owner.execute("SET ROLE probe_child").unwrap();
    owner.execute("SET app.tenant='own'").unwrap();
    assert_eq!(count(&mut owner, "N42"), 1);
    assert_point_plan(
        &mut owner,
        "SELECT count(*)::int FROM orders WHERE number='N42'",
    );
    owner.execute("RESET ROLE").unwrap();
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    owner.execute("CREATE POLICY lossy ON orders FOR SELECT TO probe_reader USING (org_id::varchar(3)='oth')").unwrap();
    owner.execute("SET ROLE probe_child").unwrap();
    assert_eq!(count(&mut owner, "N42"), 1);
    assert!(plan(
        &mut owner,
        "SELECT count(*)::int FROM orders WHERE number='N42'"
    )
    .contains("FullScan"));
}

#[test]
fn volatile_policy_init_plans_are_not_speculatively_evaluated_by_explain() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner.execute("CREATE SEQUENCE probe_effect").unwrap();
    owner.execute("CREATE FUNCTION volatile_tenant() RETURNS TEXT LANGUAGE SQL VOLATILE AS $$ SELECT nextval('probe_effect')::text $$").unwrap();
    owner
        .execute("GRANT EXECUTE ON FUNCTION volatile_tenant() TO probe_reader")
        .unwrap();
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    owner.execute("CREATE POLICY volatile_init ON orders FOR SELECT TO probe_reader USING (org_id=(SELECT volatile_tenant()))").unwrap();
    owner.execute("SET ROLE probe_reader").unwrap();
    let output = owner
        .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
    owner.execute("RESET ROLE").unwrap();
    assert_eq!(
        owner
            .execute("SELECT nextval('probe_effect')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn defaulted_overloads_are_not_misclassified_as_pure_zero_argument_calls() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("CREATE SEQUENCE probe_overload_effect")
        .unwrap();
    owner.execute("CREATE FUNCTION overloaded_tenant(value TEXT DEFAULT 'own') RETURNS TEXT LANGUAGE SQL STABLE AS $$ SELECT value $$").unwrap();
    owner.execute("CREATE FUNCTION overloaded_tenant() RETURNS TEXT LANGUAGE SQL VOLATILE AS $$ SELECT nextval('probe_overload_effect')::text $$").unwrap();
    owner
        .execute("GRANT EXECUTE ON FUNCTION overloaded_tenant() TO probe_reader")
        .unwrap();
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    owner.execute("CREATE POLICY overloaded_init ON orders FOR SELECT TO probe_reader USING (org_id=(SELECT overloaded_tenant()))").unwrap();
    owner.execute("SET ROLE probe_reader").unwrap();
    let output = owner
        .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
    owner.execute("RESET ROLE").unwrap();
    assert_eq!(
        owner
            .execute("SELECT nextval('probe_overload_effect')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn volatile_defaults_and_decorated_init_plans_are_not_executed_by_explain() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("CREATE SEQUENCE probe_hidden_effect")
        .unwrap();
    owner.execute("CREATE FUNCTION defaulted_tenant(value TEXT DEFAULT nextval('probe_hidden_effect')::text) RETURNS TEXT LANGUAGE SQL STABLE AS $$ SELECT value $$").unwrap();
    owner
        .execute("GRANT EXECUTE ON FUNCTION defaulted_tenant(TEXT) TO probe_reader")
        .unwrap();
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    for expression in [
        "(SELECT defaulted_tenant())",
        "(SELECT probe_tenant() GROUP BY nextval('probe_hidden_effect'))",
    ] {
        owner.execute(&format!("CREATE POLICY hidden_effect ON orders FOR SELECT TO probe_reader USING (org_id={expression})")).unwrap();
        owner.execute("SET ROLE probe_reader").unwrap();
        let output = owner
            .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
            .unwrap();
        assert!(output
            .rows
            .iter()
            .any(|row| row[0].to_cell().contains("FullScan")));
        owner.execute("RESET ROLE").unwrap();
        owner
            .execute("DROP POLICY hidden_effect ON orders")
            .unwrap();
    }
    assert_eq!(
        owner
            .execute("SELECT nextval('probe_hidden_effect')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn domain_cast_checks_are_not_speculatively_evaluated_by_explain() {
    let (_dir, mut db) = fixture();
    let mut owner = SqlSession::new(&mut db);
    owner
        .execute("CREATE SEQUENCE probe_domain_effect")
        .unwrap();
    owner
        .execute(
            "CREATE DOMAIN probe_tenant_domain AS TEXT CHECK (nextval('probe_domain_effect') > 0)",
        )
        .unwrap();
    owner.execute("DROP POLICY tenant ON orders").unwrap();
    owner.execute("CREATE POLICY domain_value ON orders FOR SELECT TO probe_reader USING (org_id='own'::probe_tenant_domain)").unwrap();
    owner.execute("SET ROLE probe_reader").unwrap();
    let output = owner
        .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
    owner.execute("RESET ROLE").unwrap();
    assert_eq!(
        owner
            .execute("SELECT nextval('probe_domain_effect')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn stable_policy_keys_are_deferred_until_execution_and_false_predicates_do_not_run_them() {
    let (_dir, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION stable_tenant() RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT CASE WHEN current_setting('app.effect',true)='armed' THEN (1/0)::text ELSE current_setting('app.effect',true) END $$").unwrap();
    sql.execute("GRANT EXECUTE ON FUNCTION stable_tenant() TO probe_reader")
        .unwrap();
    sql.execute("DROP POLICY tenant ON orders").unwrap();
    sql.execute("CREATE POLICY stable_key ON orders FOR SELECT TO probe_reader USING (org_id=(SELECT stable_tenant()))").unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    sql.execute("SET app.effect='armed'").unwrap();
    let output = sql
        .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
        .unwrap();
    assert!(output.rows.iter().any(|row| row[0]
        .to_cell()
        .contains("IndexScan orders_org_id_number_key")));
    assert!(output
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("RuntimeKeys deferred")));
    assert_eq!(
        sql.execute("SELECT count(*)::int FROM orders WHERE false")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    for tenant in ["own", "other", "own"] {
        sql.execute(&format!("SET app.effect='{tenant}'")).unwrap();
        assert_eq!(
            sql.execute("SELECT id FROM orders WHERE number='N42'")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String(format!("{tenant}-42"))]]
        );
    }
}

#[test]
fn stable_policy_exceptions_are_not_raised_by_explain_or_contradictory_filters() {
    let (_dir, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION failing_tenant() RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT (1/0)::text $$").unwrap();
    sql.execute("GRANT EXECUTE ON FUNCTION failing_tenant() TO probe_reader")
        .unwrap();
    sql.execute("DROP POLICY tenant ON orders").unwrap();
    sql.execute("CREATE POLICY failing_key ON orders FOR SELECT TO probe_reader USING (org_id=(SELECT failing_tenant()))").unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    sql.execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number='N42'")
        .unwrap();
    for predicate in ["false", "1=0"] {
        sql.execute(&format!(
            "EXPLAIN SELECT count(*)::int FROM orders WHERE {predicate}"
        ))
        .unwrap();
        assert_eq!(
            sql.execute(&format!(
                "SELECT count(*)::int FROM orders WHERE {predicate}"
            ))
            .unwrap()
            .rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
    assert!(sql
        .execute("SELECT count(*)::int FROM orders WHERE number='N42'")
        .is_err());
}

#[test]
fn a_new_policy_prefix_does_not_hoist_authored_volatile_filters_above_rls() {
    let (_dir, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION volatile_number() RETURNS TEXT LANGUAGE SQL VOLATILE SECURITY DEFINER AS $$ SELECT (1/0)::text $$").unwrap();
    sql.execute("GRANT EXECUTE ON FUNCTION volatile_number() TO probe_reader")
        .unwrap();
    // No own rows satisfy residual RLS. An authored volatile predicate must
    // not be hoisted into a new index key and evaluated before that denial.
    sql.execute("UPDATE orders SET visible=false WHERE org_id='own'")
        .unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    sql.execute("SET app.tenant='own'").unwrap();
    for column in ["number", "(number || '')"] {
        assert_eq!(
            sql.execute(&format!(
                "SELECT count(*)::int FROM orders WHERE {column}=volatile_number()"
            ))
            .unwrap()
            .rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
    let output = sql
        .execute("EXPLAIN SELECT count(*)::int FROM orders WHERE number=volatile_number()")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row[0].to_cell().contains("FullScan")));
}

#[test]
fn missing_pk_and_unindexed_empty_reads_do_not_initialize_unused_policy_keys() {
    let (_dir, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION failing_tenant() RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT (1/0)::text $$").unwrap();
    sql.execute("GRANT EXECUTE ON FUNCTION failing_tenant() TO probe_reader")
        .unwrap();
    sql.execute("DROP POLICY tenant ON orders").unwrap();
    sql.execute("CREATE POLICY failing_key ON orders FOR SELECT TO probe_reader USING (org_id=(SELECT failing_tenant()))").unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    assert!(sql
        .execute("SELECT id FROM orders WHERE id='missing'")
        .unwrap()
        .rows
        .is_empty());
    sql.execute("RESET ROLE").unwrap();
    sql.execute("ALTER TABLE orders DROP CONSTRAINT orders_org_id_number_key")
        .unwrap();
    sql.execute("DELETE FROM orders").unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    assert_eq!(count(&mut sql, "N42"), 0);
}

#[test]
fn domain_policy_columns_retain_full_rls_results_without_inferred_keys() {
    let (_dir, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE DOMAIN probe_org_domain AS TEXT")
        .unwrap();
    sql.execute("CREATE TABLE domain_orders(id TEXT PRIMARY KEY, org_id probe_org_domain, number TEXT, UNIQUE(org_id,number))").unwrap();
    sql.execute("INSERT INTO domain_orders VALUES ('own','own','N42'),('other','other','N42')")
        .unwrap();
    sql.execute("ALTER TABLE domain_orders ENABLE ROW LEVEL SECURITY")
        .unwrap();
    sql.execute("CREATE POLICY domain_tenant ON domain_orders FOR SELECT TO probe_reader USING (org_id=(SELECT probe_tenant()))").unwrap();
    sql.execute("GRANT SELECT ON domain_orders TO probe_reader")
        .unwrap();
    sql.execute("SET ROLE probe_reader").unwrap();
    sql.execute("SET app.tenant='own'").unwrap();
    assert_eq!(
        sql.execute("SELECT id FROM domain_orders WHERE number='N42'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("own".to_string())]]
    );
    assert!(plan(&mut sql, "SELECT id FROM domain_orders WHERE number='N42'").contains("FullScan"));
}

fn alternative_index_fixture(db: &mut BicDb, multiple_keys: bool) {
    let mut sql = SqlSession::new(db);
    for ddl in [
        "CREATE TABLE alternative_orders(id TEXT PRIMARY KEY, org_id TEXT, number TEXT, region TEXT)",
        "CREATE UNIQUE INDEX alternative_number ON alternative_orders(number)",
        "CREATE INDEX alternative_org ON alternative_orders(org_id)",
        "CREATE INDEX alternative_region ON alternative_orders(region)",
        "INSERT INTO alternative_orders VALUES ('present','own','present','west')",
        "CREATE FUNCTION alternative_failing_key() RETURNS TEXT LANGUAGE SQL STABLE SECURITY DEFINER AS $$ SELECT (1/0)::text $$",
        "GRANT SELECT,UPDATE ON alternative_orders TO probe_reader",
        "GRANT EXECUTE ON FUNCTION alternative_failing_key() TO probe_reader",
        "ALTER TABLE alternative_orders ENABLE ROW LEVEL SECURITY",
    ] {
        sql.execute(ddl).unwrap();
    }
    let policy = if multiple_keys {
        "org_id=(SELECT probe_tenant()) AND region=(SELECT alternative_failing_key())"
    } else {
        "org_id=(SELECT alternative_failing_key())"
    };
    sql.execute(&format!(
        "CREATE POLICY alternative_policy ON alternative_orders TO probe_reader USING ({policy})"
    ))
    .unwrap();
}

#[test]
fn an_existing_secondary_lookup_does_not_initialize_an_unused_policy_key() {
    let (_dir, mut db) = fixture();
    alternative_index_fixture(&mut db, false);
    let mut sql = reader(&mut db);
    // FOR UPDATE exercises the row-set locator, which must not initialize
    // every eligible secondary index before the existing number lookup.
    assert!(sql
        .execute("SELECT id FROM alternative_orders WHERE number='missing' FOR UPDATE")
        .unwrap()
        .rows
        .is_empty());
    assert!(
        sql.execute("SELECT id FROM alternative_orders WHERE number='present' FOR UPDATE")
            .is_err(),
        "policy remains active for a matching row"
    );
}

#[test]
fn alternative_policy_indexes_do_not_initialize_all_stable_keys() {
    let (_dir, mut db) = fixture();
    alternative_index_fixture(&mut db, true);
    let mut sql = reader(&mut db);
    sql.execute("SET app.tenant='own'").unwrap();
    assert!(sql
        .execute("SELECT id FROM alternative_orders WHERE number='missing' FOR UPDATE")
        .unwrap()
        .rows
        .is_empty());
    assert!(
        sql.execute("SELECT id FROM alternative_orders WHERE number='present' FOR UPDATE")
            .is_err(),
        "residual policy key still executes on a matching row"
    );
}

#[test]
fn unused_generated_full_text_indexes_do_not_disable_tenant_number_lookups() {
    let (_dir, mut db) = fixture();
    let vector = "setweight(to_tsvector('english',COALESCE(number,'')),'A') || setweight(to_tsvector('english',COALESCE(id,'')),'B')";
    SqlSession::new(&mut db)
        .execute(&format!(
            "CREATE INDEX orders_search_fts ON orders USING gin (({vector}))"
        ))
        .unwrap();
    let mut sql = reader(&mut db);
    for tenant in ["own", "other", "own"] {
        sql.execute(&format!("SET app.tenant='{tenant}'")).unwrap();
        assert_eq!(count(&mut sql, "N42"), 1);
        assert_point_plan(
            &mut sql,
            "SELECT count(*)::int FROM orders WHERE number='N42'",
        );
        assert_eq!(
            count(&mut sql, "N43"),
            0,
            "residual visibility still applies"
        );
        // An actual authored search keeps its existing FTS path and RLS.
        let query = format!(
            "SELECT count(*)::int FROM orders WHERE ({vector}) @@ to_tsquery('english','n42')"
        );
        assert_eq!(
            sql.execute(&query).unwrap().rows,
            vec![vec![SqlValue::Int(1)]]
        );
        let output = plan(&mut sql, &query);
        assert!(
            output.contains("FullTextIndexScan orders_search_fts"),
            "{output}"
        );
        assert!(!output.contains("RuntimeKeys deferred"), "{output}");
    }
}
