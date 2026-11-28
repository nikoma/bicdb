use bicdb_core::BicDb;
use bicdb_sql::{SqlErrorField, SqlSession, SqlValue};
use std::sync::{Arc, Barrier};

#[test]
fn shared_sequence_allocations_are_unique_durable_and_survive_outer_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE SEQUENCE shared_counter")
        .unwrap();
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut sql = SqlSession::new_shared(&db);
                let mut values = Vec::new();
                barrier.wait();
                for _ in 0..25 {
                    sql.execute("BEGIN").unwrap();
                    let r = sql.execute("SELECT nextval('shared_counter')").unwrap();
                    let SqlValue::Int(value) = r.rows[0][0] else {
                        panic!("int8 sequence value required")
                    };
                    values.push(value);
                    sql.execute("ROLLBACK").unwrap();
                }
                values
            })
        })
        .collect::<Vec<_>>();
    let mut values = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, (1..=200).collect::<Vec<_>>());
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    let r = SqlSession::new(&mut db)
        .execute("SELECT nextval('shared_counter')")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(201)]]);
}

#[test]
fn shared_batch_defaults_and_setval_keep_nontransactional_sequence_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE shared_rows(id bigserial PRIMARY KEY,label text)")
        .unwrap();
    let mut sql = SqlSession::new_shared(&db);
    sql.execute("BEGIN").unwrap();
    sql.execute("INSERT INTO shared_rows(label) VALUES('a'),('b'),('c')")
        .unwrap();
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM shared_rows")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        sql.execute("SELECT nextval('shared_rows_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4)]]
    );
    sql.execute("BEGIN").unwrap();
    sql.execute("SELECT setval('shared_rows_id_seq',40,false)")
        .unwrap();
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('shared_rows_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(40)]]
    );
}

#[test]
fn postgres_sequence_values_from_rows_and_nested_routines_share_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        for statement in [
            "CREATE SEQUENCE outer_seq",
            "CREATE SEQUENCE other_seq",
            "CREATE TABLE sequence_inputs (id int PRIMARY KEY)",
            "INSERT INTO sequence_inputs VALUES (1), (2), (3)",
            "CREATE FUNCTION sequence_tick() RETURNS bigint LANGUAGE plpgsql AS
             $$ BEGIN RETURN nextval('outer_seq'); END $$",
        ] {
            setup.execute(statement).unwrap();
        }
    }
    let mut sql = SqlSession::new_shared(&db);
    sql.execute("BEGIN").unwrap();
    sql.execute("SAVEPOINT s").unwrap();
    assert_eq!(
        sql.execute("SELECT sum(pg_catalog.nextval('outer_seq'))::bigint FROM sequence_inputs")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(6)]],
    );
    assert_eq!(
        sql.execute("SELECT sum(sequence_tick())::bigint FROM sequence_inputs")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(15)]],
    );
    sql.execute("ROLLBACK TO s").unwrap();
    sql.execute("ROLLBACK").unwrap();
    sql.execute("SELECT setval('outer_seq', 40, false), setval('other_seq', 90)")
        .unwrap();
    assert_eq!(
        sql.execute(
            "SELECT currval('outer_seq'), lastval(), currval('other_seq'), nextval(NULL::regclass)"
        )
        .unwrap()
        .rows,
        vec![vec![
            SqlValue::Int(6),
            SqlValue::Int(6),
            SqlValue::Int(90),
            SqlValue::Null
        ]],
    );
    assert_eq!(
        sql.execute("SELECT nextval('outer_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(40)]],
    );
    let mut other = SqlSession::new_shared(&db);
    assert_eq!(
        other
            .execute("SELECT currval('outer_seq')")
            .unwrap_err()
            .sqlstate(),
        "55000",
    );
}

#[test]
fn postgres_alter_sequence_changes_type_bounds_and_parameters_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SEQUENCE resizing START 17")
        .unwrap();
    session.execute("SELECT setval('resizing', 50000)").unwrap();
    let error = session
        .execute("ALTER SEQUENCE resizing AS smallint")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
    assert_eq!(
        error.postgres_message(),
        "RESTART value (50000) cannot be greater than MAXVALUE (32767)"
    );
    session
        .execute("ALTER SEQUENCE resizing AS smallint RESTART WITH 28 CACHE 2")
        .unwrap();
    let sequence = |session: &mut SqlSession<'_>| {
        session.execute("SELECT min_value, max_value, cache_size FROM pg_sequences WHERE sequencename = 'resizing'")
            .unwrap().rows
    };
    let narrowed = vec![vec![
        SqlValue::Int(1),
        SqlValue::Int(32767),
        SqlValue::Int(2),
    ]];
    assert_eq!(sequence(&mut session), narrowed);
    assert_eq!(
        session.execute("SELECT nextval('resizing')").unwrap().rows,
        vec![vec![SqlValue::Int(28)]],
    );
    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER SEQUENCE resizing AS integer CACHE 4")
        .unwrap();
    assert_eq!(
        sequence(&mut session),
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(2147483647),
            SqlValue::Int(4)
        ]],
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(sequence(&mut session), narrowed);

    session
        .execute("CREATE SEQUENCE resizing_fixed MAXVALUE 100000")
        .unwrap();
    let error = session
        .execute("ALTER SEQUENCE resizing_fixed AS smallint")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22023");
    assert_eq!(
        error.postgres_message(),
        "MAXVALUE (100000) is out of range for sequence data type smallint"
    );
    session
        .execute("ALTER SEQUENCE resizing_fixed AS int2 MAXVALUE 20000")
        .unwrap();
}

#[test]
fn postgres_sequence_search_path_and_discard_follow_relation_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE SEQUENCE slot_seq START 100",
        "CREATE SEQUENCE blocked_seq START 300",
        "CREATE SCHEMA seq_scope",
        "CREATE SEQUENCE seq_scope.slot_seq START 20",
        "CREATE TABLE seq_scope.blocked_seq(id int)",
        "SET search_path = seq_scope, public",
    ] {
        sql.execute(statement).unwrap();
    }
    for (query, value) in [
        ("SELECT nextval('slot_seq')", 20),
        ("SELECT nextval('public.slot_seq')", 100),
        ("SELECT nextval('public.slot_seq'::regclass)", 101),
    ] {
        assert_eq!(
            sql.execute(query).unwrap().rows,
            vec![vec![SqlValue::Int(value)]]
        );
    }
    let error = sql.execute("SELECT nextval('blocked_seq')").unwrap_err();
    assert_eq!(error.sqlstate(), "42809");
    assert_eq!(
        error.postgres_message(),
        "cannot open relation \"blocked_seq\""
    );
    assert!(error.fields().contains(&SqlErrorField::Detail(
        "This operation is not supported for tables.".into(),
    )));

    sql.execute("CREATE TEMP SEQUENCE slot_seq START 800")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('slot_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(800)]]
    );
    sql.execute("SET search_path = seq_scope, public, pg_temp")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('slot_seq'), currval('pg_temp.slot_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(21), SqlValue::Int(800)]]
    );

    sql.execute("BEGIN; DISCARD SEQUENCES; ROLLBACK").unwrap();
    for query in [
        "SELECT currval('slot_seq')",
        "SELECT lastval()",
        "SELECT currval('public.slot_seq')",
    ] {
        assert_eq!(
            sql.execute(query).unwrap_err().sqlstate(),
            "55000",
            "{query}"
        );
    }
    assert_eq!(
        sql.execute("SELECT nextval('slot_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(22)]]
    );
    sql.execute("SET search_path = ''").unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('slot_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(801)]]
    );
    assert_eq!(
        sql.execute("SELECT nextval('public.slot_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(102)]]
    );
}

#[test]
fn postgres_sequence_owned_by_requires_an_existing_column_in_the_same_schema_with_the_same_owner() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE own_target (id int PRIMARY KEY)",
        "CREATE UNIQUE INDEX own_target_idx ON own_target (id)",
        "CREATE SCHEMA own_scope",
        "CREATE TABLE own_scope.other_target (id int)",
        "CREATE ROLE own_role",
        "CREATE TABLE own_other_role (id int)",
        "ALTER TABLE own_other_role OWNER TO own_role",
    ] {
        sql.execute(statement).unwrap();
    }
    for (owner, code, message) in [
        ("invalid", "42601", "invalid OWNED BY option"),
        (
            "own_target.missing",
            "42703",
            "column \"missing\" of relation \"own_target\" does not exist",
        ),
        (
            "own_scope.other_target.id",
            "55000",
            "sequence must be in same schema as table it is linked to",
        ),
        (
            "own_other_role.id",
            "55000",
            "sequence must have same owner as table it is linked to",
        ),
        (
            "own_target_idx.id",
            "42809",
            "sequence cannot be owned by relation \"own_target_idx\"",
        ),
    ] {
        let error = sql
            .execute(&format!("CREATE SEQUENCE own_seq OWNED BY {owner}"))
            .unwrap_err();
        assert_eq!(error.sqlstate(), code);
        assert_eq!(error.postgres_message(), message);
        assert_eq!(
            sql.execute("SELECT to_regclass('own_seq')").unwrap().rows,
            vec![vec![SqlValue::Null]]
        );
    }

    sql.execute("CREATE SEQUENCE own_seq OWNED BY own_target.id")
        .unwrap();
    sql.execute("ALTER SEQUENCE own_seq OWNED BY NONE").unwrap();
    sql.execute("DROP TABLE own_target").unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('own_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    sql.execute("CREATE VIEW own_view AS SELECT 1 AS id")
        .unwrap();
    sql.execute("ALTER SEQUENCE own_seq OWNED BY own_view.id")
        .unwrap();
    sql.execute("BEGIN").unwrap();
    sql.execute("DROP VIEW own_view").unwrap();
    assert_eq!(
        sql.execute("SELECT to_regclass('own_seq')").unwrap().rows,
        vec![vec![SqlValue::Null]]
    );
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('own_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn postgres_alter_sequence_resolves_the_search_path_and_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE SEQUENCE path_seq START 100",
        "CREATE SCHEMA seq_alter_path",
        "CREATE SEQUENCE seq_alter_path.path_seq START 20",
        "CREATE TABLE seq_alter_path.path_block (id int)",
        "SET search_path = seq_alter_path, public",
        "ALTER SEQUENCE path_seq RESTART WITH 42",
    ] {
        sql.execute(statement).unwrap();
    }
    let result = sql
        .execute("SELECT nextval('path_seq'), nextval('public.path_seq')")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(42), SqlValue::Int(100)]]
    );
    let result = sql
        .execute("ALTER SEQUENCE IF EXISTS seq_alter_path.missing_seq AS smallint RESTART WITH 10")
        .unwrap();
    assert_eq!(result.command_tag.as_deref(), Some("ALTER SEQUENCE"));
    assert_eq!(result.notices.len(), 1);
    assert_eq!(
        result.notices[0].message,
        "relation \"missing_seq\" does not exist, skipping"
    );
    assert_eq!(
        sql.execute("ALTER SEQUENCE IF EXISTS path_block RESTART")
            .unwrap_err()
            .sqlstate(),
        "42809"
    );
    assert_eq!(
        sql.execute("ALTER SEQUENCE missing_seq RESTART")
            .unwrap_err()
            .sqlstate(),
        "42P01"
    );
}
