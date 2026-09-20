use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

const OUTPUT: &str =
    "id + 0 * length(broker_publish('effects', json_build_object('id', id), NULL)::text)";

fn seed(db: &mut BicDb) {
    let mut session = SqlSession::new(db);
    session
        .execute("CREATE TABLE jobs (id INT PRIMARY KEY); INSERT INTO jobs VALUES (1),(2),(3)")
        .unwrap();
}

#[test]
fn output_effects_run_only_for_locked_rows_surviving_limit_and_offset() {
    for (limit, expected) in [
        ("LIMIT 1", Some(1)),
        ("LIMIT 1 OFFSET 1", Some(2)),
        ("LIMIT 0", None),
        ("LIMIT 0 OFFSET 1", None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        seed(&mut db);
        let mut session = SqlSession::new(&mut db);
        let rows = session
            .execute(&format!(
                "SELECT {OUTPUT} FROM jobs ORDER BY id {limit} FOR UPDATE"
            ))
            .unwrap()
            .rows;
        let expected = expected
            .map(|id| vec![vec![SqlValue::Int(id)]])
            .unwrap_or_default();
        assert_eq!(rows, expected, "{limit}");
        assert_eq!(effects(&mut session), expected, "{limit}");
    }
}

#[test]
fn skipped_candidates_do_not_execute_the_output_function() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    seed(&mut db);
    let mut holder = SqlSession::new_shared(&db);
    holder.execute("BEGIN").unwrap();
    holder
        .execute("SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .unwrap();
    let mut worker = SqlSession::new_shared(&db);
    worker.execute("BEGIN").unwrap();
    assert_eq!(
        worker
            .execute(&format!(
                "SELECT {OUTPUT} FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    assert_eq!(effects(&mut worker), vec![vec![SqlValue::Int(2)]]);
    worker.execute("ROLLBACK").unwrap();
    assert_eq!(
        worker
            .execute("SELECT 10 / (id - 1) FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );

    holder
        .execute("SELECT id FROM jobs WHERE id > 1 FOR UPDATE")
        .unwrap();
    assert!(worker
        .execute(&format!(
            "SELECT {OUTPUT} FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
        ))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(effects(&mut worker), vec![vec![SqlValue::Int(2)]]);
    holder.execute("ROLLBACK").unwrap();
}

#[test]
fn ordering_by_an_output_expression_reuses_each_computed_value() {
    for order in ["chosen", "1", OUTPUT] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        seed(&mut db);
        let mut session = SqlSession::new(&mut db);
        let result = session
            .execute(&format!(
                "SELECT {OUTPUT} AS chosen FROM jobs ORDER BY {order} DESC LIMIT 1 FOR UPDATE"
            ))
            .unwrap();
        assert_eq!(result.columns, vec!["chosen"]);
        assert_eq!(result.rows, vec![vec![SqlValue::Int(3)]]);
        assert_eq!(
            effects(&mut session),
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(3)]
            ]
        );
    }
}

#[test]
fn joined_wildcards_and_positional_sorting_keep_visible_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE left_rows (id INT PRIMARY KEY, label TEXT); CREATE TABLE right_rows (id INT PRIMARY KEY); INSERT INTO left_rows VALUES (1,'z'),(2,'a'); INSERT INTO right_rows VALUES (1),(2)").unwrap();
    let result = session.execute("SELECT l.*, r.id AS right_id FROM left_rows l JOIN right_rows r ON r.id = l.id ORDER BY 2 LIMIT 1 FOR UPDATE OF l").unwrap();
    assert_eq!(result.columns, vec!["id", "label", "right_id"]);
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::String("a".into()),
            SqlValue::Int(2)
        ]]
    );
    assert!(session
        .execute("SELECT unnest(ARRAY[id]) FROM right_rows FOR UPDATE")
        .is_err());
}

fn effects(session: &mut SqlSession<'_>) -> Vec<Vec<SqlValue>> {
    let group = uuid::Uuid::new_v4();
    let result = session
        .execute(&format!(
            "SELECT broker_consume('effects', '{group}', 'worker', 100, 30000)"
        ))
        .unwrap();
    let SqlValue::Json(messages) = &result.rows[0][0] else {
        panic!("expected broker messages");
    };
    let mut ids = messages
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["payload"]["id"].as_i64().unwrap())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.into_iter().map(|id| vec![SqlValue::Int(id)]).collect()
}

#[test]
fn offset_rows_stay_locked_and_readonly_ctes_preserve_lock_targets() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    seed(&mut db);
    let mut holder = SqlSession::new_shared(&db);
    holder.execute("BEGIN").unwrap();
    assert_eq!(
        holder
            .execute("SELECT id FROM jobs ORDER BY id LIMIT 1 OFFSET 1 FOR UPDATE")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    let mut other = SqlSession::new_shared(&db);
    for id in [1, 2] {
        assert_eq!(
            other
                .execute(&format!(
                    "SELECT id FROM jobs WHERE id={id} FOR UPDATE NOWAIT"
                ))
                .unwrap_err()
                .sqlstate(),
            "55P03"
        );
    }
    assert_eq!(other.execute("WITH candidate AS (SELECT 3 AS id) SELECT j.id FROM jobs j JOIN candidate c ON c.id = j.id FOR UPDATE OF j NOWAIT").unwrap().rows, vec![vec![SqlValue::Int(3)]]);
    holder.execute("ROLLBACK").unwrap();
}

#[test]
fn repeated_routine_locking_queries_preserve_parameter_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    seed(&mut db);
    let mut session = SqlSession::new(&mut db);
    session.execute(&format!("CREATE FUNCTION choose_job(direction INT) RETURNS INT LANGUAGE plpgsql AS $$ DECLARE chosen INT; BEGIN SELECT {OUTPUT} INTO chosen FROM jobs ORDER BY id * direction LIMIT 1 FOR UPDATE; RETURN chosen; END $$")).unwrap();
    for (direction, expected) in [(1, 1), (-1, 3), (1, 1), (-1, 3)] {
        assert_eq!(
            session
                .execute(&format!("SELECT choose_job({direction})"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(expected)]]
        );
    }
    assert_eq!(
        effects(&mut session),
        vec![
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(3)],
            vec![SqlValue::Int(3)]
        ]
    );
}
