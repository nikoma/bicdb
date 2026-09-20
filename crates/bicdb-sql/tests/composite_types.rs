use bicdb_core::BicDb;
use bicdb_sql::{pg_composite_from_array_json, SqlSession, SqlValue};

#[test]
fn anonymous_records_match_postgresql_text_type_json_and_comparison_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT ROW(1, 'alpha'),
                    pg_typeof(ROW(1, 'alpha')),
                    row_to_json(ROW(1, 'alpha')),
                    (ROW(1, 'alpha')).f2,
                    ROW(1, 'alpha') = ROW(1, 'alpha'),
                    ROW(1, 'alpha') < ROW(2, 'aardvark')",
        )
        .unwrap();

    assert_eq!(result.rows[0][0].to_cell(), "(1,alpha)");
    assert_eq!(result.rows[0][1], SqlValue::String("record".to_string()));
    assert_eq!(result.rows[0][2].to_cell(), r#"{"f1":1,"f2":"alpha"}"#);
    assert_eq!(result.rows[0][3], SqlValue::String("alpha".to_string()));
    assert_eq!(result.rows[0][4], SqlValue::Bool(true));
    assert_eq!(result.rows[0][5], SqlValue::Bool(true));
    assert_eq!(result.column_types[0], Some("record".to_string()));
}

#[test]
fn anonymous_records_preserve_null_empty_quoting_and_nested_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "SELECT ROW(NULL, '', 'a,b', 'a b', 'a\\b', ROW(2, 'nested')),
                    ROW(1, NULL) = ROW(1, NULL),
                    ROW(1, NULL) IS NOT DISTINCT FROM ROW(1, NULL)",
        )
        .unwrap();

    assert_eq!(
        result.rows[0][0].to_cell(),
        r#"(,"","a,b","a b","a\\b","(2,nested)")"#
    );
    assert_eq!(result.rows[0][1], SqlValue::Null);
    assert_eq!(result.rows[0][2], SqlValue::Bool(true));
}

#[test]
fn table_rows_are_first_class_composite_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE dt_record_people (
                id int PRIMARY KEY,
                name text NOT NULL,
                score int CHECK (score > 0)
             );
             INSERT INTO dt_record_people VALUES (1, 'Ada', 10)",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT p,
                    pg_typeof(p),
                    (p).name,
                    row_to_json(p)
             FROM dt_record_people p",
        )
        .unwrap();

    assert_eq!(result.rows[0][0].to_cell(), "(1,Ada,10)");
    assert_eq!(result.column_types[0], Some("dt_record_people".to_string()));
    assert_eq!(
        result.rows[0][1],
        SqlValue::String("dt_record_people".to_string())
    );
    assert_eq!(result.rows[0][2], SqlValue::String("Ada".to_string()));
    assert_eq!(
        result.rows[0][3].to_cell(),
        r#"{"id":1,"name":"Ada","score":10}"#
    );

    let catalog = session
        .execute(
            "SELECT c.oid, c.reltype, t.oid, t.typrelid, t.typtype, t.typcategory,
                    t.typarray
             FROM pg_class c
             JOIN pg_type t ON t.oid = c.reltype
             WHERE c.relname = 'dt_record_people'",
        )
        .unwrap();
    assert_eq!(catalog.rows.len(), 1);
    assert_eq!(catalog.rows[0][1], catalog.rows[0][2]);
    assert_eq!(catalog.rows[0][0], catalog.rows[0][3]);
    assert_eq!(catalog.rows[0][4], SqlValue::String("c".to_string()));
    assert_eq!(catalog.rows[0][5], SqlValue::String("C".to_string()));
    assert_ne!(catalog.rows[0][6], SqlValue::Int(0));
}

#[test]
fn table_row_type_oids_survive_reopen_and_disappear_with_the_table() {
    let root = tempfile::tempdir().unwrap();
    let (type_oid, array_oid) = {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE durable_composite_rows (id int PRIMARY KEY, label text);
                 INSERT INTO durable_composite_rows VALUES (1, 'kept')",
            )
            .unwrap();
        let catalog = session
            .execute(
                "SELECT oid, typarray
                 FROM pg_type
                 WHERE typname = 'durable_composite_rows'",
            )
            .unwrap();
        assert_eq!(catalog.rows.len(), 1);
        let [SqlValue::Int(type_oid), SqlValue::Int(array_oid)] = catalog.rows[0].as_slice() else {
            panic!(
                "table row catalog returned non-integer OIDs: {:?}",
                catalog.rows[0]
            );
        };
        (*type_oid, *array_oid)
    };

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute("SELECT r FROM durable_composite_rows r")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "(1,kept)"
    );
    assert_eq!(
        session
            .execute(
                "SELECT oid, typarray
                 FROM pg_type
                 WHERE typname = 'durable_composite_rows'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(type_oid), SqlValue::Int(array_oid)]],
    );

    session
        .execute("DROP TABLE durable_composite_rows")
        .unwrap();
    assert!(session
        .execute(
            "SELECT oid
             FROM pg_type
             WHERE typname IN ('durable_composite_rows', '_durable_composite_rows')",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn named_composite_types_create_cast_store_and_report_catalogs() {
    let root = tempfile::tempdir().unwrap();
    let (type_oid, array_oid, relation_oid) = {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TYPE dt_contact AS (
                    name varchar(12),
                    age integer,
                    tags text[]
                 );
                 CREATE TABLE dt_contact_values (
                    id integer PRIMARY KEY,
                    contact dt_contact
                 );
                 INSERT INTO dt_contact_values VALUES (
                    1, ROW('Ada', 42, ARRAY['admin', 'clinician'])::dt_contact
                 )",
            )
            .unwrap();

        let selected = session
            .execute(
                "SELECT contact, pg_typeof(contact), (contact).name, (contact).age,
                        (contact).tags
                 FROM dt_contact_values WHERE id = 1",
            )
            .unwrap();
        assert_eq!(selected.rows.len(), 1);
        assert_eq!(
            selected.rows[0][1],
            SqlValue::String("dt_contact".to_string())
        );
        assert_eq!(selected.rows[0][2], SqlValue::String("Ada".to_string()));
        assert_eq!(selected.rows[0][3], SqlValue::Int(42));
        assert_eq!(
            selected.rows[0][4],
            SqlValue::Json(serde_json::json!(["admin", "clinician"]))
        );
        assert_eq!(
            selected.rows[0][0].to_cell(),
            "(Ada,42,\"{admin,clinician}\")"
        );
        assert_eq!(selected.column_types[0], Some("dt_contact".to_string()));

        let catalog = session
            .execute(
                "SELECT t.oid, t.typarray, t.typrelid, t.typtype, t.typcategory,
                        c.reltype, c.relkind, c.relnatts
                 FROM pg_type t
                 JOIN pg_class c ON c.oid = t.typrelid
                 WHERE t.typname = 'dt_contact'",
            )
            .unwrap();
        assert_eq!(catalog.rows.len(), 1);
        assert_eq!(catalog.rows[0][3], SqlValue::String("c".to_string()));
        assert_eq!(catalog.rows[0][4], SqlValue::String("C".to_string()));
        assert_eq!(catalog.rows[0][0], catalog.rows[0][5]);
        assert_eq!(catalog.rows[0][6], SqlValue::String("c".to_string()));
        assert_eq!(catalog.rows[0][7], SqlValue::Int(3));
        let [SqlValue::Int(type_oid), SqlValue::Int(array_oid), SqlValue::Int(relation_oid), ..] =
            catalog.rows[0].as_slice()
        else {
            panic!("named composite catalog returned invalid OIDs")
        };

        let attributes = session
            .execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attndims
                 FROM pg_attribute a
                 WHERE a.attrelid = (SELECT typrelid FROM pg_type WHERE typname = 'dt_contact')
                 ORDER BY a.attnum",
            )
            .unwrap();
        assert_eq!(
            attributes.rows,
            vec![
                vec![
                    SqlValue::String("name".to_string()),
                    SqlValue::String("character varying(12)".to_string()),
                    SqlValue::Int(0),
                ],
                vec![
                    SqlValue::String("age".to_string()),
                    SqlValue::String("integer".to_string()),
                    SqlValue::Int(0),
                ],
                vec![
                    SqlValue::String("tags".to_string()),
                    SqlValue::String("text[]".to_string()),
                    SqlValue::Int(1),
                ],
            ]
        );
        (*type_oid, *array_oid, *relation_oid)
    };

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute("SELECT (contact).name FROM dt_contact_values WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("Ada".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT oid, typarray, typrelid FROM pg_type WHERE typname = 'dt_contact'",)
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(type_oid),
            SqlValue::Int(array_oid),
            SqlValue::Int(relation_oid),
        ]]
    );
}

#[test]
fn named_composite_arrays_preserve_typed_elements_and_subscripts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TYPE dt_coordinate AS (x integer, y integer);
             CREATE TABLE dt_coordinate_sets (
                id integer PRIMARY KEY,
                points dt_coordinate[]
             );
             INSERT INTO dt_coordinate_sets VALUES (
                1,
                ARRAY[
                    ROW(10, 20)::dt_coordinate,
                    ROW(30, 40)::dt_coordinate
                ]
             )",
        )
        .unwrap();

    let result = session
        .execute(
            "SELECT (points[1]).x, (points[2]).y, pg_typeof(points)
             FROM dt_coordinate_sets WHERE id = 1",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(10),
            SqlValue::Int(40),
            SqlValue::String("dt_coordinate[]".to_string()),
        ]]
    );
    assert_eq!(result.column_types[0], Some("int4".to_string()));
    assert_eq!(result.column_types[1], Some("int4".to_string()));
    assert_eq!(result.column_types[2], Some("regtype".to_string()));
}

#[test]
fn named_composite_attributes_evolve_existing_scalar_array_and_nested_values() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TYPE dt_evolving AS (code integer, label text)")
            .unwrap();
        session
            .execute("CREATE TYPE dt_evolving_wrapper AS (member dt_evolving)")
            .unwrap();
        session
            .execute(
                "CREATE TABLE dt_evolving_values (
                    id integer PRIMARY KEY,
                    payload dt_evolving,
                    items dt_evolving[],
                    wrapper dt_evolving_wrapper
                 )",
            )
            .unwrap();
        session
            .execute(
                r#"INSERT INTO dt_evolving_values VALUES (
                    1,
                    ROW(7, 'scalar')::dt_evolving,
                    ARRAY[ROW(8, 'array')::dt_evolving],
                    ROW(ROW(9, 'nested')::dt_evolving)::dt_evolving_wrapper
                 )"#,
            )
            .unwrap();

        session
            .execute("ALTER TYPE dt_evolving ADD ATTRIBUTE enabled boolean")
            .unwrap();
        let added = session
            .execute("SELECT payload, items, wrapper FROM dt_evolving_values")
            .unwrap();
        assert_eq!(added.rows[0][0].to_cell(), "(7,scalar,)");
        assert_eq!(added.rows[0][2].to_cell(), "(\"(9,nested,)\")");
        let SqlValue::Json(serde_json::Value::Array(array)) = &added.rows[0][1] else {
            panic!("composite array was not retained as a typed JSON array")
        };
        let array_value = pg_composite_from_array_json(&array[0]).unwrap();
        assert_eq!(array_value.fields.last().unwrap().value, SqlValue::Null);

        session
            .execute("ALTER TYPE dt_evolving RENAME ATTRIBUTE label TO title")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT row_to_json(payload), items, row_to_json(wrapper)
                     FROM dt_evolving_values",
                )
                .unwrap()
                .rows[0][0]
                .to_cell(),
            r#"{"code":7,"title":"scalar","enabled":null}"#
        );
        let renamed = session
            .execute("SELECT items, row_to_json(wrapper) FROM dt_evolving_values")
            .unwrap();
        let SqlValue::Json(serde_json::Value::Array(array)) = &renamed.rows[0][0] else {
            panic!("composite array was not retained as a typed JSON array")
        };
        let array_value = pg_composite_from_array_json(&array[0]).unwrap();
        assert_eq!(array_value.fields[1].name, "title");
        assert_eq!(
            renamed.rows[0][1].to_cell(),
            r#"{"member":{"code":9,"title":"nested","enabled":null}}"#
        );

        session
            .execute("ALTER TYPE dt_evolving DROP ATTRIBUTE enabled")
            .unwrap();
        session
            .execute("ALTER TYPE dt_evolving ADD ATTRIBUTE notes text")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT a.attnum, a.attisdropped
                     FROM pg_attribute a
                     WHERE a.attrelid = (
                        SELECT typrelid FROM pg_type WHERE typname = 'dt_evolving'
                     ) ORDER BY a.attnum",
                )
                .unwrap()
                .rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Bool(false)],
                vec![SqlValue::Int(2), SqlValue::Bool(false)],
                vec![SqlValue::Int(3), SqlValue::Bool(true)],
                vec![SqlValue::Int(4), SqlValue::Bool(false)],
            ]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT payload, items, wrapper
                     FROM dt_evolving_values",
                )
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "(7,scalar,)"
        );
        let after_drop = session
            .execute("SELECT items, wrapper FROM dt_evolving_values")
            .unwrap();
        let SqlValue::Json(serde_json::Value::Array(array)) = &after_drop.rows[0][0] else {
            panic!("composite array was not retained as a typed JSON array")
        };
        assert_eq!(
            SqlValue::Composite(pg_composite_from_array_json(&array[0]).unwrap()).to_cell(),
            "(8,array,)"
        );
        assert_eq!(after_drop.rows[0][1].to_cell(), "(\"(9,nested,)\")");
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    let reopened = session
        .execute(
            "SELECT row_to_json(payload), format_type(a.atttypid, a.atttypmod)
                 FROM dt_evolving_values,
                      pg_attribute a
                 WHERE a.attrelid = (
                    SELECT typrelid FROM pg_type WHERE typname = 'dt_evolving'
                 ) AND a.attname = 'code'",
        )
        .unwrap();
    assert_eq!(
        reopened.rows[0][0].to_cell(),
        r#"{"code":7,"title":"scalar","notes":null}"#
    );
    assert_eq!(reopened.rows[0][1], SqlValue::String("integer".to_string()));
}

#[test]
fn named_composite_drop_respects_nested_dependencies_and_cascade() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TYPE dt_drop_leaf AS (value integer);
             CREATE TYPE dt_drop_parent AS (leaf dt_drop_leaf);
             CREATE TABLE dt_drop_values (
                id integer PRIMARY KEY,
                payload dt_drop_parent
             )",
        )
        .unwrap();

    let error = session.execute("DROP TYPE dt_drop_leaf").unwrap_err();
    assert!(error.to_string().contains("depends on it"));

    session.execute("DROP TYPE dt_drop_leaf CASCADE").unwrap();
    assert!(session
        .execute(
            "SELECT oid FROM pg_type
             WHERE typname IN ('dt_drop_leaf', 'dt_drop_parent')",
        )
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'dt_drop_values'
                 ORDER BY ordinal_position",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("id".to_string())]]
    );
}

#[test]
fn named_composite_rename_preserves_oids_and_updates_nested_definitions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE TYPE dt_rename_leaf AS (value integer);
               CREATE TYPE dt_rename_parent AS (leaf dt_rename_leaf);
               CREATE TABLE dt_rename_values (
                  id integer PRIMARY KEY,
                  leaf dt_rename_leaf,
                  parent dt_rename_parent
               );
               INSERT INTO dt_rename_values VALUES (
                  1,
                  ROW(4)::dt_rename_leaf,
                  '{"leaf":{"value":5}}'::jsonb::dt_rename_parent
               )"#,
        )
        .unwrap();
    let before = session
        .execute(
            "SELECT oid, typarray, typrelid FROM pg_type
             WHERE typname = 'dt_rename_leaf'",
        )
        .unwrap()
        .rows[0]
        .clone();

    session
        .execute("ALTER TYPE dt_rename_leaf RENAME TO dt_renamed_leaf")
        .unwrap();

    let after = session
        .execute(
            "SELECT oid, typarray, typrelid FROM pg_type
             WHERE typname = 'dt_renamed_leaf'",
        )
        .unwrap();
    assert_eq!(after.rows, vec![before]);
    assert_eq!(
        session
            .execute(
                "SELECT pg_typeof(leaf), row_to_json(parent)
                 FROM dt_rename_values",
            )
            .unwrap()
            .rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        vec!["dt_renamed_leaf", r#"{"leaf":{"value":5}}"#]
    );
    assert_eq!(
        session
            .execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                 FROM pg_attribute a
                 WHERE a.attrelid = (
                    SELECT typrelid FROM pg_type WHERE typname = 'dt_rename_parent'
                 ) AND a.attname = 'leaf'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("leaf".to_string()),
            SqlValue::String("dt_renamed_leaf".to_string()),
        ]]
    );
}

#[test]
fn named_composite_alter_rejects_dependent_type_changes_and_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TYPE dt_atomic AS (value text);
             CREATE TABLE dt_atomic_values (
                id integer PRIMARY KEY,
                payload dt_atomic
             );
             INSERT INTO dt_atomic_values VALUES (1, ROW('not-an-int')::dt_atomic)",
        )
        .unwrap();

    assert!(session
        .execute("ALTER TYPE dt_atomic ALTER ATTRIBUTE value TYPE integer CASCADE")
        .is_err());
    assert_eq!(
        session
            .execute("SELECT payload FROM dt_atomic_values")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "(not-an-int)"
    );
    assert_eq!(
        session
            .execute(
                "SELECT format_type(a.atttypid, a.atttypmod)
                 FROM pg_attribute a
                 WHERE a.attrelid = (
                    SELECT typrelid FROM pg_type WHERE typname = 'dt_atomic'
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("text".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TYPE dt_atomic ADD ATTRIBUTE enabled boolean CASCADE")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT count(*) FROM pg_attribute a
                 WHERE a.attrelid = (
                    SELECT typrelid FROM pg_type WHERE typname = 'dt_atomic'
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM dt_atomic_values")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "(not-an-int)"
    );
}

#[test]
fn table_row_types_keep_constraints_at_table_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE dt_boundary_item (
                name varchar(5) NOT NULL,
                quantity integer CHECK (quantity > 0),
                state text DEFAULT 'new'
             );
             CREATE TABLE dt_boundary_box (
                id integer PRIMARY KEY,
                item dt_boundary_item NOT NULL,
                items dt_boundary_item[]
             );
             INSERT INTO dt_boundary_box VALUES (
                1,
                ROW(NULL, -1, NULL)::dt_boundary_item,
                ARRAY[ROW('ok', 1, 'new')::dt_boundary_item]
             );
             INSERT INTO dt_boundary_box VALUES (
                2,
                ROW('valid', -2, 'new')::dt_boundary_item,
                ARRAY[]::dt_boundary_item[]
             )",
        )
        .unwrap();

    let selected = session
        .execute(
            "SELECT item, (item).name, (item).quantity,
                    item IS NULL, item IS NOT NULL,
                    (items[1]).name, pg_typeof(item), pg_typeof(items)
             FROM dt_boundary_box WHERE id = 1",
        )
        .unwrap();
    assert_eq!(selected.rows[0][0].to_cell(), "(,-1,)");
    assert_eq!(selected.rows[0][1], SqlValue::Null);
    assert_eq!(selected.rows[0][2], SqlValue::Int(-1));
    assert_eq!(selected.rows[0][3], SqlValue::Bool(false));
    assert_eq!(selected.rows[0][4], SqlValue::Bool(false));
    assert_eq!(selected.rows[0][5], SqlValue::String("ok".to_string()));
    assert_eq!(
        selected.rows[0][6],
        SqlValue::String("dt_boundary_item".to_string())
    );
    assert_eq!(
        selected.rows[0][7],
        SqlValue::String("dt_boundary_item[]".to_string())
    );

    let null_predicates = session
        .execute(
            "SELECT (ROW(NULL, NULL, NULL)::dt_boundary_item) IS NULL,
                    (ROW(NULL, -1, NULL)::dt_boundary_item) IS NOT NULL,
                    (ROW('ok', 1, 'new')::dt_boundary_item) IS NOT NULL",
        )
        .unwrap();
    assert_eq!(
        null_predicates.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
        ]]
    );

    let not_null = session
        .execute(
            "INSERT INTO dt_boundary_item (name, quantity, state)
             SELECT (item).name, (item).quantity, (item).state
             FROM dt_boundary_box WHERE id = 1",
        )
        .unwrap_err();
    assert!(not_null.to_string().contains("not-null constraint"));

    let check = session
        .execute(
            "INSERT INTO dt_boundary_item (name, quantity, state)
             SELECT (item).name, (item).quantity, (item).state
             FROM dt_boundary_box WHERE id = 2",
        )
        .unwrap_err();
    assert!(check.to_string().contains("check constraint"));

    session
        .execute(
            "CREATE DOMAIN dt_valid_boundary_item AS dt_boundary_item
             CHECK ((VALUE).quantity > 0 AND (VALUE).name IS NOT NULL)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT ROW('ok', 1, 'new')::dt_valid_boundary_item")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "(ok,1,new)"
    );
    assert!(session
        .execute("SELECT ROW(NULL, -1, 'new')::dt_valid_boundary_item")
        .is_err());
}

#[test]
fn table_row_type_oids_dependencies_and_renames_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let (type_oid, array_oid) = {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE dt_row_source (label text, quantity integer);
                 CREATE TABLE dt_row_holder (
                    id integer PRIMARY KEY,
                    item dt_row_source,
                    items dt_row_source[]
                 );
                 INSERT INTO dt_row_holder VALUES (
                    1,
                    ROW('scalar', 2)::dt_row_source,
                    ARRAY[ROW('array', 3)::dt_row_source]
                 )",
            )
            .unwrap();
        let oids = session
            .execute(
                "SELECT oid, typarray FROM pg_type
                 WHERE typname = 'dt_row_source'",
            )
            .unwrap();
        let [SqlValue::Int(type_oid), SqlValue::Int(array_oid)] = oids.rows[0].as_slice() else {
            panic!("table row type catalog returned invalid OIDs")
        };

        let add_error = session
            .execute("ALTER TABLE dt_row_source ADD COLUMN enabled boolean")
            .unwrap_err();
        assert!(add_error.to_string().contains("uses its row type"));
        let type_error = session
            .execute("ALTER TABLE dt_row_source ALTER COLUMN quantity TYPE bigint")
            .unwrap_err();
        assert!(type_error.to_string().contains("uses its row type"));

        session
            .execute("ALTER TABLE dt_row_source RENAME COLUMN quantity TO amount")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT (item).amount, (items[1]).amount
                     FROM dt_row_holder WHERE id = 1",
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2), SqlValue::Int(3)]]
        );

        session
            .execute("ALTER TABLE dt_row_source RENAME TO dt_row_renamed")
            .unwrap();
        let after = session
            .execute(
                "SELECT oid, typarray FROM pg_type
                 WHERE typname = 'dt_row_renamed'",
            )
            .unwrap();
        assert_eq!(
            after.rows,
            vec![vec![SqlValue::Int(*type_oid), SqlValue::Int(*array_oid)]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT pg_typeof(item), pg_typeof(items), (item).amount,
                            item::text, items::text
                     FROM dt_row_holder WHERE id = 1",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("dt_row_renamed".to_string()),
                SqlValue::String("dt_row_renamed[]".to_string()),
                SqlValue::Int(2),
                SqlValue::String("(scalar,2)".to_string()),
                SqlValue::String(r#"{"(array,3)"}"#.to_string()),
            ]]
        );
        (*type_oid, *array_oid)
    };

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT item::text, items::text
                 FROM dt_row_holder WHERE id = 1",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("(scalar,2)".to_string()),
            SqlValue::String(r#"{"(array,3)"}"#.to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT oid, typarray FROM pg_type
                 WHERE typname = 'dt_row_renamed'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(type_oid), SqlValue::Int(array_oid)]]
    );
    assert!(session.execute("DROP TABLE dt_row_renamed").is_err());
    session
        .execute("DROP TABLE dt_row_renamed CASCADE")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'dt_row_holder'
                 ORDER BY ordinal_position",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("id".to_string())]]
    );
    assert!(session
        .execute(
            "SELECT oid FROM pg_type
             WHERE typname IN ('dt_row_renamed', '_dt_row_renamed')",
        )
        .unwrap()
        .rows
        .is_empty());
}
