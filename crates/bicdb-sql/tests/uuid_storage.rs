use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use std::time::Duration;

const FIRST: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
const SECOND: &str = "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22";
const THIRD: &str = "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33";
const DEFAULTED: &str = "d0eebc99-9c0b-4ef8-bb6d-6bb9bd380a44";

#[test]
fn uuid_assignments_casts_copy_indexes_and_restart_match_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(&format!(
            "CREATE TABLE uuid_values (
                id UUID PRIMARY KEY,
                external_id UUID NOT NULL UNIQUE,
                optional_id UUID,
                defaulted_id UUID NOT NULL DEFAULT '{{{DEFAULTED}}}'
            )"
        ))
        .unwrap();
    session
        .execute("CREATE INDEX uuid_values_optional_idx ON uuid_values (optional_id)")
        .unwrap();
    session
        .execute(&format!(
            "INSERT INTO uuid_values (id, external_id)
             VALUES ('{}', '{}')",
            FIRST.to_ascii_uppercase(),
            SECOND.replace('-', "")
        ))
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, external_id, optional_id, defaulted_id FROM uuid_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String(FIRST.to_string()),
            SqlValue::String(SECOND.to_string()),
            SqlValue::Null,
            SqlValue::String(DEFAULTED.to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(&format!(
                "SELECT '{{{FIRST}}}'::uuid,
                        '{}'::uuid,
                        '{}'::uuid",
                FIRST.replace('-', ""),
                "c0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a33"
            ))
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String(FIRST.to_string()),
            SqlValue::String(FIRST.to_string()),
            SqlValue::String(THIRD.to_string()),
        ]]
    );

    session
        .execute(&format!(
            "UPDATE uuid_values
             SET optional_id = '{}'
             WHERE id = '{{{FIRST}}}'::uuid",
            "c0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a33"
        ))
        .unwrap();
    assert_eq!(
        session
            .execute(&format!(
                "SELECT id FROM uuid_values WHERE optional_id = '{{{THIRD}}}'::uuid"
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(FIRST.to_string())]]
    );

    assert_eq!(
        session
            .copy_insert_rows(
                "uuid_values",
                &["id".to_string(), "external_id".to_string()],
                vec![vec![
                    Some(SECOND.to_ascii_uppercase()),
                    Some(THIRD.to_string())
                ]],
            )
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .execute("SELECT id, external_id FROM uuid_values ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String(FIRST.to_string()),
                SqlValue::String(SECOND.to_string()),
            ],
            vec![
                SqlValue::String(SECOND.to_string()),
                SqlValue::String(THIRD.to_string()),
            ],
        ]
    );

    for sql in [
        "SELECT 'not-a-uuid'::uuid",
        "SELECT 'a0ee--bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11'::uuid",
        "SELECT ' a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 '::uuid",
        "INSERT INTO uuid_values (id, external_id) VALUES ('bad', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a55')",
        "UPDATE uuid_values SET optional_id = 'bad'",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22P02", "{sql}");
    }
    assert_eq!(
        session
            .copy_insert_rows(
                "uuid_values",
                &["id".to_string(), "external_id".to_string()],
                vec![vec![Some("bad".to_string()), Some(FIRST.to_string())]],
            )
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
    assert_eq!(
        session
            .execute(&format!(
                "INSERT INTO uuid_values (id, external_id)
                 VALUES ('e0eebc99-9c0b-4ef8-bb6d-6bb9bd380a55', '{}')",
                SECOND.to_ascii_uppercase()
            ))
            .unwrap_err()
            .sqlstate(),
        "23505"
    );

    drop(session);
    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT id, optional_id FROM uuid_values ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String(FIRST.to_string()),
                SqlValue::String(THIRD.to_string()),
            ],
            vec![SqlValue::String(SECOND.to_string()), SqlValue::Null],
        ]
    );
}

#[test]
fn uuid_generation_extraction_ordering_uniqueness_and_restart_match_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let versions = session
        .execute(
            "SELECT uuid_extract_version(uuidv4()),
                    uuid_extract_version(gen_random_uuid()),
                    uuid_extract_version(uuid_generate_v4()),
                    uuid_extract_version(uuidv7()),
                    uuid_extract_version(uuidv7(interval '2 hours'))",
        )
        .unwrap();
    assert_eq!(versions.column_types, vec![Some("int2".to_string()); 5]);
    assert_eq!(
        versions.rows,
        vec![vec![
            SqlValue::Int(4),
            SqlValue::Int(4),
            SqlValue::Int(4),
            SqlValue::Int(7),
            SqlValue::Int(7)
        ]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT uuid_extract_version('00000000-0000-0000-8000-000000000000'::uuid),
                        uuid_extract_version('00000000-0000-f000-8000-000000000000'::uuid),
                        uuid_extract_version('00000000-0000-0000-0000-000000000000'::uuid),
                        uuid_extract_timestamp('d62f9b40-1f3d-11ef-8f7d-0242ac120002'::uuid),
                        uuid_extract_timestamp('01890f3b-9e80-7000-8000-000000000000'::uuid),
                        uuid_extract_timestamp('d62f9b40-1f3d-41ef-8f7d-0242ac120002'::uuid)"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(0),
            SqlValue::Int(15),
            SqlValue::Null,
            SqlValue::String("2024-05-31 11:06:31.868499+00".to_string()),
            SqlValue::String("2023-07-01 02:15:12.768+00".to_string()),
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT uuid_extract_version(NULL), uuid_extract_timestamp(NULL)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null, SqlValue::Null]]
    );

    let first = session.execute("SELECT uuidv7()").unwrap().rows[0][0].to_cell();
    std::thread::sleep(Duration::from_millis(2));
    let second = session.execute("SELECT uuidv7()").unwrap().rows[0][0].to_cell();
    assert!(first < second, "UUIDv7 values must follow creation time");

    session
        .execute(
            "CREATE TABLE generated_uuids (
                id UUID PRIMARY KEY DEFAULT uuidv7(),
                legacy_id UUID NOT NULL UNIQUE DEFAULT uuidv4(),
                payload TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute("CREATE INDEX generated_uuids_payload_idx ON generated_uuids (payload)")
        .unwrap();
    for index in 0..64 {
        session
            .execute(&format!(
                "INSERT INTO generated_uuids (payload) VALUES ('row-{index:02}')"
            ))
            .unwrap();
    }
    assert_eq!(
        session
            .execute("SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT legacy_id) FROM generated_uuids")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(64), SqlValue::Int(64), SqlValue::Int(64)]]
    );
    let before_restart = session
        .execute("SELECT id, legacy_id FROM generated_uuids ORDER BY id")
        .unwrap()
        .rows;

    drop(session);
    db.close().unwrap();
    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut reopened_session = SqlSession::new(&mut reopened);
    assert_eq!(
        reopened_session
            .execute("SELECT id, legacy_id FROM generated_uuids ORDER BY id")
            .unwrap()
            .rows,
        before_restart
    );
    assert_eq!(
        reopened_session
            .execute("SELECT payload FROM generated_uuids WHERE payload = 'row-31'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("row-31".to_string())]]
    );
}
