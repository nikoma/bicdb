//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn gitlab_partition_catalog_views_reflect_partition_metadata() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_static")
        .unwrap();
    session
        .execute(
            r#"
            CREATE VIEW postgres_partitioned_tables AS
             SELECT (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text) AS identifier,
                pg_class.oid,
                pg_namespace.nspname AS schema,
                pg_class.relname AS name,
                    CASE partitioned_tables.partstrat
                        WHEN 'l'::"char" THEN 'list'::text
                        WHEN 'r'::"char" THEN 'range'::text
                        WHEN 'h'::"char" THEN 'hash'::text
                        ELSE NULL::text
                    END AS strategy,
                array_agg(pg_attribute.attname) AS key_columns
               FROM (((( SELECT pg_partitioned_table.partrelid,
                        pg_partitioned_table.partstrat,
                        unnest(pg_partitioned_table.partattrs) AS column_position
                       FROM pg_partitioned_table) partitioned_tables
                 JOIN pg_class ON ((partitioned_tables.partrelid = pg_class.oid)))
                 JOIN pg_namespace ON ((pg_class.relnamespace = pg_namespace.oid)))
                 JOIN pg_attribute ON (((pg_attribute.attrelid = pg_class.oid) AND (pg_attribute.attnum = partitioned_tables.column_position))))
              WHERE (pg_namespace.nspname = "current_schema"())
              GROUP BY (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text), pg_class.oid, pg_namespace.nspname, pg_class.relname,
                    CASE partitioned_tables.partstrat
                        WHEN 'l'::"char" THEN 'list'::text
                        WHEN 'r'::"char" THEN 'range'::text
                        WHEN 'h'::"char" THEN 'hash'::text
                        ELSE NULL::text
                    END
            "#,
        )
        .unwrap();
    session
        .execute(
            r#"
            CREATE VIEW postgres_partitions AS
             SELECT (((pg_namespace.nspname)::text || '.'::text) || (pg_class.relname)::text) AS identifier,
                pg_class.oid,
                pg_namespace.nspname AS schema,
                pg_class.relname AS name,
                (((parent_namespace.nspname)::text || '.'::text) || (parent_class.relname)::text) AS parent_identifier,
                pg_get_expr(pg_class.relpartbound, pg_inherits.inhrelid) AS condition
               FROM ((((pg_class
                 JOIN pg_namespace ON ((pg_namespace.oid = pg_class.relnamespace)))
                 JOIN pg_inherits ON ((pg_class.oid = pg_inherits.inhrelid)))
                 JOIN pg_class parent_class ON ((pg_inherits.inhparent = parent_class.oid)))
                 JOIN pg_namespace parent_namespace ON ((parent_class.relnamespace = parent_namespace.oid)))
              WHERE (pg_class.relispartition AND (pg_namespace.nspname = ANY (ARRAY["current_schema"(), 'gitlab_partitions_dynamic'::name, 'gitlab_partitions_static'::name])))
            "#,
        )
        .unwrap();

    session
        .execute(
            "CREATE TABLE p_ai_active_context_code_repositories \
             (id bigint NOT NULL, project_id bigint NOT NULL) \
             PARTITION BY RANGE (project_id)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT identifier, schema, name, strategy, key_columns
                   FROM postgres_partitioned_tables
                   WHERE identifier = 'public.p_ai_active_context_code_repositories'
                   LIMIT 1"#
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("public.p_ai_active_context_code_repositories".to_string()),
            SqlValue::String("public".to_string()),
            SqlValue::String("p_ai_active_context_code_repositories".to_string()),
            SqlValue::String("range".to_string()),
            SqlValue::String("project_id".to_string()),
        ]]
    );
    assert!(session
        .execute(
            r#"SELECT identifier
               FROM postgres_partitions
               WHERE parent_identifier = 'public.p_ai_active_context_code_repositories'
               LIMIT 1"#
        )
        .unwrap()
        .rows
        .is_empty());

    session
        .execute(
            "CREATE TABLE p_events \
             (id bigint NOT NULL, partition_id bigint NOT NULL) \
             PARTITION BY LIST (partition_id)",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE gitlab_partitions_static.p_events_00 \
             (id bigint NOT NULL, partition_id bigint NOT NULL)",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE ONLY p_events \
             ATTACH PARTITION gitlab_partitions_static.p_events_00 \
             FOR VALUES IN (0)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT identifier, schema, name, parent_identifier, condition
                   FROM postgres_partitions
                   WHERE parent_identifier = 'public.p_events'
                   LIMIT 1"#
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("gitlab_partitions_static.p_events_00".to_string()),
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String("p_events_00".to_string()),
            SqlValue::String("public.p_events".to_string()),
            SqlValue::String("FOR VALUES IN (0)".to_string()),
        ]]
    );
}

#[test]
fn postgres_exclusion_constraints_enforce_same_key_range_overlap() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE incident_management_oncall_shifts (
                id bigint PRIMARY KEY,
                rotation_id bigint NOT NULL,
                starts_at timestamptz NOT NULL,
                ends_at timestamptz NOT NULL
            )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "ALTER TABLE ONLY incident_management_oncall_shifts
                 ADD CONSTRAINT inc_mgmnt_no_overlapping_oncall_shifts
                 EXCLUDE USING gist (
                   rotation_id WITH =,
                   tstzrange(starts_at, ends_at, '[)'::text) WITH &&
                 )"
            )
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );

    session
        .execute(
            "INSERT INTO incident_management_oncall_shifts
             (id, rotation_id, starts_at, ends_at)
             VALUES (1, 7, '2026-01-01 09:00:00+00', '2026-01-01 11:00:00+00')",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO incident_management_oncall_shifts
             (id, rotation_id, starts_at, ends_at)
             VALUES (2, 8, '2026-01-01 10:00:00+00', '2026-01-01 12:00:00+00')",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO incident_management_oncall_shifts
             (id, rotation_id, starts_at, ends_at)
             VALUES (3, 7, '2026-01-01 11:00:00+00', '2026-01-01 13:00:00+00')",
        )
        .unwrap();

    let overlap = session
        .execute(
            "INSERT INTO incident_management_oncall_shifts
             (id, rotation_id, starts_at, ends_at)
             VALUES (4, 7, '2026-01-01 10:30:00+00', '2026-01-01 12:30:00+00')",
        )
        .unwrap_err();
    assert_eq!(overlap.sqlstate(), "23P01");
    assert!(overlap
        .to_string()
        .contains("inc_mgmnt_no_overlapping_oncall_shifts"));

    let update_overlap = session
        .execute(
            "UPDATE incident_management_oncall_shifts
             SET rotation_id = 7
             WHERE id = 2",
        )
        .unwrap_err();
    assert_eq!(update_overlap.sqlstate(), "23P01");

    assert_eq!(
        session
            .execute(
                "SELECT conname, contype
                 FROM pg_catalog.pg_constraint
                 WHERE conname = 'inc_mgmnt_no_overlapping_oncall_shifts'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("inc_mgmnt_no_overlapping_oncall_shifts".to_string()),
            SqlValue::String("x".to_string()),
        ]]
    );

    session
        .execute(
            "CREATE TABLE overlapping_shifts (
                id bigint PRIMARY KEY,
                rotation_id bigint NOT NULL,
                starts_at timestamptz NOT NULL,
                ends_at timestamptz NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO overlapping_shifts
             (id, rotation_id, starts_at, ends_at)
             VALUES
             (1, 9, '2026-01-02 09:00:00+00', '2026-01-02 11:00:00+00'),
             (2, 9, '2026-01-02 10:00:00+00', '2026-01-02 12:00:00+00')",
        )
        .unwrap();
    let existing_overlap = session
        .execute(
            "ALTER TABLE ONLY overlapping_shifts
             ADD CONSTRAINT overlapping_shifts_no_overlap
             EXCLUDE USING gist (
               rotation_id WITH =,
               tstzrange(starts_at, ends_at, '[)'::text) WITH &&
             )",
        )
        .unwrap_err();
    assert_eq!(existing_overlap.sqlstate(), "23P01");
}

#[test]
fn postgres_exclusion_constraints_enforce_native_range_column_overlap() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ci_partitions (
                id bigint PRIMARY KEY,
                builds_id_range int8range
            )",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "ALTER TABLE ci_partitions
                 ADD CONSTRAINT check_ci_partitions_builds_id_range_no_overlap
                 EXCLUDE USING gist (builds_id_range WITH &&)
                 WHERE (builds_id_range IS NOT NULL)"
            )
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );

    session
        .execute("INSERT INTO ci_partitions (id, builds_id_range) VALUES (1, '[1,10)')")
        .unwrap();
    session
        .execute("INSERT INTO ci_partitions (id, builds_id_range) VALUES (2, '[10,20)')")
        .unwrap();
    session
        .execute("INSERT INTO ci_partitions (id, builds_id_range) VALUES (3, NULL)")
        .unwrap();

    let overlap = session
        .execute("INSERT INTO ci_partitions (id, builds_id_range) VALUES (4, '[9,12)')")
        .unwrap_err();
    assert_eq!(overlap.sqlstate(), "23P01");
    assert!(overlap
        .to_string()
        .contains("check_ci_partitions_builds_id_range_no_overlap"));

    let update_overlap = session
        .execute("UPDATE ci_partitions SET builds_id_range = '[5,8)' WHERE id = 3")
        .unwrap_err();
    assert_eq!(update_overlap.sqlstate(), "23P01");

    session
        .execute(
            "CREATE TABLE overlapping_ci_partitions (
                id bigint PRIMARY KEY,
                builds_id_range int8range
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO overlapping_ci_partitions (id, builds_id_range)
             VALUES (1, '[1,10)'), (2, '[2,3)')",
        )
        .unwrap();
    let existing_overlap = session
        .execute(
            "ALTER TABLE overlapping_ci_partitions
             ADD CONSTRAINT overlapping_ci_partitions_no_overlap
             EXCLUDE USING gist (builds_id_range WITH &&)
             WHERE (builds_id_range IS NOT NULL)",
        )
        .unwrap_err();
    assert_eq!(existing_overlap.sqlstate(), "23P01");
}

#[test]
fn range_exclusion_constraints_and_gist_spgist_catalogs_match_postgres() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        .unwrap();
    session
        .execute("CREATE TABLE exclusion_keys (id bigint PRIMARY KEY, code text, span int4range)")
        .unwrap();
    session
        .execute(
            "ALTER TABLE exclusion_keys
             ADD CONSTRAINT exclusion_keys_code_span_key
             EXCLUDE USING gist (code WITH =, span WITH =)",
        )
        .unwrap();
    session
        .execute("INSERT INTO exclusion_keys VALUES (1, 'a', '[1,3)'), (2, 'a', '[2,4)')")
        .unwrap();
    let duplicate = session
        .execute("INSERT INTO exclusion_keys VALUES (3, 'a', '[1,3)')")
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "23P01");
    session
        .execute("CREATE INDEX exclusion_keys_span_spgist ON exclusion_keys USING spgist (span)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id FROM exclusion_keys
                 WHERE span && '[2,3)'::int4range ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );

    session
        .execute(
            "CREATE TABLE range_reservations (
                id bigint PRIMARY KEY,
                account bigint NOT NULL,
                occupied int4multirange,
                exact_start numeric,
                exact_end numeric
            )",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE range_reservations
             ADD CONSTRAINT range_reservations_no_overlap
             EXCLUDE USING gist (account WITH =, occupied WITH &&)
             WHERE (occupied IS NOT NULL)",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX range_reservations_occupied_gist
             ON range_reservations USING gist (occupied)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO range_reservations (id, account, occupied)
             VALUES (1, 7, '{[1,3),[8,10)}'), (2, 7, '{[3,8)}')",
        )
        .unwrap();
    let overlap = session
        .execute(
            "INSERT INTO range_reservations (id, account, occupied)
             VALUES (3, 7, '{[2,4)}')",
        )
        .unwrap_err();
    assert_eq!(overlap.sqlstate(), "23P01");
    session
        .execute(
            "INSERT INTO range_reservations (id, account, occupied)
             VALUES (4, 8, '{[2,4)}')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT c.conindid = i.indexrelid, i.indisexclusion, am.amname
                 FROM pg_catalog.pg_constraint c
                 JOIN pg_catalog.pg_index i ON i.indexrelid = c.conindid
                 JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
                 JOIN pg_catalog.pg_am am ON am.oid = ic.relam
                 WHERE c.conname = 'range_reservations_no_overlap'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("gist".to_string()),
        ]]
    );
    let definitions = session
        .execute(
            "SELECT pg_get_constraintdef(c.oid), pg_get_indexdef(c.conindid)
             FROM pg_catalog.pg_constraint c
             WHERE c.conname = 'range_reservations_no_overlap'",
        )
        .unwrap()
        .rows;
    assert_eq!(definitions.len(), 1);
    assert!(definitions[0][0]
        .to_cell()
        .contains("EXCLUDE USING gist (account WITH =, occupied WITH &&)"));
    assert!(definitions[0][1]
        .to_cell()
        .contains("USING gist (account, occupied) WHERE (occupied IS NOT NULL)"));
    let dependent = session
        .execute("DROP INDEX range_reservations_no_overlap")
        .unwrap_err();
    assert_eq!(dependent.sqlstate(), "2BP01");

    session
        .execute("CREATE INDEX range_reservations_spgist ON range_reservations USING spgist (numrange(exact_start, exact_end))")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT am.amname
                 FROM pg_catalog.pg_class ic
                 JOIN pg_catalog.pg_am am ON am.oid = ic.relam
                 WHERE ic.relname = 'range_reservations_spgist'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("spgist".to_string())]]
    );
    let unsupported = session
        .execute(
            "CREATE INDEX range_reservations_mr_spgist
             ON range_reservations USING spgist (occupied)",
        )
        .unwrap_err();
    assert_eq!(unsupported.sqlstate(), "42704");

    session
        .execute(
            "ALTER TABLE range_reservations
             ADD CONSTRAINT range_reservations_not_adjacent
             EXCLUDE USING gist (numrange(exact_start, exact_end) WITH -|-)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO range_reservations
             (id, account, exact_start, exact_end)
             VALUES (5, 9, 0.0000001, 0.0000002)",
        )
        .unwrap();
    let adjacent = session
        .execute(
            "INSERT INTO range_reservations
             (id, account, exact_start, exact_end)
             VALUES (6, 9, 0.0000002, 0.0000003)",
        )
        .unwrap_err();
    assert_eq!(adjacent.sqlstate(), "23P01");
    session
        .execute(
            "INSERT INTO range_reservations
             (id, account, exact_start, exact_end)
             VALUES (7, 9, 0.00000015, 0.00000025)",
        )
        .unwrap();

    drop(session);
    assert!(db.verify_index("exclusion_keys_span_spgist").unwrap().valid);
    assert!(
        db.verify_index("range_reservations_occupied_gist")
            .unwrap()
            .valid
    );
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT am.amname
                 FROM pg_catalog.pg_class ic
                 JOIN pg_catalog.pg_am am ON am.oid = ic.relam
                 WHERE ic.relname = 'range_reservations_spgist'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("spgist".to_string())]]
    );
    let persisted_conflict = session
        .execute(
            "INSERT INTO range_reservations (id, account, occupied)
             VALUES (8, 7, '{[2,4)}')",
        )
        .unwrap_err();
    assert_eq!(persisted_conflict.sqlstate(), "23P01");
}

#[test]
fn range_arrays_catalogs_statistics_and_selectivity_survive_reopen() {
    let (dir, mut db) = empty_test_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE range_array_matrix (
                    id integer PRIMARY KEY,
                    i4 int4range[], i8 int8range[], nr numrange[],
                    tr tsrange[], tzr tstzrange[], dr daterange[],
                    i4m int4multirange[], i8m int8multirange[], nrm nummultirange[],
                    trm tsmultirange[], tzrm tstzmultirange[], drm datemultirange[]
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO range_array_matrix VALUES (
                    1,
                    ARRAY['[1,3)'::int4range, 'empty'::int4range],
                    ARRAY['[9007199254740993,9007199254741000)'::int8range],
                    ARRAY['[0.10,9.99)'::numrange],
                    ARRAY['[2024-01-01 00:00:00,2024-01-02)'::tsrange],
                    ARRAY['[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)'::tstzrange],
                    ARRAY['[2024-01-01,2024-01-03)'::daterange],
                    ARRAY['{[1,3)}'::int4multirange, '{}'::int4multirange],
                    ARRAY['{[9007199254740993,9007199254741000)}'::int8multirange],
                    ARRAY['{[0.10,9.99)}'::nummultirange],
                    ARRAY['{[2024-01-01 00:00:00,2024-01-02)}'::tsmultirange],
                    ARRAY['{[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)}'::tstzmultirange],
                    ARRAY['{[2024-01-01,2024-01-03)}'::datemultirange]
                )",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT cardinality(i4), i4[1], i4[2], pg_typeof(i4),
                            cardinality(i4m), i4m[1], i4m[2], pg_typeof(i4m)
                     FROM range_array_matrix",
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(2),
                SqlValue::String("[1,3)".to_string()),
                SqlValue::String("empty".to_string()),
                SqlValue::String("int4range[]".to_string()),
                SqlValue::Int(2),
                SqlValue::String("{[1,3)}".to_string()),
                SqlValue::String("{}".to_string()),
                SqlValue::String("int4multirange[]".to_string()),
            ]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT typname, typinput, typoutput, typreceive, typsend,
                            typanalyze, typelem, typarray
                     FROM pg_catalog.pg_type
                     WHERE typname IN ('int4range', '_int4range',
                                       'int4multirange', '_int4multirange')
                     ORDER BY oid",
                )
                .unwrap()
                .rows,
            vec![
                vec![
                    SqlValue::String("int4range".to_string()),
                    SqlValue::String("range_in".to_string()),
                    SqlValue::String("range_out".to_string()),
                    SqlValue::String("range_recv".to_string()),
                    SqlValue::String("range_send".to_string()),
                    SqlValue::String("range_typanalyze".to_string()),
                    SqlValue::Int(0),
                    SqlValue::Int(3905),
                ],
                vec![
                    SqlValue::String("_int4range".to_string()),
                    SqlValue::String("array_in".to_string()),
                    SqlValue::String("array_out".to_string()),
                    SqlValue::String("array_recv".to_string()),
                    SqlValue::String("array_send".to_string()),
                    SqlValue::String("array_typanalyze".to_string()),
                    SqlValue::Int(3904),
                    SqlValue::Int(0),
                ],
                vec![
                    SqlValue::String("int4multirange".to_string()),
                    SqlValue::String("multirange_in".to_string()),
                    SqlValue::String("multirange_out".to_string()),
                    SqlValue::String("multirange_recv".to_string()),
                    SqlValue::String("multirange_send".to_string()),
                    SqlValue::String("multirange_typanalyze".to_string()),
                    SqlValue::Int(0),
                    SqlValue::Int(6150),
                ],
                vec![
                    SqlValue::String("_int4multirange".to_string()),
                    SqlValue::String("array_in".to_string()),
                    SqlValue::String("array_out".to_string()),
                    SqlValue::String("array_recv".to_string()),
                    SqlValue::String("array_send".to_string()),
                    SqlValue::String("array_typanalyze".to_string()),
                    SqlValue::Int(4451),
                    SqlValue::Int(0),
                ],
            ]
        );

        session
            .execute(
                "CREATE TABLE range_statistics_matrix (
                    id integer PRIMARY KEY,
                    span int4range,
                    spans int4multirange
                )",
            )
            .unwrap();
        let values = (0..100)
            .map(|value| {
                format!(
                    "({value}, '[{value},{})', '{{[{value},{})}}')",
                    value + 10,
                    value + 10
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!(
                "INSERT INTO range_statistics_matrix VALUES {values}"
            ))
            .unwrap();
        session.execute("ANALYZE range_statistics_matrix").unwrap();

        let stats = session
            .execute(
                "SELECT attname, cardinality(range_bounds_histogram),
                        cardinality(range_length_histogram),
                        pg_typeof(range_bounds_histogram),
                        pg_typeof(range_length_histogram), range_empty_frac
                 FROM pg_catalog.pg_stats
                 WHERE tablename = 'range_statistics_matrix'
                   AND attname IN ('span', 'spans')
                 ORDER BY attname",
            )
            .unwrap();
        assert_eq!(stats.rows.len(), 2);
        for row in stats.rows {
            assert_eq!(row[1], SqlValue::Int(100));
            assert_eq!(row[2], SqlValue::Int(100));
            assert_eq!(row[3], SqlValue::String("anyarray".to_string()));
            assert_eq!(row[4], SqlValue::String("anyarray".to_string()));
            assert_float_close(&row[5], 0.0, f64::EPSILON);
        }

        let overlap = session
            .execute(
                "EXPLAIN ANALYZE SELECT id FROM range_statistics_matrix
                 WHERE span && '[50,52)'::int4range",
            )
            .unwrap();
        assert!(overlap
            .rows
            .iter()
            .any(|row| row[0].to_cell() == "EstimatedRows 11"));
        assert!(
            overlap
                .rows
                .iter()
                .any(|row| row[0].to_cell() == "ActualRows 11"),
            "unexpected overlap plan: {:?}",
            overlap.rows
        );
        let contains = session
            .execute(
                "EXPLAIN ANALYZE SELECT id FROM range_statistics_matrix
                 WHERE spans @> 50",
            )
            .unwrap();
        assert!(contains
            .rows
            .iter()
            .any(|row| row[0].to_cell() == "EstimatedRows 10"));
        assert!(contains
            .rows
            .iter()
            .any(|row| row[0].to_cell() == "ActualRows 10"));
    }

    let persisted_stats = db.table_statistics("range_statistics_matrix").unwrap();
    let multirange_stats = persisted_stats
        .columns
        .values()
        .find(|stats| stats.field == IndexField::MetadataPath(vec!["spans".to_string()]))
        .and_then(|stats| stats.range.as_ref())
        .unwrap();
    assert_eq!(multirange_stats.samples.first().unwrap(), "{[0,10)}");
    assert_eq!(multirange_stats.bounds_histogram.first().unwrap(), "[0,10)");

    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    assert_eq!(
        session
            .execute(
                "SELECT cardinality(i4), i4[1], cardinality(i4m), i4m[1]
                 FROM range_array_matrix",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::String("[1,3)".to_string()),
            SqlValue::Int(2),
            SqlValue::String("{[1,3)}".to_string()),
        ]]
    );
    assert!(session
        .execute(
            "EXPLAIN SELECT id FROM range_statistics_matrix
             WHERE spans @> 50",
        )
        .unwrap()
        .rows
        .iter()
        .any(|row| row[0].to_cell() == "EstimatedRows 10"));
}

#[test]
fn sample_realtime_trigger_records_local_pg_notify_events() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = r#"
CREATE OR REPLACE FUNCTION carrier_notify_realtime_event() RETURNS trigger AS $carrier_realtime$
BEGIN
  PERFORM pg_notify('carrier_realtime_events', NEW.sequence::text);
  RETURN NEW;
END
$carrier_realtime$ LANGUAGE plpgsql;
"#;
    let trigger = r#"
DROP TRIGGER IF EXISTS carrier_events_realtime_notify ON carrier_events;
CREATE TRIGGER carrier_events_realtime_notify
AFTER INSERT ON carrier_events
FOR EACH ROW EXECUTE FUNCTION carrier_notify_realtime_event();
"#;

    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                r#"
CREATE TABLE IF NOT EXISTS carrier_events (
  id UUID PRIMARY KEY,
  sequence BIGINT GENERATED BY DEFAULT AS IDENTITY UNIQUE,
  event_name TEXT NOT NULL,
  payload JSONB NOT NULL,
  "current_user" JSONB,
  trace_context JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"#,
            )
            .unwrap();
        session.execute(fixture).unwrap();
        for statement in trigger.split(';').filter(|sql| !sql.trim().is_empty()) {
            session.execute(statement).unwrap();
        }
        session.execute(fixture).unwrap();
        for statement in trigger.split(';').filter(|sql| !sql.trim().is_empty()) {
            session.execute(statement).unwrap();
        }

        session
            .execute(
                "INSERT INTO carrier_events (id, event_name, payload, created_at) VALUES ('00000000-0000-0000-0000-000000000075', 'carrier.created', '{}', '2026-06-20T00:00:00Z')",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT channel, payload, table_name, trigger_name, function_name FROM pg_catalog.bicdb_notifications ORDER BY id"
                )
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("carrier_realtime_events".to_string()),
                SqlValue::String("1".to_string()),
                SqlValue::String("carrier_events".to_string()),
                SqlValue::String("carrier_events_realtime_notify".to_string()),
                SqlValue::String("carrier_notify_realtime_event".to_string()),
            ]]
        );
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SELECT proname FROM pg_catalog.pg_proc WHERE proname = 'carrier_notify_realtime_event'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "carrier_notify_realtime_event".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute("SELECT tgname FROM pg_catalog.pg_trigger WHERE tgname = 'carrier_events_realtime_notify'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "carrier_events_realtime_notify".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute("SELECT channel, payload FROM pg_catalog.bicdb_notifications")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("carrier_realtime_events".to_string()),
            SqlValue::String("1".to_string()),
        ]]
    );
    session
        .execute(
            "INSERT INTO carrier_events (id, event_name, payload, created_at) VALUES ('00000000-0000-0000-0000-000000000076', 'carrier.updated', '{}', '2026-06-20T00:00:01Z')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT payload FROM pg_catalog.bicdb_notifications ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("1".to_string())],
            vec![SqlValue::String("2".to_string())],
        ]
    );
}

#[test]
fn gitlab_create_function_options_are_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                r#"
                CREATE FUNCTION find_namespaces_by_id(namespaces_id bigint) RETURNS namespaces
                    LANGUAGE plpgsql STABLE COST 1 PARALLEL SAFE
                    AS $$
                BEGIN
                    RETURN NULL;
                END
                $$;

                CREATE TABLE gitlab_function_fallback_probe (id TEXT PRIMARY KEY);
                "#,
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE TABLE"
    );
    assert_eq!(
        session
            .execute(
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'find_namespaces_by_id'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("find_namespaces_by_id".to_string()),
            SqlValue::String("f".to_string())
        ]]
    );
}

#[test]
fn sequences_nextval_currval_setval_and_persistence_work() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE SEQUENCE invoice_seq INCREMENT BY 3 START WITH 7")
            .unwrap();
        assert!(matches!(
            session.execute("SELECT currval('invoice_seq')"),
            Err(SqlError::InvalidSql(message)) if message.contains("not yet defined")
        ));
        assert_eq!(
            session
                .execute("SELECT nextval('invoice_seq')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(7)]]
        );
        assert_eq!(
            session
                .execute("SELECT currval('invoice_seq')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(7)]]
        );
        assert_eq!(
            session
                .execute("SELECT setval('invoice_seq', 20, false)")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(20)]]
        );
        assert_eq!(
            session
                .execute("SELECT nextval('invoice_seq')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(20)]]
        );
        assert_eq!(
            session
                .execute("SELECT nextval('invoice_seq'::regclass)")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(23)]]
        );
    }

    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlEngine::new(&db)
            .execute("SELECT last_value, is_called FROM invoice_seq")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(23), SqlValue::Bool(true)]]
    );
}

#[test]
fn sequence_privileges_are_enforced_for_runtime_roles() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE TABLE invoices (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY)")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    let denied = session
        .execute("SELECT nextval('invoices_id_seq')")
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT USAGE, SELECT ON SEQUENCE invoices_id_seq TO carrier_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT nextval('invoices_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    let denied = session
        .execute("SELECT setval('invoices_id_seq', 10, true)")
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT UPDATE ON SEQUENCE invoices_id_seq TO carrier_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT setval('invoices_id_seq', 10, true)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(10)]]
    );
}

#[test]
fn postgres_create_sequence_option_blocks_are_accepted() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE SEQUENCE shared_audit_event_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1
            "#,
        )
        .unwrap();
    session
        .execute("CREATE TABLE shared_audit_events (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("ALTER SEQUENCE shared_audit_event_id_seq OWNED BY shared_audit_events.id")
        .unwrap();
    session
        .execute("ALTER SEQUENCE shared_audit_event_id_seq OWNED BY NONE")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT nextval('shared_audit_event_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT sequence_name FROM information_schema.sequences \
                 WHERE sequence_name = 'shared_audit_event_id_seq'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "shared_audit_event_id_seq".to_string()
        )]]
    );
}

#[test]
fn drop_table_removes_owned_sequences_and_rolls_back_dependency() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE work_item_custom_types (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE SEQUENCE work_item_custom_types_id_seq")
        .unwrap();
    session
        .execute("ALTER SEQUENCE work_item_custom_types_id_seq OWNED BY work_item_custom_types.id")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("DROP TABLE work_item_custom_types")
        .unwrap();
    session
        .execute("CREATE TABLE work_item_custom_types (id bigint NOT NULL)")
        .unwrap();
    session
        .execute("CREATE SEQUENCE work_item_custom_types_id_seq START WITH 1001")
        .unwrap();
    session
        .execute("ALTER SEQUENCE work_item_custom_types_id_seq OWNED BY work_item_custom_types.id")
        .unwrap();
    session.execute("ROLLBACK").unwrap();

    assert_eq!(
        session
            .execute("SELECT nextval('work_item_custom_types_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("DROP TABLE work_item_custom_types")
        .unwrap();
    session
        .execute("CREATE TABLE work_item_custom_types (id bigint NOT NULL)")
        .unwrap();
    session
        .execute("CREATE SEQUENCE work_item_custom_types_id_seq START WITH 1001")
        .unwrap();
    session
        .execute("ALTER SEQUENCE work_item_custom_types_id_seq OWNED BY work_item_custom_types.id")
        .unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute("SELECT nextval('work_item_custom_types_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1001)]]
    );
}

#[test]
fn procedural_sequence_migration_block_is_idempotent_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let migration = r#"
DO $carrier_events_sequence$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class
    WHERE relkind = 'S' AND relname = 'carrier_events_sequence_seq'
  ) THEN
    CREATE SEQUENCE carrier_events_sequence_seq;
  END IF;
END
$carrier_events_sequence$;
"#;
    let default_block = r#"
DO $carrier_events_sequence_default$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'carrier_events'
      AND column_name = 'sequence'
      AND is_identity <> 'YES'
  ) THEN
    ALTER TABLE carrier_events ALTER COLUMN sequence SET DEFAULT nextval('carrier_events_sequence_seq');
  END IF;
END
$carrier_events_sequence_default$;
"#;

    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE carrier_events (id UUID PRIMARY KEY, sequence BIGINT)")
            .unwrap();
        session
            .execute("INSERT INTO carrier_events (id, sequence) VALUES ('00000000-0000-0000-0000-000000000001', NULL)")
            .unwrap();

        let not_null_error = session
            .execute("ALTER TABLE carrier_events ALTER COLUMN sequence SET NOT NULL")
            .unwrap_err();
        assert_eq!(not_null_error.sqlstate(), "23502");

        session.execute(migration).unwrap();
        session.execute(default_block).unwrap();
        session
            .execute("UPDATE carrier_events SET sequence = nextval('carrier_events_sequence_seq') WHERE sequence IS NULL")
            .unwrap();
        session
            .execute("ALTER TABLE carrier_events ALTER COLUMN sequence SET NOT NULL")
            .unwrap();
        session
            .execute(
                "INSERT INTO carrier_events (id) VALUES ('00000000-0000-0000-0000-000000000002')",
            )
            .unwrap();

        assert_eq!(
            session
                .execute("SELECT sequence FROM carrier_events ORDER BY sequence")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        assert_eq!(
            session
                .execute("SELECT column_default, is_nullable FROM information_schema.columns WHERE table_name = 'carrier_events' AND column_name = 'sequence'")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("nextval('carrier_events_sequence_seq'::regclass)".to_string()),
                SqlValue::String("NO".to_string()),
            ]]
        );
        assert_eq!(
            session
                .execute(
                    "SELECT relname FROM pg_class WHERE relname = 'carrier_events_sequence_seq'"
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::String(
                "carrier_events_sequence_seq".to_string()
            )]]
        );

        session.execute(migration).unwrap();
        session.execute(default_block).unwrap();
        session
            .execute("UPDATE carrier_events SET sequence = nextval('carrier_events_sequence_seq') WHERE sequence IS NULL")
            .unwrap();
        session
            .execute("ALTER TABLE carrier_events ALTER COLUMN sequence SET NOT NULL")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT COUNT(*) FROM pg_class WHERE relname = 'carrier_events_sequence_seq'"
                )
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
    }

    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlEngine::new(&db)
            .execute("SELECT last_value, is_called FROM carrier_events_sequence_seq")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2), SqlValue::Bool(true)]]
    );
}

#[test]
fn procedural_constraint_migration_block_is_idempotent() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id UUID PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE observations (id UUID PRIMARY KEY, patient_id UUID)")
        .unwrap();
    let migration = r#"
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'observations_patient_id_fkey'
      AND conrelid = 'observations'::regclass
  ) THEN
    ALTER TABLE observations ADD CONSTRAINT observations_patient_id_fkey
      FOREIGN KEY (patient_id) REFERENCES patients(id);
  END IF;
END
$$;
"#;

    session.execute(migration).unwrap();
    session.execute(migration).unwrap();
    let duplicate = session
        .execute(
            "ALTER TABLE observations ADD CONSTRAINT observations_patient_id_fkey \
             FOREIGN KEY (patient_id) REFERENCES patients(id)",
        )
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "42710");
    assert_eq!(
        session
            .execute(
                "SELECT count(*) FROM pg_constraint WHERE conname = 'observations_patient_id_fkey'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn procedural_constraint_migration_supports_conjoined_catalog_guards() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE conditions (id UUID PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE service_requests (id UUID PRIMARY KEY, diagnosis_condition_id UUID)")
        .unwrap();
    let migration = r#"
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'conditions'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'service_requests_diagnosis_condition_id_fkey'
      AND conrelid = 'service_requests'::regclass
  ) THEN
    ALTER TABLE service_requests
      ADD CONSTRAINT service_requests_diagnosis_condition_id_fkey
      FOREIGN KEY (diagnosis_condition_id) REFERENCES conditions(id);
  END IF;
END $$
"#;

    session.execute(migration).unwrap();
    session.execute(migration).unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT count(*) FROM pg_constraint \
                 WHERE conname = 'service_requests_diagnosis_condition_id_fkey'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn procedural_relation_compatibility_migration_is_idempotent() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE compactitems (id UUID PRIMARY KEY, label TEXT)")
        .unwrap();
    let migration = r#"
-- Preserve legacy names as compatibility views.
DO $migration$
DECLARE
  relation_pair TEXT[];
  relation_pairs CONSTANT TEXT[][] := ARRAY[
    ARRAY['compactitems', 'compact_items']
  ];
BEGIN
  FOREACH relation_pair SLICE 1 IN ARRAY relation_pairs LOOP
    IF to_regclass('public.' || relation_pair[2]) IS NULL THEN
      IF to_regclass('public.' || relation_pair[1]) IS NULL THEN
        RAISE EXCEPTION 'missing relation';
      END IF;
      EXECUTE format(
        'ALTER TABLE public.%I RENAME TO %I', relation_pair[1], relation_pair[2]
      );
    END IF;
    IF to_regclass('public.' || relation_pair[1]) IS NULL THEN
      EXECUTE format(
        'CREATE VIEW public.%I WITH (security_invoker = true) AS SELECT * FROM public.%I',
        relation_pair[1], relation_pair[2]
      );
    END IF;
  END LOOP;
END
$migration$
"#;

    session.execute(migration).unwrap();
    session.execute(migration).unwrap();
    session
        .execute(
            "INSERT INTO compact_items VALUES ('00000000-0000-0000-0000-000000000001', 'kept')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM compactitems")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("kept".to_string())]]
    );
}

#[test]
fn alter_default_privileges_revoke_preserves_deny_by_default() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE SCHEMA IF NOT EXISTS carrier_private")
        .unwrap();
    session
        .execute("REVOKE ALL ON SCHEMA carrier_private FROM PUBLIC, carrier_app")
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             REVOKE ALL ON TABLES FROM PUBLIC, carrier_app",
        )
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA carrier_private \
             REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, carrier_app",
        )
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, carrier_app",
        )
        .unwrap();
    session
        .execute("CREATE TABLE future_records (id UUID PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT has_table_privilege('carrier_app', 'future_records', 'SELECT')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
}

#[test]
fn create_extension_leading_batch_preserves_following_default_privileges() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute(
            "CREATE EXTENSION IF NOT EXISTS pgcrypto;
             ALTER DEFAULT PRIVILEGES IN SCHEMA public
               GRANT SELECT ON TABLES TO carrier_app;
             CREATE TABLE batch_default_records (id INTEGER PRIMARY KEY);",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege(
                    'carrier_app', 'batch_default_records', 'SELECT'
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn policy_accepts_schema_qualified_current_user_function_call() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE SCHEMA carrier_private;
               CREATE FUNCTION carrier_private.current_user()
               RETURNS JSONB LANGUAGE SQL STABLE
               AS $$ SELECT '{"id":"u1"}'::JSONB $$;
               CREATE TABLE carrier_workflow_state (
                 id TEXT PRIMARY KEY,
                 tenant_id TEXT NOT NULL,
                 workspace_id TEXT NOT NULL
               );
               CREATE POLICY carrier_workflow_state_scope_policy
               ON carrier_workflow_state
               USING (
                 tenant_id = '' AND workspace_id = ''
                 AND (SELECT carrier_private.current_user()) IS NOT NULL
               )
               WITH CHECK (
                 tenant_id = '' AND workspace_id = ''
                 AND (SELECT carrier_private.current_user()) IS NOT NULL
               );"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT policyname FROM pg_policies \
                 WHERE policyname = 'carrier_workflow_state_scope_policy'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "carrier_workflow_state_scope_policy".to_string(),
        )]],
    );
}

#[test]
fn alter_default_privileges_grant_applies_to_future_carrier_objects() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE SCHEMA IF NOT EXISTS carrier_private")
        .unwrap();
    session
        .execute("CREATE TABLE before_default_grant (id INTEGER PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO carrier_app",
        )
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             GRANT EXECUTE ON FUNCTIONS TO carrier_app",
        )
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA carrier_private \
             GRANT SELECT ON TABLES TO carrier_app",
        )
        .unwrap();
    session
        .execute("CREATE TABLE after_default_grant (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    session
        .execute("CREATE TABLE carrier_private.future_private_table (id INTEGER PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION future_default_function() RETURNS INTEGER LANGUAGE plpgsql \
             AS $$ BEGIN RETURN 7; END $$",
        )
        .unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             REVOKE SELECT ON TABLES FROM carrier_app",
        )
        .unwrap();
    session
        .execute("CREATE TABLE after_default_revoke (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT defaclobjtype, defaclacl \
                 FROM pg_default_acl \
                 WHERE defaclnamespace = 2200 \
                 ORDER BY defaclobjtype",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("f".to_string()),
                SqlValue::String("{carrier_app=X/bicdb}".to_string()),
            ],
            vec![
                SqlValue::String("r".to_string()),
                SqlValue::String("{carrier_app=awd/bicdb}".to_string()),
            ],
        ]
    );

    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege('carrier_app', 'before_default_grant', 'SELECT'), \
                        has_table_privilege('carrier_app', 'after_default_grant', 'SELECT'), \
                        has_table_privilege('carrier_app', 'after_default_revoke', 'SELECT'), \
                        has_table_privilege(\
                          'carrier_app', 'carrier_private.future_private_table', 'SELECT'\
                        )",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
        ]]
    );
    session
        .execute("INSERT INTO after_default_grant VALUES (1, 'allowed')")
        .unwrap();
    session
        .execute("UPDATE after_default_grant SET value = 'updated' WHERE id = 1")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT future_default_function()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(7)]]
    );
    session
        .execute("DELETE FROM after_default_grant WHERE id = 1")
        .unwrap();
}

#[test]
fn alter_default_privileges_accepts_leading_comments() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute(
            "-- Keep later tables usable for the application role.\n\
             ALTER DEFAULT PRIVILEGES IN SCHEMA public\n\
               GRANT SELECT ON TABLES TO carrier_app",
        )
        .unwrap();
    session
        .execute("CREATE TABLE after_commented_default_grant (id INTEGER PRIMARY KEY)")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege(\
                   'carrier_app', 'after_commented_default_grant', 'SELECT'\
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
}

#[test]
fn table_privilege_inquiry_resolves_schemas_lists_and_boolean_types() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session.execute("CREATE SCHEMA carrier_private").unwrap();
    session
        .execute("CREATE TABLE carrier_private.dependencies (id INTEGER PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE public_records (id INTEGER PRIMARY KEY)")
        .unwrap();
    session
        .execute("GRANT USAGE ON SCHEMA carrier_private TO carrier_app")
        .unwrap();
    session
        .execute("GRANT SELECT ON TABLE carrier_private.dependencies TO carrier_app")
        .unwrap();
    session
        .execute("GRANT SELECT ON TABLE public_records TO carrier_app")
        .unwrap();

    let privileges = session
        .execute(
            "SELECT has_schema_privilege('carrier_app', 'carrier_private', 'USAGE'), \
                    has_table_privilege(\
                      'carrier_app', 'carrier_private.dependencies', 'SELECT'\
                    ), \
                    has_table_privilege(\
                      'carrier_app', 'public.public_records', 'INSERT, SELECT'\
                    )",
        )
        .unwrap();
    assert_eq!(
        privileges.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]],
    );
    assert_eq!(privileges.column_types, vec![Some("bool".to_string()); 3]);
    drop(session);
    assert_eq!(
        infer_query_result_types(
            &db,
            "SELECT has_schema_privilege($1, 'carrier_private', 'USAGE'), \
                    has_table_privilege(\
                      $1, 'carrier_private.dependencies', 'SELECT'\
                    ), \
                    has_function_privilege($1, $2, 'EXECUTE')",
        )
        .unwrap(),
        Some(vec![Some("bool".to_string()); 3]),
    );
}

#[test]
fn alter_default_privileges_are_transactional_and_persistent() {
    let (dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             GRANT SELECT ON TABLES TO carrier_app",
        )
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    session
        .execute("CREATE TABLE rollback_default_check (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege(
                    'carrier_app', 'rollback_default_check', 'SELECT'
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             GRANT SELECT ON TABLES TO carrier_app",
        )
        .unwrap();
    session.execute("COMMIT").unwrap();
    drop(session);
    drop(db);

    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    session
        .execute("CREATE TABLE persisted_default_check (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege(
                    'carrier_app', 'persisted_default_check', 'SELECT'
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn function_execute_privileges_are_enforced() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION protected_value() RETURNS INTEGER LANGUAGE plpgsql \
             AS $$ BEGIN RETURN 7; END $$",
        )
        .unwrap();
    session
        .execute("REVOKE EXECUTE ON FUNCTION protected_value() FROM PUBLIC, carrier_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    let denied = session.execute("SELECT protected_value()").unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION protected_value() TO carrier_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session.execute("SELECT protected_value()").unwrap().rows,
        vec![vec![SqlValue::Int(7)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT has_function_privilege(\
                   'carrier_app', 'protected_value()', 'EXECUTE'\
                 )",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
    let oid_privileges = session
        .execute("SELECT has_function_privilege('carrier_app', oid, 'EXECUTE') FROM pg_proc")
        .unwrap()
        .rows;
    assert!(oid_privileges.contains(&vec![SqlValue::Bool(true)]));
}

#[test]
fn grant_execute_on_all_functions_uses_the_routine_schema_field() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session.execute("CREATE SCHEMA carrier_private").unwrap();
    session
        .execute(
            "CREATE FUNCTION carrier_private.protected_value() RETURNS INTEGER \
             LANGUAGE plpgsql AS $$ BEGIN RETURN 7; END $$",
        )
        .unwrap();
    session
        .execute(
            "REVOKE EXECUTE ON FUNCTION carrier_private.protected_value() \
             FROM PUBLIC, carrier_app",
        )
        .unwrap();
    session
        .execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA carrier_private TO carrier_app")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT has_function_privilege(\
                   'carrier_app', oid, 'EXECUTE'\
                 ) \
                   FROM pg_proc \
                  WHERE pronamespace = (\
                    SELECT oid FROM pg_namespace WHERE nspname = 'carrier_private'\
                  ) \
                    AND proname = 'protected_value'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
}

#[test]
fn table_privileges_are_enforced_for_runtime_roles() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE TABLE protected_rows (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO protected_rows VALUES (1, 'owner-only')")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    let denied = session
        .execute("SELECT value FROM protected_rows")
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
    let denied = session
        .execute("INSERT INTO protected_rows VALUES (2, 'denied')")
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
    session.execute("RESET SESSION AUTHORIZATION").unwrap();
    session
        .execute("GRANT SELECT, INSERT ON TABLE protected_rows TO carrier_app")
        .unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM protected_rows")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("owner-only".to_string())]]
    );
    session
        .execute("INSERT INTO protected_rows VALUES (2, 'allowed')")
        .unwrap();
    let denied = session
        .execute(
            "INSERT INTO protected_rows VALUES (2, 'changed') \
             ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
}

#[test]
fn aggregate_filter_excludes_false_rows() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE migration_state (id INTEGER PRIMARY KEY, dirty BOOLEAN NOT NULL)")
        .unwrap();
    session
        .execute("INSERT INTO migration_state VALUES (1, FALSE), (2, FALSE), (3, TRUE)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT COUNT(dirty)::BIGINT AS applied, \
                 COUNT(dirty) FILTER (WHERE dirty)::BIGINT AS dirty \
                 FROM migration_state",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3), SqlValue::Int(1)]]
    );
}

#[test]
fn security_definer_functions_use_owner_relation_authority() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE TABLE protected_values (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO protected_values VALUES (1, 'reviewed')")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION reviewed_value() RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER \
             AS $$ DECLARE result TEXT; BEGIN \
             SELECT value INTO result FROM protected_values WHERE id = 1; \
             RETURN result; END $$",
        )
        .unwrap();
    session
        .execute("REVOKE EXECUTE ON FUNCTION reviewed_value() FROM PUBLIC, carrier_app")
        .unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION reviewed_value() TO carrier_app")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT prosecdef FROM pg_proc WHERE proname = 'reviewed_value'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session.execute("SELECT reviewed_value()").unwrap().rows,
        vec![vec![SqlValue::String("reviewed".to_string())]]
    );
    let denied = session
        .execute("SELECT value FROM protected_values")
        .unwrap_err();
    assert_eq!(denied.sqlstate(), "42501");
}

#[test]
fn carrier_function_dependency_block_grants_invoker_dependencies() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE TABLE dependency_values (id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO dependency_values VALUES (1, 'reachable')")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION dependency_helper() RETURNS TEXT LANGUAGE plpgsql \
             AS $$ DECLARE result TEXT; BEGIN \
             SELECT value INTO result FROM dependency_values WHERE id = 1; \
             RETURN result; END $$",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION dependency_root() RETURNS TEXT LANGUAGE plpgsql \
             AS $$ BEGIN RETURN dependency_helper(); END $$",
        )
        .unwrap();
    session
        .execute("REVOKE EXECUTE ON FUNCTION dependency_helper() FROM PUBLIC, carrier_app")
        .unwrap();
    session
        .execute("REVOKE EXECUTE ON FUNCTION dependency_root() FROM PUBLIC, carrier_app")
        .unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION dependency_root() TO carrier_app")
        .unwrap();
    session.execute("BEGIN").unwrap();
    let block = r#"DO $carrier_grant_function_dependencies$
DECLARE
  runtime_role_oid OID := 'carrier_app'::regrole::oid;
BEGIN
  -- carrier_runtime_function_dependency_work
  -- carrier_runtime_relation_dependency_work
END
$carrier_grant_function_dependencies$"#;
    session.execute(block).unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT has_table_privilege('carrier_app', 'dependency_values', 'SELECT')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
    session.execute(block).unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session.execute("SELECT dependency_root()").unwrap().rows,
        vec![vec![SqlValue::String("reachable".to_string())]]
    );
}

#[test]
fn procedural_role_creation_guard_is_idempotent() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    let sql = r#"
DO $carrier_app_role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'carrier_app') THEN
    CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END
$carrier_app_role$
"#;
    session.execute(sql).unwrap();
    session.execute(sql).unwrap();
    assert_eq!(
        session
            .execute("SELECT rolname FROM pg_roles WHERE rolname = 'carrier_app'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("carrier_app".to_string())]]
    );
}

#[test]
fn carrier_relation_revoke_block_clears_existing_runtime_grants() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute("CREATE TABLE records (id UUID PRIMARY KEY)")
        .unwrap();
    session
        .execute("GRANT SELECT ON records TO PUBLIC")
        .unwrap();
    session
        .execute("GRANT SELECT ON records TO carrier_app")
        .unwrap();
    let sql = r#"
DO $carrier_revoke_relations$
DECLARE
  relation RECORD;
BEGIN
  FOR relation IN
    SELECT class.oid::regclass AS name, class.relkind
    FROM pg_catalog.pg_class AS class
    JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
    WHERE namespace.nspname = 'public'
      AND class.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
  LOOP
    IF relation.relkind = 'S' THEN
      EXECUTE format('REVOKE ALL ON SEQUENCE %s FROM PUBLIC, carrier_app', relation.name);
    ELSE
      EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC, carrier_app', relation.name);
    END IF;
  END LOOP;
END
$carrier_revoke_relations$
"#;
    session.execute(sql).unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT has_table_privilege('public', 'records', 'SELECT'), \
                 has_table_privilege('carrier_app', 'records', 'SELECT')",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false), SqlValue::Bool(false)]]
    );
}

#[test]
fn carrier_context_key_block_installs_transaction_local_secret() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE SCHEMA IF NOT EXISTS carrier_private; \
             CREATE TABLE carrier_private.context_signing_keys (key_id SMALLINT PRIMARY KEY, \
             secret TEXT NOT NULL, installed_at TIMESTAMPTZ NOT NULL)",
        )
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute(
            "SELECT set_config('carrier.context_signing_key', \
             'test-signing-key-0123456789-abcdef', true)",
        )
        .unwrap();
    let sql = r#"
DO $carrier_context_key$
DECLARE
  supplied_key TEXT := current_setting('carrier.context_signing_key', true);
BEGIN
  IF supplied_key IS NULL OR length(supplied_key) < 32 THEN
    RAISE EXCEPTION 'missing key';
  END IF;
  INSERT INTO carrier_private.context_signing_keys (key_id, secret, installed_at)
  VALUES (1, supplied_key, clock_timestamp())
  ON CONFLICT (key_id) DO UPDATE
    SET secret = EXCLUDED.secret, installed_at = EXCLUDED.installed_at;
END
$carrier_context_key$
"#;
    session.execute(sql).unwrap();
    assert_eq!(
        session
            .execute("SELECT secret FROM carrier_private.context_signing_keys WHERE key_id = 1",)
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "test-signing-key-0123456789-abcdef".to_string()
        )]]
    );
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn plpgsql_raise_exception_is_distinct_from_an_exception_handler_clause() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"
CREATE FUNCTION carrier_guard(allowed BOOLEAN, operation TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT allowed THEN
    RAISE EXCEPTION 'operation % is denied', operation USING ERRCODE = '42501';
  END IF;
END
$$
"#,
        )
        .unwrap();

    session
        .execute("SELECT carrier_guard(TRUE, 'read')")
        .unwrap();
    let error = session
        .execute("SELECT carrier_guard(FALSE, 'write')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    assert_eq!(error.to_string(), "operation write is denied");
}

#[test]
fn distinct_from_precedes_boolean_operators_in_queries_and_plpgsql() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT
                   1 IS DISTINCT FROM 1 OR 2 IS DISTINCT FROM 1 AS distinct_or,
                   1 IS NOT DISTINCT FROM 1 OR 2 IS NOT DISTINCT FROM 1 AS not_distinct_or,
                   1 IS DISTINCT FROM 1
                     OR 2 IS DISTINCT FROM 1 AND 3 IS DISTINCT FROM 3 AS mixed_precedence,
                   TRUE IS DISTINCT FROM (FALSE OR TRUE) AS parenthesized_rhs",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );

    session
        .execute(
            r#"
CREATE FUNCTION arbitrary_binding_matches(selected_pid INTEGER, selected_tx BIGINT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
  IF selected_pid IS DISTINCT FROM pg_backend_pid()
     OR selected_tx IS DISTINCT FROM txid_current() THEN
    RETURN FALSE;
  END IF;
  RETURN TRUE;
END
$$
"#,
        )
        .unwrap();
    session.execute("BEGIN").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT
                   arbitrary_binding_matches(pg_backend_pid(), txid_current()),
                   arbitrary_binding_matches(pg_backend_pid() + 1, txid_current())",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(false)]]
    );
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn pgcrypto_hmac_sha256_supports_routine_variable_inputs() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"
CREATE FUNCTION sign_payload(payload_text TEXT, secret_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
  signature TEXT;
BEGIN
  signature := encode(
    public.hmac(
      convert_to(payload_text, 'UTF8'),
      convert_to(secret_text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );
  RETURN signature;
END
$$
"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT sign_payload('payload', 'secret')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4".to_string()
        )]]
    );
}

#[test]
fn pgcrypto_digest_supports_nested_routine_variable_functions() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"
CREATE FUNCTION digest_signature(signature_hex TEXT)
RETURNS BYTEA
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN public.digest(convert_to(lower(signature_hex), 'UTF8'), 'sha256');
END
$$
"#,
        )
        .unwrap();
    session
        .execute(
            r#"
CREATE FUNCTION signature_matches(signature_hex TEXT, expected_signature TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
  IF signature_hex IS NULL
     OR length(signature_hex) <> 64
     OR public.digest(convert_to(lower(signature_hex), 'UTF8'), 'sha256')
        IS DISTINCT FROM public.digest(convert_to(expected_signature, 'UTF8'), 'sha256') THEN
    RETURN FALSE;
  END IF;
  RETURN TRUE;
END
$$
"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT encode(digest_signature('ABCDEF'), 'hex')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721".to_string()
        )]]
    );
    let signature = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    assert_eq!(
        session
            .execute(&format!(
                "SELECT signature_matches('{}', '{}')",
                signature.to_ascii_uppercase(),
                signature
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn txid_current_is_stable_inside_an_explicit_transaction() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    let first = session.execute("SELECT txid_current()").unwrap().rows;
    let second = session
        .execute("SELECT pg_catalog.txid_current()")
        .unwrap()
        .rows;
    assert_eq!(first, second);
    assert!(matches!(
        first.as_slice(),
        [row] if matches!(row.as_slice(), [SqlValue::Int(value)] if *value > 0)
    ));
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn carrier_authored_function_grant_block_provisions_execute_privileges() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE carrier_app LOGIN NOSUPERUSER NOBYPASSRLS")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION reviewed_helper(value INTEGER) RETURNS INTEGER LANGUAGE plpgsql \
             AS $$ BEGIN RETURN value + 1; END $$",
        )
        .unwrap();
    session
        .execute("REVOKE EXECUTE ON FUNCTION reviewed_helper(INTEGER) FROM PUBLIC, carrier_app")
        .unwrap();
    let block = r#"
DO $carrier_grant_authored_functions$
DECLARE
  requested RECORD;
  implementation RECORD;
BEGIN
  FOR requested IN
    SELECT * FROM (VALUES
      ('public', 'reviewed_helper', 1::SMALLINT)
    ) AS required(schema_name, function_name, argument_count)
  LOOP
    FOR implementation IN
      SELECT procedure.oid::regprocedure AS identity
      FROM pg_catalog.pg_proc AS procedure
      JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
      WHERE namespace.nspname = requested.schema_name
        AND procedure.proname = requested.function_name
    LOOP
      EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO carrier_app', implementation.identity);
    END LOOP;
  END LOOP;
END
$carrier_grant_authored_functions$
"#;
    session.execute(block).unwrap();
    session
        .execute("SET SESSION AUTHORIZATION carrier_app")
        .unwrap();
    assert_eq!(
        session.execute("SELECT reviewed_helper(6)").unwrap().rows,
        vec![vec![SqlValue::Int(7)]]
    );
}

#[test]
fn plpgsql_function_accepts_explicit_void_return_type() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE OR REPLACE FUNCTION carrier_add_fk_if_possible(
                 source_table text,
                 constraint_name text,
                 source_column text,
                 target_table text,
                 target_column text DEFAULT 'id'
               ) RETURNS void
               LANGUAGE plpgsql
               AS $$
               BEGIN
                 RETURN;
               END;
               $$"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT pg_get_function_result(oid) FROM pg_proc \
                 WHERE proname = 'carrier_add_fk_if_possible'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("void".to_string())]]
    );
}

#[test]
fn plpgsql_dynamic_execute_supports_single_alter_table_statement() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id UUID PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE observations (id UUID PRIMARY KEY, patient_id UUID)")
        .unwrap();
    session
        .execute(
            r#"CREATE OR REPLACE FUNCTION carrier_add_fk_if_possible(
                 source_table text,
                 constraint_name text,
                 source_column text,
                 target_table text,
                 target_column text DEFAULT 'id'
               ) RETURNS void
               LANGUAGE plpgsql
               AS $$
               BEGIN
                 IF to_regclass('public.' || source_table) IS NOT NULL
                    AND to_regclass('public.' || target_table) IS NOT NULL
                    AND NOT EXISTS (
                      SELECT 1 FROM pg_constraint WHERE conname = constraint_name
                    ) THEN
                   EXECUTE format(
                     'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I(%I)',
                     source_table,
                     constraint_name,
                     source_column,
                     target_table,
                     target_column
                   );
                 END IF;
               END;
               $$"#,
        )
        .unwrap();
    session
        .execute(
            "SELECT carrier_add_fk_if_possible('observations', \
             'observations_patient_id_dynamic_fkey', 'patient_id', 'patients')",
        )
        .unwrap();

    session
        .execute(
            "SELECT carrier_add_fk_if_possible('observations', \
             'observations_patient_id_dynamic_fkey', 'patient_id', 'patients')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT count(*) FROM pg_constraint \
                 WHERE conname = 'observations_patient_id_dynamic_fkey'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn serial_and_identity_columns_insert_defaults_and_catalogs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE invoices (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE events (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)",
        )
        .unwrap();

    session
        .execute("INSERT INTO invoices (label) VALUES ('first'), ('second')")
        .unwrap();
    session
        .execute("INSERT INTO events (name) VALUES ('created')")
        .unwrap();
    session
        .execute("INSERT INTO events (id, name) VALUES (42, 'manual')")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT id, label FROM invoices ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("first".to_string())],
            vec![SqlValue::Int(2), SqlValue::String("second".to_string())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id, name FROM events ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::String("created".to_string())],
            vec![SqlValue::Int(42), SqlValue::String("manual".to_string())],
        ]
    );

    assert!(session
        .execute("SELECT sequence_name FROM information_schema.sequences WHERE sequence_name = 'invoices_id_seq'")
        .unwrap()
        .rows
        .iter()
        .any(|row| row[0] == SqlValue::String("invoices_id_seq".to_string())));
    assert_eq!(
        session
            .execute(
                "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'events_id_seq'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("events_id_seq".to_string()),
            SqlValue::String("S".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT sequencename, last_value FROM pg_catalog.pg_sequences WHERE sequencename = 'invoices_id_seq'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("invoices_id_seq".to_string()),
            SqlValue::Int(2),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT pg_get_serial_sequence('invoices', 'id')::regclass")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("invoices_id_seq".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT pg_get_serial_sequence('public.events', 'id')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("events_id_seq".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT pg_get_serial_sequence('invoices', 'label')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    assert_eq!(
        session
            .execute("SELECT attname, atthasdef, attidentity FROM pg_catalog.pg_attribute WHERE attname = 'id' ORDER BY attidentity")
            .unwrap()
            .rows
            .into_iter()
            .filter(|row| row[1] == SqlValue::Bool(true))
            .collect::<Vec<_>>(),
        vec![
            vec![
                SqlValue::String("id".to_string()),
                SqlValue::Bool(true),
                SqlValue::String(String::new()),
            ],
        ]
    );
    session
        .execute(
            r#"
            CREATE VIEW postgres_sequences AS
             SELECT seq_pg_class.relname AS seq_name,
                dep_pg_class.relname AS table_name,
                pg_attribute.attname AS col_name,
                pg_sequence.seqmax AS seq_max,
                pg_sequence.seqmin AS seq_min,
                pg_sequence.seqstart AS seq_start,
                pg_sequence_last_value((pg_sequence.seqrelid)::regclass) AS last_value
               FROM ((((pg_class seq_pg_class
                 JOIN pg_sequence ON ((seq_pg_class.oid = pg_sequence.seqrelid)))
                 LEFT JOIN pg_depend ON (((seq_pg_class.oid = pg_depend.objid) AND (pg_depend.classid = ('pg_class'::regclass)::oid) AND (pg_depend.refclassid = ('pg_class'::regclass)::oid))))
                 LEFT JOIN pg_class dep_pg_class ON ((pg_depend.refobjid = dep_pg_class.oid)))
                 LEFT JOIN pg_attribute ON (((dep_pg_class.oid = pg_attribute.attrelid) AND (pg_depend.refobjsubid = pg_attribute.attnum))))
              WHERE (seq_pg_class.relkind = 'S'::"char")
            "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT "postgres_sequences".*
                   FROM "postgres_sequences"
                   WHERE "postgres_sequences"."seq_name" = 'invoices_id_seq'
                   LIMIT 1"#
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("invoices_id_seq".to_string()),
            SqlValue::String("invoices".to_string()),
            SqlValue::String("id".to_string()),
            SqlValue::Int(i32::MAX as i64),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Int(2),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT seq_name
                 FROM postgres_sequences
                 WHERE table_name = 'events' AND col_name = 'id'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("events_id_seq".to_string())]]
    );
}

#[test]
fn typed_serial_and_identity_ranges_restarts_and_dependencies_match_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE sequence_types (
                s SMALLSERIAL,
                i SERIAL,
                b BIGSERIAL,
                si SMALLINT GENERATED BY DEFAULT AS IDENTITY,
                ia INTEGER GENERATED ALWAYS AS IDENTITY,
                sd BIGINT GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1)
            )",
        )
        .unwrap();

    for (sequence, oid, minimum, maximum, start, increment) in [
        ("sequence_types_s_seq", 21, 1, i16::MAX as i64, 1, 1),
        ("sequence_types_i_seq", 23, 1, i32::MAX as i64, 1, 1),
        ("sequence_types_b_seq", 20, 1, i64::MAX, 1, 1),
        ("sequence_types_si_seq", 21, 1, i16::MAX as i64, 1, 1),
        ("sequence_types_ia_seq", 23, 1, i32::MAX as i64, 1, 1),
        ("sequence_types_sd_seq", 20, i64::MIN, -1, -1, -1),
    ] {
        assert_eq!(
            session
                .execute(&format!(
                    "SELECT seqtypid, seqmin, seqmax, seqstart, seqincrement
                     FROM pg_catalog.pg_sequence
                     WHERE seqrelid = '{sequence}'::regclass"
                ))
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(oid),
                SqlValue::Int(minimum),
                SqlValue::Int(maximum),
                SqlValue::Int(start),
                SqlValue::Int(increment),
            ]]
        );
    }

    assert_eq!(
        session
            .execute(
                "SELECT is_nullable, column_default, is_identity,
                        identity_generation, identity_start, identity_increment,
                        identity_maximum, identity_minimum, identity_cycle
                 FROM information_schema.columns
                 WHERE table_name = 'sequence_types' AND column_name = 'si'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("NO".to_string()),
            SqlValue::Null,
            SqlValue::String("YES".to_string()),
            SqlValue::String("BY DEFAULT".to_string()),
            SqlValue::String("1".to_string()),
            SqlValue::String("1".to_string()),
            SqlValue::String(i16::MAX.to_string()),
            SqlValue::String("1".to_string()),
            SqlValue::String("NO".to_string()),
        ]]
    );

    let generated_always = session
        .execute("INSERT INTO sequence_types (ia) VALUES (42)")
        .unwrap_err();
    assert_eq!(generated_always.sqlstate(), "428C9");
    assert_eq!(
        session
            .execute("INSERT INTO sequence_types DEFAULT VALUES RETURNING s, i, b, si, ia, sd")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Int(1),
            SqlValue::Int(-1),
        ]]
    );

    let invalid_restart = session
        .execute("ALTER SEQUENCE sequence_types_s_seq RESTART WITH 32768")
        .unwrap_err();
    assert_eq!(invalid_restart.sqlstate(), "22023");
    let invalid_start = session
        .execute("CREATE SEQUENCE invalid_small AS SMALLINT START WITH 32768")
        .unwrap_err();
    assert_eq!(invalid_start.sqlstate(), "22023");

    session
        .execute("ALTER SEQUENCE sequence_types_s_seq RESTART WITH 32767")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT nextval('sequence_types_s_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(i16::MAX as i64)]]
    );
    let overflow = session
        .execute("SELECT nextval('sequence_types_s_seq')")
        .unwrap_err();
    assert_eq!(overflow.sqlstate(), "2200H");
    let setval = session
        .execute("SELECT setval('sequence_types_s_seq', 32768)")
        .unwrap_err();
    assert_eq!(setval.sqlstate(), "22003");

    session
        .execute("ALTER TABLE sequence_types ALTER COLUMN ia RESTART WITH 20")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "INSERT INTO sequence_types (s, i, b, si, sd)
                 VALUES (10, 10, 10, 10, 10) RETURNING ia",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(20)]]
    );
    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE sequence_types ALTER COLUMN ia RESTART WITH 30")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT nextval('sequence_types_ia_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(21)]]
    );

    let owned = session
        .execute("DROP SEQUENCE sequence_types_i_seq")
        .unwrap_err();
    assert_eq!(owned.sqlstate(), "2BP01");
    session
        .execute("DROP SEQUENCE sequence_types_i_seq CASCADE")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT column_default FROM information_schema.columns
                 WHERE table_name = 'sequence_types' AND column_name = 'i'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]]
    );
    session
        .execute("ALTER SEQUENCE sequence_types_b_seq OWNED BY NONE")
        .unwrap();
    let detached_default = session
        .execute("DROP SEQUENCE sequence_types_b_seq")
        .unwrap_err();
    assert_eq!(detached_default.sqlstate(), "2BP01");
    session
        .execute("DROP SEQUENCE sequence_types_b_seq CASCADE")
        .unwrap();
    let identity_owned = session
        .execute("DROP SEQUENCE sequence_types_si_seq CASCADE")
        .unwrap_err();
    assert_eq!(identity_owned.sqlstate(), "2BP01");
    let identity_owner_change = session
        .execute("ALTER SEQUENCE sequence_types_si_seq OWNED BY NONE")
        .unwrap_err();
    assert_eq!(identity_owner_change.sqlstate(), "0A000");
    let identity_drop_default = session
        .execute("ALTER TABLE sequence_types ALTER COLUMN si DROP DEFAULT")
        .unwrap_err();
    assert_eq!(identity_drop_default.sqlstate(), "42601");

    session
        .execute("ALTER TABLE sequence_types RENAME COLUMN s TO renamed_s")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT pg_get_serial_sequence('sequence_types', 'renamed_s')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("sequence_types_s_seq".to_string())]]
    );
    session
        .execute("ALTER TABLE sequence_types DROP COLUMN renamed_s")
        .unwrap();
    assert!(session
        .execute(
            "SELECT relname FROM pg_catalog.pg_class
             WHERE relname = 'sequence_types_s_seq'",
        )
        .unwrap()
        .rows
        .is_empty());

    session
        .execute("CREATE SEQUENCE descending AS INTEGER INCREMENT BY -1")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT nextval('descending'), nextval('descending')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(-1), SqlValue::Int(-2)]]
    );

    session
        .execute("CREATE TABLE generated_add (label TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO generated_add (label) VALUES ('a'), ('b')")
        .unwrap();
    session
        .execute("ALTER TABLE generated_add ADD COLUMN serial_value SMALLSERIAL")
        .unwrap();
    session
        .execute(
            "ALTER TABLE generated_add ADD COLUMN identity_value
             INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 10)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT label, serial_value, identity_value
                 FROM generated_add ORDER BY label",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::Int(1),
                SqlValue::Int(10),
            ],
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::Int(2),
                SqlValue::Int(11),
            ],
        ]
    );

    drop(session);
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT seqtypid, seqmin, seqmax FROM pg_catalog.pg_sequence
                 WHERE seqrelid = 'sequence_types_si_seq'::regclass",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(21),
            SqlValue::Int(1),
            SqlValue::Int(i16::MAX as i64),
        ]]
    );
}

#[test]
fn setval_accepts_pg_get_serial_sequence_and_scalar_subquery_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE group_push_rules (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();
    session
        .execute("INSERT INTO group_push_rules (label) VALUES ('first')")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT setval(
                   pg_get_serial_sequence('group_push_rules', 'id'),
                   GREATEST(
                     (SELECT COALESCE(MAX(id), 0) FROM group_push_rules) + 1000,
                     nextval(pg_get_serial_sequence('group_push_rules', 'id'))
                   )
                 )"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1001)]]
    );
    assert_eq!(
        session
            .execute("SELECT nextval(pg_get_serial_sequence('group_push_rules', 'id'))")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1002)]]
    );
}

#[test]
fn sequence_nextval_is_not_rolled_back_with_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE invoices (id SERIAL PRIMARY KEY, label TEXT)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO invoices (label) VALUES ('rolled back')")
        .unwrap();
    session.execute("ROLLBACK").unwrap();

    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM invoices")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(0)
    );
    session
        .execute("INSERT INTO invoices (label) VALUES ('kept')")
        .unwrap();
    assert_eq!(
        session.execute("SELECT id FROM invoices").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn jsonb_operators_and_vector_ordering_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE memories (id TEXT PRIMARY KEY, content TEXT, metadata JSONB, embedding VECTOR(3))")
        .unwrap();
    session
        .execute("INSERT INTO memories (id, content, metadata, embedding) VALUES ('m1', 'clinic', '{\"clinic\":\"rural-7\"}'::jsonb, '[1,0,0]')")
        .unwrap();
    session
        .execute("INSERT INTO memories (id, content, metadata, embedding) VALUES ('m2', 'other', '{\"clinic\":\"urban-1\"}'::jsonb, '[0,1,0]')")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT metadata->>'clinic' FROM memories WHERE metadata @> '{\"clinic\":\"rural-7\"}'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("rural-7".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT id FROM memories ORDER BY embedding <=> '[1,0,0]' LIMIT 1")
            .unwrap()
            .rows[0][0],
        SqlValue::String("m1".to_string())
    );

    drop(session);
    db.create_vector_index("memories", HnswIndexConfig::default())
        .unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute("SET bicdb.vector_search = 'ann'")
            .unwrap()
            .command_tag
            .as_deref(),
        Some("SET")
    );
    session.execute("SET bicdb.ef_search = 32").unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM memories ORDER BY embedding <=> '[1,0,0]' LIMIT 1")
            .unwrap()
            .rows[0][0],
        SqlValue::String("m1".to_string())
    );
}

#[test]
fn postgres_jsonb_scalar_null_path_and_containment_semantics_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE jsonb_parity (
                id TEXT PRIMARY KEY,
                value JSONB,
                payload JSONB
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO jsonb_parity (id, value, payload) VALUES
                ('sql_null', NULL, NULL),
                ('json_null', 'null'::jsonb, 'null'::jsonb),
                ('string', '"hello"'::jsonb, '[{"x":1}]'::jsonb),
                ('number', '1.00'::jsonb, '[]'::jsonb),
                ('boolean', 'true'::jsonb, '{}'::jsonb),
                ('object', '{"a":null,"arr":[10,20],"nested":{"key":"value"}}'::jsonb,
                           '[{"x":1}]'::jsonb)"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT id, jsonb_typeof(value), value IS NULL
                 FROM jsonb_parity
                 ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("boolean".to_string()),
                SqlValue::String("boolean".to_string()),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("json_null".to_string()),
                SqlValue::String("null".to_string()),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("number".to_string()),
                SqlValue::String("number".to_string()),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("object".to_string()),
                SqlValue::String("object".to_string()),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::String("sql_null".to_string()),
                SqlValue::Null,
                SqlValue::Bool(true),
            ],
            vec![
                SqlValue::String("string".to_string()),
                SqlValue::String("string".to_string()),
                SqlValue::Bool(false),
            ],
        ]
    );

    let extracted = session
        .execute(
            "SELECT value->'a', value->>'a', value->'arr'->0,
                    value->'arr'->>0, value->'arr'-> -1,
                    value->'missing', value->'nested'->'key',
                    payload->0->'x'
             FROM jsonb_parity
             WHERE id = 'object'",
        )
        .unwrap();
    assert_eq!(
        extracted.rows,
        vec![vec![
            SqlValue::Json(json!(null)),
            SqlValue::Null,
            SqlValue::Json(json!(10)),
            SqlValue::String("10".to_string()),
            SqlValue::Json(json!(20)),
            SqlValue::Null,
            SqlValue::Json(json!("value")),
            SqlValue::Json(json!(1)),
        ]]
    );
    assert_eq!(
        extracted.column_types,
        vec![
            Some("jsonb".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
        ]
    );

    let constants = session
        .execute(
            r#"SELECT
                '{"a":[10,20]}'::jsonb->'a'-> -1,
                '{"a":null}'::jsonb->>'a',
                '{"a":1}'::jsonb @> '{"a":null}'::jsonb,
                '{"a":null}'::jsonb @> '{"a":null}'::jsonb,
                '[1,2]'::jsonb @> '2'::jsonb,
                '[1,2,[1,3]]'::jsonb @> '[1,3]'::jsonb,
                '[[1,2]]'::jsonb @> '[[1]]'::jsonb,
                '{"a":[1,2]}'::jsonb @> '{"a":1}'::jsonb,
                '9007199254740993'::jsonb @> '9007199254740992'::jsonb,
                '1.00'::jsonb @> '1'::jsonb,
                '9007199254740993'::jsonb = '9007199254740992'::jsonb,
                '1.00'::jsonb = '1'::jsonb,
                '[null,1.00]'::jsonb = '[null,1]'::jsonb"#,
        )
        .unwrap();
    assert_eq!(
        constants.rows,
        vec![vec![
            SqlValue::Json(json!(20)),
            SqlValue::Null,
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(constants.column_types[0], Some("jsonb".to_string()));
    assert_eq!(constants.column_types[1], Some("text".to_string()));
    assert!(constants.column_types[2..]
        .iter()
        .all(|pg_type| pg_type.as_deref() == Some("bool")));
}

#[test]
fn postgres_jsonb_total_ordering_matches_container_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE jsonb_ordering (value jsonb PRIMARY KEY)")
        .unwrap();
    session
        .execute(
            r#"INSERT INTO jsonb_ordering (value) VALUES
                ('null'), ('"b"'), ('"a"'), ('1'), ('0'), ('-1'),
                ('true'), ('false'), ('[]'), ('[1]'), ('[1,2]'), ('[1,1]'),
                ('{}'), ('{"b":0}'), ('{"a":2}'), ('{"a":1}')"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT value FROM jsonb_ordering ORDER BY value")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Json(json!([]))],
            vec![SqlValue::Json(json!(null))],
            vec![SqlValue::Json(json!("a"))],
            vec![SqlValue::Json(json!("b"))],
            vec![SqlValue::Json(json!(-1))],
            vec![SqlValue::Json(json!(0))],
            vec![SqlValue::Json(json!(1))],
            vec![SqlValue::Json(json!(false))],
            vec![SqlValue::Json(json!(true))],
            vec![SqlValue::Json(json!([1]))],
            vec![SqlValue::Json(json!([1, 1]))],
            vec![SqlValue::Json(json!([1, 2]))],
            vec![SqlValue::Json(json!({}))],
            vec![SqlValue::Json(json!({"a": 1}))],
            vec![SqlValue::Json(json!({"a": 2}))],
            vec![SqlValue::Json(json!({"b": 0}))],
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT '1e100'::jsonb > '9e99'::jsonb,
                        '-1e100'::jsonb < '-9e99'::jsonb,
                        '1.20'::jsonb = '1.2'::jsonb,
                        '{\"aa\":0}'::jsonb > '{\"b\":0}'::jsonb",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
}

#[test]
fn postgres_jsonb_subscripts_are_zero_based_and_support_object_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE jsonb_subscripts (id text PRIMARY KEY, payload jsonb, labels text[])",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO jsonb_subscripts VALUES
                ('one', '{"items":[10,20],"0":"zero"}', ARRAY['first','second'])"#,
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT payload['items'][0], payload['items'][-1], payload[0],
                        payload['missing'], labels[1]
                 FROM jsonb_subscripts WHERE id = 'one'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!(10)),
            SqlValue::Json(json!(20)),
            SqlValue::Json(json!("zero")),
            SqlValue::Null,
            SqlValue::String("first".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute("SELECT ('[10,20]'::jsonb)[0], ('[10,20]'::jsonb)['1']")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!(10)), SqlValue::Json(json!(20))]]
    );
}

#[test]
fn postgres_jsonb_mutation_and_catalog_functions_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT jsonb_object(ARRAY['a','b'], ARRAY['1',NULL]),
                      jsonb_array_element('[10,20]'::jsonb, -1),
                      jsonb_object_field_text('{"a":null}'::jsonb, 'a'),
                      jsonb_insert('[0,1,2]'::jsonb, '{1}', '9', false),
                      jsonb_insert('[0,1,2]'::jsonb, '{1}', '9', true),
                      jsonb_set_lax('{"a":1}'::jsonb, '{a}', NULL, true, 'use_json_null'),
                      jsonb_set_lax('{"a":1}'::jsonb, '{a}', NULL, true, 'delete_key'),
                      jsonb_contains('{"a":1}'::jsonb, '{"a":1}'::jsonb),
                      jsonb_exists_any('{"a":1}'::jsonb, ARRAY['x','a'])"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Json(json!({"a": "1", "b": null})),
            SqlValue::Json(json!(20)),
            SqlValue::Null,
            SqlValue::Json(json!([0, 9, 1, 2])),
            SqlValue::Json(json!([0, 1, 9, 2])),
            SqlValue::Json(json!({"a": null})),
            SqlValue::Json(json!({})),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("bool".to_string()),
            Some("bool".to_string()),
        ]
    );
}

#[test]
fn postgres_jsonb_composite_primary_keys_preserve_json_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE jsonb_composite_pk (
                id jsonb,
                shard text,
                PRIMARY KEY (id, shard)
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO jsonb_composite_pk (id, shard) VALUES
                ('"x"'::jsonb, 'a'),
                ('null'::jsonb, 'b'),
                ('["null","b"]'::jsonb, 'c')"#,
        )
        .unwrap();

    let result = session
        .execute("SELECT id, jsonb_typeof(id), shard FROM jsonb_composite_pk ORDER BY shard")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Json(json!("x")),
                SqlValue::String("string".to_string()),
                SqlValue::String("a".to_string()),
            ],
            vec![
                SqlValue::Json(json!(null)),
                SqlValue::String("null".to_string()),
                SqlValue::String("b".to_string()),
            ],
            vec![
                SqlValue::Json(json!(["null", "b"])),
                SqlValue::String("array".to_string()),
                SqlValue::String("c".to_string()),
            ],
        ]
    );
    assert_eq!(result.column_types[0], Some("jsonb".to_string()));
}

#[test]
fn postgres_jsonb_unique_keys_use_exact_canonical_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE jsonb_unique_values (id text PRIMARY KEY, value jsonb UNIQUE)")
        .unwrap();
    session
        .execute(
            "INSERT INTO jsonb_unique_values (id, value) VALUES
                ('one', '1'::jsonb),
                ('big-a', '9007199254740992'::jsonb),
                ('big-b', '9007199254740993'::jsonb),
                ('json-null', 'null'::jsonb),
                ('sql-null-a', NULL),
                ('sql-null-b', NULL)",
        )
        .unwrap();

    let numeric_duplicate = session
        .execute("INSERT INTO jsonb_unique_values (id, value) VALUES ('one-decimal', '1.0'::jsonb)")
        .unwrap_err();
    assert_eq!(numeric_duplicate.sqlstate(), "23505");
    let json_null_duplicate = session
        .execute(
            "INSERT INTO jsonb_unique_values (id, value) VALUES ('json-null-2', 'null'::jsonb)",
        )
        .unwrap_err();
    assert_eq!(json_null_duplicate.sqlstate(), "23505");
}

#[test]
fn postgres_json_constructors_mutation_and_carrier_operators_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let functions = session
        .execute(
            r#"SELECT
                jsonb_build_object('a', 1, 'missing', NULL),
                jsonb_build_array(1, NULL, 'x'),
                json_build_object('a', true),
                json_array_length('[1,null,3]'::json),
                jsonb_array_length('[1,2]'::jsonb),
                jsonb_set('{"a":[1,2]}'::jsonb, '{a,-1}'::text[], '9'::jsonb),
                jsonb_strip_nulls('{"a":null,"b":[null,1]}'::jsonb),
                to_jsonb('plain text'::text)"#,
        )
        .unwrap();
    assert_eq!(
        functions.rows,
        vec![vec![
            SqlValue::Json(json!({"a": 1, "missing": null})),
            SqlValue::Json(json!([1, null, "x"])),
            SqlValue::JsonText(PgJsonText::parse(r#"{"a" : true}"#.to_string()).unwrap()),
            SqlValue::Int(3),
            SqlValue::Int(2),
            SqlValue::Json(json!({"a": [1, 9]})),
            SqlValue::Json(json!({"b": [null, 1]})),
            SqlValue::Json(json!("plain text")),
        ]]
    );
    assert_eq!(
        functions.column_types,
        vec![
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("json".to_string()),
            Some("int4".to_string()),
            Some("int4".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
        ]
    );

    let operators = session
        .execute(
            r#"SELECT
                '{"a":{"b":[10,null]},"drop":1}'::jsonb #>> '{a,b,0}'::text[],
                '{"a":{"b":[10,null]}}'::jsonb #> '{a,b,1}'::text[],
                '{"a":1}'::jsonb ? 'a',
                '["a","b",1]'::jsonb ? 'b',
                '{"a":1,"b":2}'::jsonb - 'a',
                '["a","b","a"]'::jsonb - 'a',
                '[10,20,30]'::jsonb - -1,
                '{"a":{"b":[10,20]}}'::jsonb #- '{a,b,-1}'::text[],
                '{"a":1,"b":2}'::jsonb - ARRAY['a', 'missing'],
                '["a","b",1]'::jsonb - ARRAY['a', 'missing'],
                '"scalar"'::jsonb ? 'scalar',
                '{}'::jsonb ?& ARRAY[NULL]::text[],
                '{}'::jsonb ?| ARRAY[NULL]::text[],
                '1'::jsonb || '2'::jsonb,
                '{"a":1}'::jsonb || '2'::jsonb"#,
        )
        .unwrap();
    assert_eq!(
        operators.rows,
        vec![vec![
            SqlValue::String("10".to_string()),
            SqlValue::Json(json!(null)),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Json(json!({"b": 2})),
            SqlValue::Json(json!(["b"])),
            SqlValue::Json(json!([10, 20])),
            SqlValue::Json(json!({"a": {"b": [10]}})),
            SqlValue::Json(json!({"b": 2})),
            SqlValue::Json(json!(["b", 1])),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Json(json!([1, 2])),
            SqlValue::Json(json!([{"a": 1}, 2])),
        ]]
    );
    assert_eq!(operators.column_types[0], Some("text".to_string()));
    assert_eq!(operators.column_types[1], Some("jsonb".to_string()));
    assert_eq!(operators.column_types[2], Some("bool".to_string()));
    assert_eq!(operators.column_types[3], Some("bool".to_string()));
    assert!(operators.column_types[4..10]
        .iter()
        .all(|pg_type| pg_type.as_deref() == Some("jsonb")));
    assert_eq!(operators.column_types[10], Some("bool".to_string()));
    assert_eq!(operators.column_types[11], Some("bool".to_string()));
    assert_eq!(operators.column_types[12], Some("bool".to_string()));
    assert!(operators.column_types[13..]
        .iter()
        .all(|pg_type| pg_type.as_deref() == Some("jsonb")));

    let lazy_delete_paths = session
        .execute(
            r#"SELECT '{}'::jsonb #- ARRAY[NULL]::text[],
                      '{}'::jsonb #- ARRAY['missing', NULL]::text[],
                      '[]'::jsonb #- ARRAY['x']::text[],
                      '{"a":1}'::jsonb #- '{a,b}'::text[]"#,
        )
        .unwrap();
    assert_eq!(
        lazy_delete_paths.rows,
        vec![vec![
            SqlValue::Json(json!({})),
            SqlValue::Json(json!({})),
            SqlValue::Json(json!([])),
            SqlValue::Json(json!({"a": 1})),
        ]]
    );

    let reached_null_path = session
        .execute(r#"SELECT '{"a":1}'::jsonb #- ARRAY['a', NULL]::text[]"#)
        .unwrap_err();
    assert_eq!(reached_null_path.sqlstate(), "22004");

    session
        .execute(
            "CREATE TABLE json_dynamic_paths (
                id text PRIMARY KEY,
                doc jsonb NOT NULL,
                path_key text NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            r#"INSERT INTO json_dynamic_paths (id, doc, path_key)
               VALUES ('one', '{"a":1,"b":2}'::jsonb, 'a')"#,
        )
        .unwrap();
    let dynamic_paths = session
        .execute(
            r#"SELECT doc -> path_key, doc ->> path_key,
                      '{"a":1,"b":2}'::jsonb -> path_key
               FROM json_dynamic_paths"#,
        )
        .unwrap();
    assert_eq!(
        dynamic_paths.rows,
        vec![vec![
            SqlValue::Json(json!(1)),
            SqlValue::String("1".to_string()),
            SqlValue::Json(json!(1)),
        ]]
    );
    assert_eq!(
        dynamic_paths.column_types,
        vec![
            Some("jsonb".to_string()),
            Some("text".to_string()),
            Some("jsonb".to_string()),
        ]
    );
}

#[test]
fn postgres_jsonb_agg_null_ordering_distinct_and_grouping_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE jsonb_agg_parity (
                id TEXT PRIMARY KEY,
                bucket TEXT,
                label TEXT,
                rank_value INTEGER,
                tie_value TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO jsonb_agg_parity
                (id, bucket, label, rank_value, tie_value) VALUES
                ('a', 'g1', 'alpha', 1, 'b'),
                ('b', 'g1', 'beta', 1, NULL),
                ('c', 'g1', 'gamma', 1, 'a'),
                ('d', 'g2', 'delta', 2, NULL),
                ('e', 'g2', NULL, 3, 'c')",
        )
        .unwrap();

    let ordered = session
        .execute(
            "SELECT
                jsonb_agg(label ORDER BY rank_value ASC, tie_value ASC) AS ascending,
                jsonb_agg(label ORDER BY rank_value DESC, tie_value DESC) AS descending,
                jsonb_agg(label ORDER BY rank_value ASC, tie_value DESC NULLS LAST) AS explicit_nulls,
                json_agg(label ORDER BY rank_value ASC, tie_value ASC) AS json_ascending
             FROM jsonb_agg_parity",
        )
        .unwrap();
    assert_eq!(
        ordered.rows,
        vec![vec![
            SqlValue::Json(json!(["gamma", "alpha", "beta", "delta", null])),
            SqlValue::Json(json!([null, "delta", "beta", "alpha", "gamma"])),
            SqlValue::Json(json!(["alpha", "gamma", "beta", "delta", null])),
            SqlValue::JsonText(
                PgJsonText::parse(r#"["gamma", "alpha", "beta", "delta", null]"#.to_string())
                    .unwrap()
            ),
        ]]
    );
    assert_eq!(
        ordered.column_types,
        vec![
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("jsonb".to_string()),
            Some("json".to_string()),
        ]
    );

    let empty = session
        .execute("SELECT jsonb_agg(label) FROM jsonb_agg_parity WHERE id = 'missing'")
        .unwrap();
    assert_eq!(empty.rows, vec![vec![SqlValue::Null]]);
    assert_eq!(empty.column_types, vec![Some("jsonb".to_string())]);

    let grouped = session
        .execute(
            "SELECT bucket,
                    jsonb_agg(
                        jsonb_build_object('label', label, 'rank', rank_value)
                        ORDER BY rank_value DESC, tie_value ASC
                    ) AS items
             FROM jsonb_agg_parity
             GROUP BY bucket
             ORDER BY bucket",
        )
        .unwrap();
    assert_eq!(
        grouped.rows,
        vec![
            vec![
                SqlValue::String("g1".to_string()),
                SqlValue::Json(json!([
                    {"label": "gamma", "rank": 1},
                    {"label": "alpha", "rank": 1},
                    {"label": "beta", "rank": 1}
                ])),
            ],
            vec![
                SqlValue::String("g2".to_string()),
                SqlValue::Json(json!([
                    {"label": null, "rank": 3},
                    {"label": "delta", "rank": 2}
                ])),
            ],
        ]
    );
    assert_eq!(grouped.column_types[1], Some("jsonb".to_string()));

    session
        .execute("CREATE TABLE jsonb_agg_roles (id TEXT PRIMARY KEY, role TEXT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO jsonb_agg_roles (id, role) VALUES
                ('a', ' admin '), ('b', 'admin'), ('c', NULL),
                ('d', NULL), ('e', 'editor')",
        )
        .unwrap();
    let distinct = session
        .execute(
            "SELECT jsonb_agg(DISTINCT trim(role) ORDER BY trim(role))
             FROM jsonb_agg_roles",
        )
        .unwrap();
    assert_eq!(
        distinct.rows,
        vec![vec![SqlValue::Json(json!(["admin", "editor", null]))]]
    );
    assert_eq!(distinct.column_types, vec![Some("jsonb".to_string())]);

    let error = session
        .execute(
            "SELECT jsonb_agg(DISTINCT role ORDER BY id)
             FROM jsonb_agg_roles",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P10");
    assert!(error
        .to_string()
        .contains("ORDER BY expressions must appear in argument list"));

    session
        .execute("CREATE TABLE jsonb_distinct_numbers (id bigint PRIMARY KEY, value jsonb)")
        .unwrap();
    session
        .execute(
            "INSERT INTO jsonb_distinct_numbers (id, value) VALUES
                (1, '1'::jsonb),
                (2, '1.0'::jsonb),
                (3, '9007199254740992'::jsonb),
                (4, '9007199254740993'::jsonb)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT jsonb_agg(DISTINCT value) FROM jsonb_distinct_numbers")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Json(json!([
            1,
            9007199254740992_i64,
            9007199254740993_i64
        ]))]]
    );
}

#[test]
fn expired_deadline_aborts_full_scan_with_query_canceled_sqlstate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("scan_timeout").unwrap();
    db.batch_insert(
        "scan_timeout",
        (0..2_000)
            .map(|idx| Record::new(format!("r{idx:04}")).with_metadata(json!({"value": idx})))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let error = SqlEngine::new(&db)
        .with_cancellation(expired_cancellation())
        .execute("SELECT id FROM scan_timeout WHERE id <> 'missing' ORDER BY id")
        .unwrap_err();
    assert!(matches!(error, SqlError::BicDb(BicDbError::QueryTimedOut)));
    assert_eq!(error.sqlstate(), "57014");
}

#[test]
fn expired_deadline_aborts_vector_ordering_with_query_canceled_sqlstate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("vector_timeout").unwrap();
    db.batch_insert(
        "vector_timeout",
        (0..2_000)
            .map(|idx| {
                let x = (idx % 100) as f32 / 100.0;
                Record::new(format!("v{idx:04}")).with_vector(vec![x, 1.0 - x, 0.5])
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let error = SqlEngine::new(&db)
        .with_cancellation(expired_cancellation())
        .execute("SELECT id FROM vector_timeout ORDER BY embedding <=> '[1,0,0]' LIMIT 5")
        .unwrap_err();
    assert!(matches!(error, SqlError::BicDb(BicDbError::QueryTimedOut)));
    assert_eq!(error.sqlstate(), "57014");
}

#[test]
fn routine_grants_accept_multiline_keywords_and_preserve_quoted_role_names() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE ROLE routine_owner SUPERUSER")
        .unwrap();
    session
        .execute("CREATE ROLE \"reader FROM archive\"")
        .unwrap();
    session.execute("CREATE FUNCTION checked_value() RETURNS INTEGER LANGUAGE plpgsql AS $$ BEGIN RETURN 7; END $$").unwrap();
    for separator in ["\n", "\t", "\r\n", "  "] {
        let revoke = [
            "REVOKE",
            "ALL",
            "ON",
            "FUNCTION",
            "checked_value()",
            "FROM",
            "PUBLIC, \"reader FROM archive\"",
        ]
        .join(separator);
        session.execute(&revoke).unwrap();
        session
            .execute("SET SESSION AUTHORIZATION \"reader FROM archive\"")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT checked_value()")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
        let grant = [
            "GRANT",
            "EXECUTE",
            "ON",
            "FUNCTION",
            "checked_value()",
            "TO",
            "\"reader FROM archive\"",
        ]
        .join(separator);
        session.execute(&grant).unwrap();
        let owner_grant = [
            "GRANT",
            "EXECUTE",
            "ON",
            "FUNCTION",
            "checked_value()",
            "TO",
            "routine_owner",
            "WITH",
            "GRANT",
            "OPTION",
        ]
        .join(separator);
        session.execute(&owner_grant).unwrap();
        session
            .execute("SET SESSION AUTHORIZATION \"reader FROM archive\"")
            .unwrap();
        assert_eq!(
            session.execute("SELECT checked_value()").unwrap().rows,
            vec![vec![SqlValue::Int(7)]]
        );
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
    }
}
