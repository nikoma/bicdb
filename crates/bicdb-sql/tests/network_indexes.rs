use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn plan_lines(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect()
}

#[test]
fn network_btree_and_hash_indexes_are_canonical_and_durable() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE network_index_values (
                    id int4 PRIMARY KEY,
                    address inet NOT NULL,
                    mac macaddr NOT NULL
                 );
                 INSERT INTO network_index_values VALUES
                    (1, '192.168.1.1/24', '08:00:2b:01:02:03'),
                    (2, '192.168.1.0/25', '08:00:2b:01:02:04'),
                    (3, '192.168.0.255/32', '08:00:2b:01:02:05'),
                    (4, '192.168.1.200/24', '08:00:2b:01:02:06');
                 CREATE INDEX idx_network_address
                    ON network_index_values USING btree (address);
                 CREATE INDEX idx_network_mac
                    ON network_index_values USING hash (mac);",
            )
            .unwrap();

        assert!(plan_lines(
            &mut session,
            "SELECT id FROM network_index_values WHERE address = '192.168.1.1/24'::inet",
        )
        .iter()
        .any(|line| line.contains("IndexScan idx_network_address")));
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM network_index_values WHERE mac = '08002b:010204'::macaddr",
        )
        .iter()
        .any(|line| line.contains("IndexScan idx_network_mac")));
        assert!(!plan_lines(
            &mut session,
            "SELECT id FROM network_index_values WHERE mac > '08:00:2b:01:02:03'::macaddr",
        )
        .iter()
        .any(|line| line.contains("IndexRangeScan idx_network_mac")));
        assert!(!plan_lines(
            &mut session,
            "SELECT mac FROM network_index_values ORDER BY mac",
        )
        .iter()
        .any(|line| line.contains("OrderedIndexScan idx_network_mac")));
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM network_index_values WHERE address >= '192.168.1.1/24'::inet",
        )
        .iter()
        .any(|line| line.contains("IndexRangeScan idx_network_address")));
        assert!(plan_lines(
            &mut session,
            "SELECT address FROM network_index_values ORDER BY address",
        )
        .iter()
        .any(|line| line.contains("OrderedIndexScan idx_network_address")));
        assert_eq!(
            session
                .execute("SELECT address FROM network_index_values ORDER BY address")
                .unwrap()
                .rows,
            [
                "192.168.0.255/32",
                "192.168.1.1/24",
                "192.168.1.200/24",
                "192.168.1.0/25",
            ]
            .into_iter()
            .map(|value| vec![SqlValue::String(value.to_string())])
            .collect::<Vec<_>>(),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT indexdef FROM pg_indexes
                     WHERE tablename = 'network_index_values'
                       AND indexname = 'idx_network_mac'",
                )
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "CREATE INDEX idx_network_mac ON network_index_values USING hash (mac)",
        );

        session
            .execute(
                "CREATE TABLE unique_network_values (id int4 PRIMARY KEY, address inet UNIQUE)",
            )
            .unwrap();
        session
            .execute("INSERT INTO unique_network_values VALUES (1, '192.0.2.1')")
            .unwrap();
        assert_eq!(
            session
                .execute("INSERT INTO unique_network_values VALUES (2, '192.0.2.1/32')")
                .unwrap_err()
                .sqlstate(),
            "23505",
        );
        assert_eq!(
            session
                .execute(
                    "CREATE UNIQUE INDEX unique_network_hash
                     ON unique_network_values USING hash (address)",
                )
                .unwrap_err()
                .sqlstate(),
            "0A000",
        );
    }

    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT id FROM network_index_values
                 WHERE mac = '08-00-2b-01-02-04'::macaddr",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]],
    );
    assert!(plan_lines(
        &mut session,
        "SELECT address FROM network_index_values ORDER BY address",
    )
    .iter()
    .any(|line| line.contains("OrderedIndexScan idx_network_address")));
}

#[test]
fn analyze_uses_network_histograms_for_operator_selectivity() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE network_stats_values (id int4 PRIMARY KEY, address inet)")
        .unwrap();
    let mut values = (0..4)
        .map(|index| format!("({index}, '10.1.{index}.1/24')"))
        .collect::<Vec<_>>();
    values.extend((0..96).map(|index| format!("({}, '172.16.{index}.1/24')", index + 4)));
    session
        .execute(&format!(
            "INSERT INTO network_stats_values VALUES {}",
            values.join(",")
        ))
        .unwrap();
    for predicate in [
        "address <<= '10.1.0.0/16'::cidr",
        "address && '10.1.0.0/16'::cidr",
    ] {
        let lines = plan_lines(
            &mut session,
            &format!("SELECT id FROM network_stats_values WHERE {predicate}"),
        );
        assert!(
            lines.iter().any(|line| line == "EstimatedRows 1"),
            "{lines:?}"
        );
    }
    session.execute("ANALYZE network_stats_values").unwrap();

    for predicate in [
        "address <<= '10.1.0.0/16'::cidr",
        "'10.1.0.0/16'::cidr >>= address",
        "address && '10.1.0.0/16'::cidr",
    ] {
        let lines = plan_lines(
            &mut session,
            &format!("SELECT id FROM network_stats_values WHERE {predicate}"),
        );
        assert!(
            lines.iter().any(|line| line == "EstimatedRows 3"),
            "{lines:?}"
        );
    }
    assert_eq!(
        session
            .execute(
                "SELECT cardinality(histogram_bounds), pg_typeof(histogram_bounds)
                 FROM pg_stats
                 WHERE tablename = 'network_stats_values' AND attname = 'address'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(100),
            SqlValue::String("anyarray".to_string()),
        ]],
    );
}
