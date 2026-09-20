use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn session() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    (root, db)
}

#[test]
fn inet_operators_match_postgresql() {
    let (_root, mut db) = session();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT
                    '192.168.1.5/24'::inet << '192.168.0.0/16'::inet,
                    '192.168.1.5/24'::inet <<= '192.168.1.0/24'::inet,
                    '192.168.0.0/16'::inet >> '192.168.1.5/24'::inet,
                    '192.168.1.0/24'::inet >>= '192.168.1.5/24'::inet,
                    '192.168.1.5/24'::inet && '192.168.1.128/25'::inet,
                    '10/8'::cidr = '10.0.0.0/8'::inet,
                    '192.168.1.1/24'::inet < '192.168.1.0/25'::inet,
                    ~ '192.0.2.1/24'::inet,
                    '192.0.2.1/24'::inet & '255.255.255.0'::inet,
                    '192.0.2.1/24'::inet | '0.0.0.15'::inet,
                    '192.0.2.1'::inet + 5,
                    5 + '192.0.2.1'::inet,
                    '192.0.2.10'::inet - 5,
                    '192.0.2.10'::inet - '192.0.2.1'::inet",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::String("63.255.253.254/24".to_string()),
            SqlValue::String("192.0.2.0/32".to_string()),
            SqlValue::String("192.0.2.15/32".to_string()),
            SqlValue::String("192.0.2.6/32".to_string()),
            SqlValue::String("192.0.2.6/32".to_string()),
            SqlValue::String("192.0.2.5/32".to_string()),
            SqlValue::Int(9),
        ]],
    );
}

#[test]
fn network_and_mac_functions_match_postgresql() {
    let (_root, mut db) = session();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT
                    abbrev('10.1.0.0/16'::cidr),
                    abbrev('2001:db8:1::/48'::cidr),
                    host('192.168.1.5/24'::inet),
                    text('::1'::inet),
                    family('::1'::inet),
                    masklen('10/8'::cidr),
                    network('192.168.1.5/24'::inet),
                    broadcast('192.168.1.5/24'::inet),
                    netmask('192.168.1.5/24'::inet),
                    hostmask('192.168.1.5/24'::inet),
                    set_masklen('192.168.1.5/24'::inet, 16),
                    set_masklen('192.168.1.5/24'::inet, '12'),
                    set_masklen('192.168.1.0/24'::cidr, 16),
                    inet_same_family('10.0.0.1'::inet, '::1'::inet),
                    inet_merge('192.168.1.5/24'::inet, '192.168.2.7/24'::inet),
                    trunc('08:00:2b:01:02:03'::macaddr),
                    trunc('08:00:2b:01:02:03:04:05'::macaddr8),
                    macaddr8_set7bit('00:00:00:00:00:00:00:00'::macaddr8)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("10.1/16".to_string()),
            SqlValue::String("2001:db8:1/48".to_string()),
            SqlValue::String("192.168.1.5".to_string()),
            SqlValue::String("::1/128".to_string()),
            SqlValue::Int(6),
            SqlValue::Int(8),
            SqlValue::String("192.168.1.0/24".to_string()),
            SqlValue::String("192.168.1.255/24".to_string()),
            SqlValue::String("255.255.255.0/32".to_string()),
            SqlValue::String("0.0.0.255/32".to_string()),
            SqlValue::String("192.168.1.5/16".to_string()),
            SqlValue::String("192.168.1.5/12".to_string()),
            SqlValue::String("192.168.0.0/16".to_string()),
            SqlValue::Bool(false),
            SqlValue::String("192.168.0.0/22".to_string()),
            SqlValue::String("08:00:2b:00:00:00".to_string()),
            SqlValue::String("08:00:2b:00:00:00:00:00".to_string()),
            SqlValue::String("02:00:00:00:00:00:00:00".to_string()),
        ]],
    );

    assert_eq!(
        session
            .execute(
                "SELECT
                    ~ '08:00:2b:01:02:03'::macaddr,
                    '08:00:2b:ff:00:ff'::macaddr & 'ff:ff:ff:00:ff:00'::macaddr,
                    '08:00:2b:00:00:00'::macaddr | '00:00:00:01:02:03'::macaddr,
                    ~ '08:00:2b:01:02:03:04:05'::macaddr8,
                    '08:00:2b:01:02:03'::macaddr < '08:00:2b:01:02:04'::macaddr,
                    '08:00:2b:01:02:03:04:05'::macaddr8 =
                        '08:00:2b:01:02:03:04:05'::macaddr8",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("f7:ff:d4:fe:fd:fc".to_string()),
            SqlValue::String("08:00:2b:00:00:00".to_string()),
            SqlValue::String("08:00:2b:01:02:03".to_string()),
            SqlValue::String("f7:ff:d4:fe:fd:fc:fb:fa".to_string()),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]],
    );
}

#[test]
fn network_ordering_predicates_and_aggregates_are_typed() {
    let (_root, mut db) = session();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE network_values (id int4 PRIMARY KEY, address inet NOT NULL);
             INSERT INTO network_values VALUES
                (1, '192.168.1.1/24'),
                (2, '192.168.1.0/25'),
                (3, '192.168.0.255/32'),
                (4, '192.168.1.200/24');",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT address FROM network_values ORDER BY address")
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
            .execute("SELECT min(address), max(address) FROM network_values")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("192.168.0.255/32".to_string()),
            SqlValue::String("192.168.1.0/25".to_string()),
        ]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT address FROM network_values
                 WHERE address <<= '192.168.1.0/24'::cidr ORDER BY address",
            )
            .unwrap()
            .rows
            .len(),
        3,
    );
}

#[test]
fn network_operations_fail_with_postgresql_sqlstates() {
    let (_root, mut db) = session();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT '255.255.255.255'::inet + 1",
        "SELECT '::'::inet - '8000::'::inet",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22003");
    }
    for sql in [
        "SELECT '10.0.0.1'::inet & '::1'::inet",
        "SELECT inet_merge('10.0.0.1'::inet, '::1'::inet)",
        "SELECT set_masklen('10.0.0.1'::inet, 33)",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22023");
    }
    for sql in [
        "SELECT family('10.0.0.1'::inet, '10.0.0.2'::inet)",
        "SELECT set_masklen('10.0.0.1'::inet, 8::int8)",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42883");
    }
}
