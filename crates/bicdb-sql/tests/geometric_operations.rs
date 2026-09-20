use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn string(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

#[test]
fn postgres_geometric_constructors_accessors_and_transforms_match() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT point('[(0,0),(4,2)]'::lseg),
                        point('(0,0),(4,2)'::box),
                        box(point(0,0), point(4,2)),
                        circle(point(1,2), 3),
                        lseg(point(0,0), point(4,2)),
                        line(point(0,0), point(4,2)),
                        path('((0,0),(1,0),(1,1))'::polygon),
                        polygon('(0,0),(4,2)'::box)",
            )
            .unwrap()
            .rows,
        vec![vec![
            string("(2,1)"),
            string("(2,1)"),
            string("(4,2),(0,0)"),
            string("<(1,2),3>"),
            string("[(0,0),(4,2)]"),
            string("{0.5,-1,0}"),
            string("((0,0),(1,0),(1,1))"),
            string("((0,0),(0,2),(4,2),(4,0))"),
        ]],
    );

    assert_eq!(
        session
            .execute(
                "SELECT center('(0,0),(4,2)'::box),
                        diagonal('(0,0),(4,2)'::box),
                        bound_box('(0,0),(1,1)'::box, '(2,2),(3,3)'::box),
                        diameter('<(0,0),2>'::circle),
                        radius('<(0,0),2>'::circle),
                        height('(0,0),(4,2)'::box),
                        width('(0,0),(4,2)'::box),
                        isclosed('((0,0),(1,1))'::path),
                        isopen('[(0,0),(1,1)]'::path),
                        npoints('((0,0),(1,0),(1,1))'::polygon),
                        slope(point(0,0), point(2,4)),
                        ishorizontal(point(0,1), point(2,1)),
                        isvertical(point(1,0), point(1,2)),
                        length('[(0,0),(3,4)]'::lseg),
                        area('[(0,0),(4,0),(4,3)]'::path),
                        length('abc'::text)",
            )
            .unwrap()
            .rows,
        vec![vec![
            string("(2,1)"),
            string("[(4,2),(0,0)]"),
            string("(3,3),(0,0)"),
            SqlValue::Float(4.0),
            SqlValue::Float(2.0),
            SqlValue::Float(2.0),
            SqlValue::Float(4.0),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Int(3),
            SqlValue::Float(2.0),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Float(5.0),
            SqlValue::Null,
            SqlValue::Int(3),
        ]],
    );

    assert_eq!(
        session
            .execute(
                "SELECT point(1,2) + point(3,4),
                        point(1,2) - point(3,4),
                        point(1,2) * point(3,4),
                        point(1,2) / point(3,4),
                        '[(0,0),(1,0)]'::path + '[(2,0),(3,0)]'::path,
                        '(0,0),(2,2)'::box # '(1,1),(3,3)'::box",
            )
            .unwrap()
            .rows,
        vec![vec![
            string("(4,6)"),
            string("(-2,-2)"),
            string("(-5,10)"),
            string("(0.44,0.08)"),
            string("[(0,0),(1,0),(2,0),(3,0)]"),
            string("(2,2),(1,1)"),
        ]],
    );
}

#[test]
fn postgres_geometric_operators_match_for_constants_and_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT @-@ '[(0,0),(3,4)]'::lseg,
                        @-@ '[(0,0),(3,4)]'::path,
                        @@ '(0,0),(4,2)'::box,
                        # '((0,0),(1,0),(1,1))'::path,
                        ?- '[(0,0),(3,0)]'::lseg,
                        ?| '[(0,0),(0,3)]'::lseg",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Float(5.0),
            SqlValue::Float(5.0),
            string("(2,1)"),
            SqlValue::Int(3),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]],
    );

    assert_eq!(
        session
            .execute(
                "SELECT '[(0,0),(3,4)]'::lseg = '[(3,4),(0,0)]'::lseg,
                        '[(0,0),(3,4)]'::lseg < '[(0,0),(6,8)]'::lseg,
                        '(0,0),(2,2)'::box = '(0,0),(1,4)'::box,
                        '(0,0),(2,2)'::box ~= '(0,0),(1,4)'::box,
                        point(0,0) <> point(1,1),
                        '{1,-1,0}'::line = '{2,-2,0}'::line,
                        '((0,0),(1,0),(1,1))'::polygon ~=
                            '((1,0),(1,1),(0,0))'::polygon",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]],
    );
    for sql in [
        "SELECT '(0,0),(2,2)'::box <> '(0,0),(1,4)'::box",
        "SELECT '{1,-1,0}'::line < '{1,1,-2}'::line",
        "SELECT point(0,0) = point(0,0)",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42883");
    }

    assert_eq!(
        session
            .execute(
                "SELECT point(1,0) <@ '[(0,0),(2,0)]'::lseg,
                        point(1,1) <@ '(0,0),(2,2)'::box,
                        '(0,0),(1,1)'::box <@ '(0,0),(2,2)'::box,
                        point(1,1) <@ '((0,0),(2,0),(2,2),(0,2))'::polygon,
                        point(1,1) <@ '<(0,0),2>'::circle,
                        '<(0,0),1>'::circle <@ '<(0,0),2>'::circle,
                        '[(0,0),(2,2)]'::lseg ?# '[(0,2),(2,0)]'::lseg,
                        '(0,0),(2,2)'::box && '(1,1),(3,3)'::box",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true); 8]],
    );

    assert_eq!(
        session
            .execute(
                "SELECT point(0,0) <-> point(3,4),
                        point(1,3) <-> '[(0,0),(2,0)]'::lseg,
                        point(5,1) <-> '(0,0),(2,2)'::box,
                        point(5,0) <-> '<(0,0),2>'::circle,
                        '(0,0),(2,2)'::box <-> '(2,0),(4,2)'::box,
                        '<(0,0),1>'::circle <-> '<(5,0),2>'::circle,
                        '{1,-1,0}'::line # '{1,1,-2}'::line,
                        point(2,3) ## '[(0,0),(4,0)]'::lseg,
                        '[(1,1),(2,2)]'::lseg ## '(0,0),(3,3)'::box,
                        '[(1,1),(4,4)]'::lseg ## '(0,0),(3,3)'::box,
                        '[(0,0),(2,0)]'::lseg ## '[(3,1),(3,3)]'::lseg,
                        '[(3,1),(3,3)]'::lseg ## '[(0,0),(2,0)]'::lseg,
                        '{0,1,0}'::line ## '[(0,2),(3,2)]'::lseg",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Float(5.0),
            SqlValue::Float(3.0),
            SqlValue::Float(3.0),
            SqlValue::Float(3.0),
            SqlValue::Float(2.0),
            SqlValue::Float(2.0),
            string("(1,1)"),
            string("(2,0)"),
            string("(1.5,1.5)"),
            string("(1.5,1.5)"),
            string("(3,1)"),
            string("(2,0)"),
            SqlValue::Null,
        ]],
    );

    session
        .execute(
            "CREATE TABLE geometric_operator_rows (
                id text PRIMARY KEY,
                segment lseg NOT NULL,
                region box NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO geometric_operator_rows VALUES
                ('hit', '[(0,0),(2,2)]', '(0,0),(2,2)'),
                ('miss', '[(5,5),(6,6)]', '(5,5),(6,6)')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id FROM geometric_operator_rows
                 WHERE segment ?# '[(0,2),(2,0)]'::lseg
                   AND region @> point(1,1)
                 ORDER BY id",
            )
            .unwrap()
            .rows,
        vec![vec![string("hit")]],
    );
}
