use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn unknown_array_concat_literals_resolve_to_array_and_typed_values_resolve_to_elements() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (query, expected) in [
        ("SELECT ('{p}' || ARRAY['a','b'])::text", "{p,a,b}"),
        ("SELECT (ARRAY['a','b'] || '{p}')::text", "{a,b,p}"),
        ("SELECT ('p'::text || ARRAY['a','b'])::text", "{p,a,b}"),
        ("SELECT (ARRAY['a','b'] || 'p'::text)::text", "{a,b,p}"),
        ("SELECT (ARRAY[1,2] || NULL)::text", "{1,2}"),
        ("SELECT (ARRAY[1,2] || NULL::int)::text", "{1,2,NULL}"),
    ] {
        assert_eq!(
            session.execute(query).unwrap().rows,
            vec![vec![SqlValue::String(expected.to_string())]],
            "{query}",
        );
    }

    for (query, sqlstate) in [
        ("SELECT 'p' || ARRAY['a','b']", "22P02"),
        ("SELECT ARRAY['a','b'] || 'p'", "22P02"),
        ("SELECT 'p'::text || ARRAY[1,2]", "42883"),
    ] {
        assert_eq!(
            session.execute(query).unwrap_err().sqlstate(),
            sqlstate,
            "{query}"
        );
    }
}
