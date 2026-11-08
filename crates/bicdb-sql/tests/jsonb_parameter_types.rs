use bicdb_core::BicDb;
use bicdb_sql::{infer_parameter_types, SqlSession};

#[test]
fn jsonb_existence_parameters_follow_the_operator_signature() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE documents(id TEXT PRIMARY KEY, data JSONB)")
        .unwrap();
    for (query, types) in [
        ("SELECT data ? $1 FROM documents", vec!["text"]),
        ("SELECT data ?& $1 FROM documents", vec!["text[]"]),
        ("SELECT data ?| $1 FROM documents", vec!["text[]"]),
        ("SELECT $1 ? $2", vec!["jsonb", "text"]),
        ("SELECT $1 ?& $2", vec!["jsonb", "text[]"]),
        ("SELECT $1 ?| $2", vec!["jsonb", "text[]"]),
        (
            "SELECT id FROM documents WHERE EXISTS (SELECT 1 FROM documents c WHERE c.id=documents.id AND c.data ? $1)",
            vec!["text"],
        ),
        (
            "SELECT id FROM documents WHERE data ? $1 AND id=$2 AND data ?& $3",
            vec!["text", "text", "text[]"],
        ),
        ("SELECT data ? $1::text FROM documents", vec!["text"]),
        ("SELECT data ?| $1::text[] FROM documents", vec!["text[]"]),
    ] {
        assert_eq!(
            infer_parameter_types(&db, query).unwrap(),
            types.into_iter().map(|value| Some(value.to_owned())).collect::<Vec<_>>(),
            "{query}"
        );
    }
}
