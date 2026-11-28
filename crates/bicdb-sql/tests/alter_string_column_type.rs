use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn changing_text_to_varchar_validates_length_and_rolls_back_catalog_changes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default().with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE strings (s text); INSERT INTO strings VALUES ('hello'), (NULL)")
            .unwrap();
        let error = sql
            .execute("ALTER TABLE strings ALTER COLUMN s TYPE varchar(4)")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22001");
        sql.execute("BEGIN; ALTER TABLE strings ALTER COLUMN s TYPE varchar(5)")
            .unwrap();
        assert_eq!(
            sql.execute("SELECT s, pg_typeof(s)::text FROM strings WHERE s IS NOT NULL")
                .unwrap()
                .rows,
            [vec![
                SqlValue::String("hello".into()),
                SqlValue::String("character varying".into()),
            ]],
        );
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("SELECT s, pg_typeof(s)::text FROM strings WHERE s IS NOT NULL")
                .unwrap()
                .rows,
            [vec![
                SqlValue::String("hello".into()),
                SqlValue::String("text".into()),
            ]],
        );
    }
}

#[test]
fn widening_character_columns_pads_values_to_the_new_width() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE padded_values (s char(2)); INSERT INTO padded_values VALUES ('a')")
        .unwrap();
    sql.execute("ALTER TABLE padded_values ALTER COLUMN s TYPE char(4)")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT octet_length(s), s::text FROM padded_values")
            .unwrap()
            .rows,
        [vec![SqlValue::Int(4), SqlValue::String("a".into())]],
    );
}
