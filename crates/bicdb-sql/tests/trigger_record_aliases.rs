use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_trigger_record_aliases_are_live_across_nested_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE alias_rows (id int PRIMARY KEY, quantity int)")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION alias_rows_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            edited ALIAS FOR NEW;
            previous ALIAS FOR OLD;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                edited.quantity := previous.quantity + edited.quantity;
            END IF;
            DECLARE
                edited int := 999;
                another ALIAS FOR NEW;
            BEGIN
                another.quantity := another.quantity + 2;
            END;
            NEW.quantity := edited.quantity + 3;
            RETURN edited;
        END;
        $$",
    )
    .unwrap();
    sql.execute(
        "CREATE TRIGGER alias_rows_before BEFORE INSERT OR UPDATE ON alias_rows
         FOR EACH ROW EXECUTE FUNCTION alias_rows_trigger()",
    )
    .unwrap();
    for (statement, expected) in [
        (
            "INSERT INTO alias_rows VALUES (1, 10) RETURNING quantity",
            15,
        ),
        (
            "UPDATE alias_rows SET quantity = 20 WHERE id = 1 RETURNING quantity",
            40,
        ),
    ] {
        assert_eq!(
            sql.execute(statement).unwrap().rows,
            [vec![SqlValue::Int(expected)]]
        );
    }
}
