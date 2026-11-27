use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_positional_parameter_aliases_share_assignment_storage() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE FUNCTION alias_scalar(input_value int) RETURNS int[] LANGUAGE plpgsql AS $$
         DECLARE aliased ALIAS FOR $1;
         BEGIN
             aliased := aliased + 1;
             input_value := input_value + 2;
             DECLARE input_value int := 99;
             BEGIN
                 aliased := aliased + 5;
             END;
             RETURN ARRAY[$1, input_value, aliased];
         END $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION alias_array(input_value int[]) RETURNS int[] LANGUAGE plpgsql AS $$
         DECLARE aliased ALIAS FOR $1;
         BEGIN
             aliased[1] := 9;
             input_value[2] := 8;
             RETURN $1;
         END $$",
    )
    .unwrap();
    assert_eq!(
        sql.execute("SELECT alias_scalar(4)::text, alias_array(ARRAY[1,2])::text")
            .unwrap()
            .rows,
        [vec![
            SqlValue::String("{12,12,12}".into()),
            SqlValue::String("{9,8}".into()),
        ]],
    );
}
