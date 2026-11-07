//! PgJDBC key and index metadata run as the driver sends it.
//! `DatabaseMetaData.getPrimaryKeys` and `getIndexInfo` expand
//! `pg_index.indkey` with `information_schema._pg_expandarray` in the SELECT
//! list (twice, in lockstep, for primary keys), select its record fields with
//! `(f).x` / `(f).n`, and read `pg_index.indoption` by 0-based int2vector
//! subscript. Each failed: the expansion was rejected as an unknown scalar
//! function, and int2vector subscripts counted from 1.
use super::*;

/// PgJDBC 42.7.8 `getPrimaryKeys` (server >= 11); `$1` schema, `$2` table.
const DRIVER_PRIMARY_KEYS_SQL: &str = "SELECT        result.TABLE_CAT AS \"TABLE_CAT\",        result.TABLE_SCHEM AS \"TABLE_SCHEM\",        result.TABLE_NAME AS \"TABLE_NAME\",        result.COLUMN_NAME AS \"COLUMN_NAME\",        result.KEY_SEQ AS \"KEY_SEQ\",        result.PK_NAME AS \"PK_NAME\"FROM      (SELECT current_database() AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM, i.indnkeyatts as KEY_COUNT FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = $1 AND ct.relname = $2 AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x AND result.KEY_SEQ <= KEY_COUNT  ORDER BY result.table_name, result.pk_name, result.key_seq";

/// PgJDBC 42.7.8 `getIndexInfo(.., unique = false, ..)` (server >= 9.6);
/// `$1` schema, `$2` table.
const DRIVER_INDEX_INFO_SQL: &str = "SELECT     tmp.TABLE_CAT AS \"TABLE_CAT\",     tmp.TABLE_SCHEM AS \"TABLE_SCHEM\",     tmp.TABLE_NAME AS \"TABLE_NAME\",     tmp.NON_UNIQUE AS \"NON_UNIQUE\",     tmp.INDEX_QUALIFIER AS \"INDEX_QUALIFIER\",     tmp.INDEX_NAME AS \"INDEX_NAME\",     tmp.TYPE AS \"TYPE\",     tmp.ORDINAL_POSITION AS \"ORDINAL_POSITION\",     trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS \"COLUMN_NAME\",   CASE tmp.AM_NAME     WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint       WHEN 1 THEN 'D'       ELSE 'A'     END     ELSE NULL   END AS \"ASC_OR_DESC\",     tmp.CARDINALITY AS \"CARDINALITY\",     tmp.PAGES AS \"PAGES\",     tmp.FILTER_CONDITION AS \"FILTER_CONDITION\",     tmp.REMARKS AS \"REMARKS\"FROM (SELECT current_database() AS TABLE_CAT,  n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE,   NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME,   CASE i.indisclustered     WHEN true THEN 1    ELSE CASE am.amname       WHEN 'hash' THEN 2      ELSE 3    END   END AS TYPE,   (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION,   ci.reltuples AS CARDINALITY,   ci.relpages AS PAGES,   pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION,   ci.oid AS CI_OID,   i.indoption AS I_INDOPTION,   am.amname AS AM_NAME,   d.description AS REMARKS FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)   LEFT JOIN pg_catalog.pg_description d ON (ci.oid = d.objoid) WHERE true  AND n.nspname = $1 AND ct.relname = $2) AS tmp ORDER BY \"NON_UNIQUE\", \"TYPE\", \"INDEX_NAME\", \"ORDINAL_POSITION\" ";

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn driver_sql(sql: &str, schema: &str, table: &str) -> String {
    sql.replace("$1", &format!("'{schema}'"))
        .replace("$2", &format!("'{table}'"))
}

fn strings<const N: usize>(rows: &[[&str; N]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

/// A primary key declared out of column order, a two-column unique index
/// and a descending index.
fn indexed_schema() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE md_t (a INT NOT NULL, b TEXT, c INT NOT NULL, d INT, PRIMARY KEY (c, a))",
        "CREATE UNIQUE INDEX md_t_b_c_uq ON md_t (b, c)",
        "CREATE INDEX md_t_d_desc ON md_t (d DESC)",
        "CREATE TABLE md_other (id INT PRIMARY KEY, v INT)",
    ] {
        session.execute(sql).unwrap();
    }
    drop(session);
    (root, db)
}

#[test]
fn pg_expandarray_expands_int2vector_keys_in_lockstep() {
    let (_root, mut db) = indexed_schema();
    let mut session = SqlSession::new(&mut db);
    // Two set-returning calls over the same key vector expand side by side
    // (PostgreSQL 10+), not as a product; `n` counts from 1, `x` is the
    // element. int2vector subscripts start at 0.
    assert_eq!(
        rows(
            &mut session,
            "SELECT (information_schema._pg_expandarray(i.indkey)).n AS n, \
             information_schema._pg_expandarray(i.indkey) AS k, i.indkey[0], i.indkey[1] \
             FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid \
             WHERE c.relname = 'md_t_pkey'",
        ),
        strings(&[["1", "(3,1)", "3", "1"], ["2", "(1,2)", "3", "1"]])
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT k.n, (k.f).x FROM (SELECT (information_schema._pg_expandarray(i.indkey)).n AS n, \
             information_schema._pg_expandarray(i.indkey) AS f \
             FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid \
             WHERE c.relname = 'md_t_b_c_uq') k ORDER BY k.n",
        ),
        strings(&[["1", "2"], ["2", "3"]])
    );
}

#[test]
fn driver_primary_keys_list_key_columns_in_key_order() {
    let (_root, mut db) = indexed_schema();
    let mut session = SqlSession::new(&mut db);
    fn keys(session: &mut SqlSession<'_>, table: &str) -> Vec<Vec<String>> {
        rows(
            session,
            &driver_sql(DRIVER_PRIMARY_KEYS_SQL, "public", table),
        )
        .into_iter()
        // TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME
        .map(|row| row[1..6].to_vec())
        .collect()
    }
    assert_eq!(
        keys(&mut session, "md_t"),
        strings(&[
            ["public", "md_t", "c", "1", "md_t_pkey"],
            ["public", "md_t", "a", "2", "md_t_pkey"],
        ])
    );
    assert_eq!(
        keys(&mut session, "md_other"),
        strings(&[["public", "md_other", "id", "1", "md_other_pkey"]])
    );
    assert!(keys(&mut session, "missing").is_empty());
}

#[test]
fn driver_index_info_reports_each_index_column_and_direction() {
    let (_root, mut db) = indexed_schema();
    let mut session = SqlSession::new(&mut db);
    let indexes = rows(
        &mut session,
        &driver_sql(DRIVER_INDEX_INFO_SQL, "public", "md_t"),
    )
    .into_iter()
    // TABLE_NAME, NON_UNIQUE, INDEX_NAME, TYPE, ORDINAL_POSITION, COLUMN_NAME,
    // ASC_OR_DESC, FILTER_CONDITION
    .map(|row| [2, 3, 5, 6, 7, 8, 9, 12].map(|at| row[at].clone()).to_vec())
    .collect::<Vec<_>>();
    assert_eq!(
        indexes,
        strings(&[
            ["md_t", "false", "md_t_b_c_uq", "3", "1", "b", "A", ""],
            ["md_t", "false", "md_t_b_c_uq", "3", "2", "c", "A", ""],
            ["md_t", "false", "md_t_pkey", "3", "1", "c", "A", ""],
            ["md_t", "false", "md_t_pkey", "3", "2", "a", "A", ""],
            ["md_t", "true", "md_t_d_desc", "3", "1", "d", "D", ""],
        ])
    );
}
