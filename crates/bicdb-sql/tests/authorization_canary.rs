//! Adversarial authorization invariant (P0-C regression). A protected table
//! carries a unique canary. Under an UNPRIVILEGED identity, no relational
//! access shape BicDB supports may reach it — the canary must never appear in
//! a returned row, and the shape must be denied (never merely "no rows"). The
//! same shape must SUCCEED for a granted role, and be DENIED AGAIN after
//! REVOKE. This is the "optimization must never change authorization
//! semantics" guard: it forced out the indexed-equi-join fast-path bypass and
//! is meant to force out the next one (spatial join, vector NN, ...).
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn fresh() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    {
        let mut o = SqlSession::new(&mut db);
        for stmt in [
            "CREATE ROLE mallory LOGIN NOSUPERUSER",
            "CREATE ROLE bob LOGIN NOSUPERUSER",
            "CREATE TABLE secrets (id INT PRIMARY KEY, n INT, v TEXT, loc point, body TEXT)",
            "CREATE INDEX secrets_n ON secrets(n)",
            "CREATE INDEX secrets_loc ON secrets USING gist (loc)",
            "CREATE INDEX secrets_fts ON secrets USING GIN (to_tsvector('english', COALESCE(body,'')))",
            "INSERT INTO secrets VALUES \
             (1,5,'CANARY-XYZ',point(1,1),'canary poison'),\
             (2,9,'CANARY-QRS',point(2,2),'more canary')",
            "CREATE TABLE pub (id INT PRIMARY KEY, x TEXT)",
            "GRANT ALL ON pub TO mallory",
            "GRANT ALL ON pub TO bob",
        ] {
            let _ = o.execute(stmt);
        }
    }
    (directory, db)
}

/// Every relational access shape that could touch `secrets`. A read shape
/// returns rows; a mutation shape (insert-select) is a read of `secrets` too.
const SHAPES: &[&str] = &[
    "SELECT * FROM secrets",
    "SELECT v FROM secrets WHERE id=1",
    "SELECT v FROM secrets WHERE n=5",
    "SELECT count(*) FROM secrets",
    "SELECT min(v), max(id) FROM secrets",
    "SELECT sum(n) FROM secrets",
    "SELECT v, count(*) FROM secrets GROUP BY v",
    "SELECT v FROM secrets ORDER BY id LIMIT 1",
    "SELECT DISTINCT v FROM secrets",
    "SELECT EXISTS(SELECT 1 FROM secrets) AS e",
    "SELECT v FROM secrets UNION SELECT 'x'",
    "SELECT s.v FROM secrets s JOIN generate_series(1,2) g ON s.id=g",
    "SELECT s.v FROM generate_series(1,2) g JOIN secrets s ON s.id=g",
    "SELECT s.v FROM generate_series(1,2) g JOIN secrets s ON s.id>0",
    "SELECT s.v FROM (VALUES (1),(2)) g(n) JOIN secrets s ON s.id=g.n",
    "SELECT v FROM secrets WHERE loc << point(999,0)",
    "SELECT v FROM secrets ORDER BY loc <-> point(0,0) LIMIT 1",
    "SELECT s.v FROM (SELECT point(1,1) AS p) g JOIN secrets s ON (s.loc <-> g.p) < 100",
    "SELECT v FROM secrets WHERE to_tsvector('english', COALESCE(body,'')) @@ to_tsquery('english','canary')",
    "SELECT v, row_number() OVER () FROM secrets",
    "WITH x AS (SELECT * FROM secrets) SELECT * FROM x",
    "SELECT (SELECT v FROM secrets WHERE id=1) AS v",
    "INSERT INTO pub SELECT id, v FROM secrets",
    "SELECT max(id) FROM secrets",
    "SELECT min(n) FROM secrets",
];

fn denied_or_unsupported(session: &mut SqlSession<'_>, sql: &str) {
    match session.execute(sql) {
        Ok(result) => {
            let rendered = format!("{:?}", result.rows);
            assert!(
                !rendered.contains("CANARY"),
                "CONFIDENTIALITY BYPASS — canary leaked via `{sql}`: {rendered}"
            );
            // A shape that runs without error but returns no canary is still a
            // bypass of the table gate (it read the table and filtered): the
            // authorization model denies the *access*, not just the rows.
            panic!("`{sql}` was ALLOWED for an unprivileged role; it must be denied");
        }
        Err(error) => {
            let message = format!("{error}");
            // Denial is correct. An "unsupported SQL" shape is acceptable (the
            // path does not exist), but it must not be a *silent success*.
            assert!(
                message.contains("permission denied")
                    || message.contains("authorization")
                    || message.contains("unsupported")
                    || message.contains("not supported"),
                "`{sql}` failed with a non-authorization error: {message}"
            );
        }
    }
}

#[test]
fn unprivileged_identity_cannot_reach_the_canary_through_any_shape() {
    let (_d, mut db) = fresh();
    let mut m = SqlSession::new_unprivileged(&mut db, "mallory");
    for sql in SHAPES {
        denied_or_unsupported(&mut m, sql);
    }
}

#[test]
fn granted_then_revoked_flips_access_for_read_shapes() {
    let (_d, mut db) = fresh();
    // A representative read shape from each family that returned rows.
    let read_shapes: &[&str] = &[
        "SELECT count(*) FROM secrets",
        "SELECT s.v FROM generate_series(1,2) g JOIN secrets s ON s.id=g",
        "SELECT max(id) FROM secrets",
        "SELECT v FROM secrets WHERE n=5",
        "WITH x AS (SELECT * FROM secrets) SELECT * FROM x",
    ];
    {
        let mut o = SqlSession::new(&mut db);
        o.execute("GRANT SELECT ON secrets TO bob").unwrap();
    }
    {
        let mut b = SqlSession::new_unprivileged(&mut db, "bob");
        for sql in read_shapes {
            b.execute(sql)
                .unwrap_or_else(|e| panic!("granted bob should run `{sql}`: {e}"));
        }
    }
    {
        let mut o = SqlSession::new(&mut db);
        o.execute("REVOKE SELECT ON secrets FROM bob").unwrap();
    }
    {
        let mut b = SqlSession::new_unprivileged(&mut db, "bob");
        for sql in read_shapes {
            denied_or_unsupported(&mut b, sql);
        }
    }
}
