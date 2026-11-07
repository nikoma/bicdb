//! A predicate on a CAST of an indexed column is not a predicate on the
//! column: `CAST(name AS VARCHAR(3)) = 'AD_'` matches 'AD_Table', and
//! `CAST(amount AS NUMERIC(5,0)) = 2` matches 1.50 and 2.40. An index probe of
//! the uncast key finds neither, so no index or primary-key lookup may answer
//! such a predicate; the rows must be PostgreSQL's.
use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    if profile == "postgres" {
        return session.execute(sql);
    }
    let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
    session.execute(&sql)
}

/// The `tag` of every row, sorted, or the error text.
fn tags(
    session: &mut SqlSession<'_>,
    profile: &str,
    sql: &str,
) -> std::result::Result<Vec<String>, String> {
    let mut tags = run(session, profile, sql)
        .map_err(|error| error.to_string())?
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>();
    tags.sort();
    Ok(tags)
}

/// (predicate, PostgreSQL's tags, whether the Oracle profile must match them
/// exactly). Oracle's narrowing VARCHAR cast is checked against the
/// unindexed result of the same profile instead.
fn predicates() -> Vec<(&'static str, Vec<&'static str>, bool)> {
    let lossy = vec!["r1", "r2", "r3"];
    vec![
        (
            "CAST(name_text AS VARCHAR(3)) = 'AD_'",
            lossy.clone(),
            false,
        ),
        (
            "'AD_' = CAST(name_text AS VARCHAR(3))",
            lossy.clone(),
            false,
        ),
        ("CAST(amount AS NUMERIC(5,0)) = 2", lossy.clone(), true),
        (
            "CAST(amount AS NUMERIC(5,0)) BETWEEN 2 AND 2",
            lossy.clone(),
            true,
        ),
        (
            "CAST(amount AS NUMERIC(5,0)) >= 2 AND CAST(amount AS NUMERIC(5,0)) <= 2",
            lossy.clone(),
            true,
        ),
        ("CAST(id AS NUMERIC(5,0)) = 2", lossy.clone(), true),
        ("CAST(id AS NUMERIC(5,0)) IN (2)", lossy.clone(), true),
        ("CAST(id AS NUMERIC(5,0)) IN (2, 7)", lossy, true),
        // Uncast controls: the indexes themselves answer correctly.
        ("name_text = 'AD_'", vec!["r3"], true),
        ("amount = 2", vec!["r2"], true),
        ("id = 2.4", vec!["r3"], true),
    ]
}

fn setup(session: &mut SqlSession<'_>, profile: &str) {
    let (number, text) = if profile == "postgres" {
        ("NUMERIC(10,2)", "VARCHAR(40)")
    } else {
        ("NUMBER(10,2)", "VARCHAR2(40)")
    };
    run(
        session,
        profile,
        &format!(
            "CREATE TABLE cast_probe (id {number} PRIMARY KEY, amount {number}, name_text {text}, tag {text})"
        ),
    )
    .unwrap();
    for (value, name, tag) in [
        ("1.50", "AD_Table", "r1"),
        ("2.00", "AD_X", "r2"),
        ("2.40", "AD_", "r3"),
        ("2.50", "ADX", "r4"),
        ("3.00", "Other", "r5"),
    ] {
        run(
            session,
            profile,
            &format!("INSERT INTO cast_probe VALUES ({value}, {value}, '{name}', '{tag}')"),
        )
        .unwrap();
    }
    for id in 100..500 {
        run(
            session,
            profile,
            &format!("INSERT INTO cast_probe VALUES ({id}, {id}, 'F{id}', 'f{id}')"),
        )
        .unwrap();
    }
}

fn create_indexes(session: &mut SqlSession<'_>, profile: &str) {
    for sql in [
        "CREATE INDEX cast_probe_name ON cast_probe (name_text)",
        "CREATE INDEX cast_probe_amount ON cast_probe (amount)",
    ] {
        run(session, profile, sql).unwrap();
    }
}

fn check(
    session: &mut SqlSession<'_>,
    profile: &str,
    mode: &str,
    unindexed: &[std::result::Result<Vec<String>, String>],
) {
    for ((predicate, expected, oracle_exact), unindexed) in predicates().into_iter().zip(unindexed)
    {
        let limited = if profile == "postgres" {
            " ORDER BY id LIMIT 10"
        } else {
            " ORDER BY id FETCH FIRST 10 ROWS ONLY"
        };
        for suffix in ["", " ORDER BY id", " ORDER BY id DESC", limited] {
            let sql = format!("SELECT tag FROM cast_probe WHERE {predicate}{suffix}");
            let actual = tags(session, profile, &sql);
            assert_eq!(
                &actual, unindexed,
                "{profile}/{mode}: {sql}: index changed the result"
            );
            if profile == "postgres" || oracle_exact {
                assert_eq!(
                    actual,
                    Ok(expected
                        .iter()
                        .map(|tag| tag.to_string())
                        .collect::<Vec<_>>()),
                    "{profile}/{mode}: {sql}"
                );
            }
        }
    }
}

#[test]
fn lossy_casts_of_indexed_columns_never_use_the_index() {
    for profile in ["postgres", "oracle-preview-v1"] {
        for mode in ["embedded", "server_paged"] {
            let root = tempfile::tempdir().unwrap();
            let storage = if mode == "embedded" {
                bicdb_core::StorageMode::EmbeddedMemory
            } else {
                bicdb_core::StorageMode::ServerPaged
            };
            let config = bicdb_core::DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(storage);
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            setup(&mut session, profile);
            // Without secondary indexes (the primary key still has its own).
            let unindexed: Vec<_> = predicates()
                .iter()
                .map(|(predicate, _, _)| {
                    tags(
                        &mut session,
                        profile,
                        &format!("SELECT tag FROM cast_probe WHERE {predicate}"),
                    )
                })
                .collect();
            create_indexes(&mut session, profile);
            if mode == "embedded" {
                check(&mut session, profile, mode, &unindexed);
                continue;
            }
            drop(session);
            db.close().unwrap();
            // Reopen: lazy server-paged tables.
            let mut db = BicDb::open_with_config(root.path(), config).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            check(&mut session, profile, mode, &unindexed);
        }
    }
}
