//! A store holding a partition tree more than one level deep, with a
//! PRIMARY KEY or UNIQUE constraint on its root, must reopen. Every
//! partition carries its root's constraint, enforced by the root's index;
//! the startup check used to accept that only from a direct parent, so a
//! two-level tree kept the whole database closed.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{ensure_primary_key_indexes, ensure_unique_constraint_indexes, SqlSession};

fn reopen(path: &std::path::Path, mode: &StorageMode) -> BicDb {
    let mut db = BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap();
    ensure_primary_key_indexes(&mut db).unwrap();
    ensure_unique_constraint_indexes(&mut db).unwrap();
    db
}

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

#[test]
fn nested_partition_trees_with_a_key_reopen_and_stay_enforced() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for depth in [2usize, 3] {
            for key in ["primary key (id)", "unique (id)"] {
                let dir = tempfile::tempdir().unwrap();
                {
                    let mut db = reopen(dir.path(), &mode);
                    run(
                        &mut db,
                        &format!("create table root (id int, {key}) partition by range (id)"),
                    );
                    let mut parent = "root".to_string();
                    for level in 1..depth {
                        let name = format!("level{level}");
                        run(
                            &mut db,
                            &format!(
                                "create table {name} partition of {parent} \
                                 for values from (1) to (100) partition by range (id)"
                            ),
                        );
                        parent = name;
                    }
                    run(
                        &mut db,
                        &format!(
                            "create table leaf partition of {parent} for values from (1) to (50)"
                        ),
                    );
                    run(
                        &mut db,
                        "create function tree_trigger() returns trigger language plpgsql \
                         as $$ begin return new; end $$",
                    );
                    run(
                        &mut db,
                        "create trigger tree_insert after insert on root \
                         for each row execute function tree_trigger()",
                    );
                    run(&mut db, "insert into root values (7)");
                    db.close().unwrap();
                }
                let label = format!("{mode:?} depth={depth} {key}");
                let mut db = reopen(dir.path(), &mode);
                let duplicate = SqlSession::new(&mut db)
                    .execute("insert into root values (7)")
                    .unwrap_err();
                assert_eq!(duplicate.sqlstate(), "23505", "{label}: {duplicate}");
                run(&mut db, "insert into root values (8)");
                // DROP TABLE takes the partitions' inherited triggers along.
                run(&mut db, "drop table root");
                db.close().unwrap();
                let mut db = reopen(dir.path(), &mode);
                run(&mut db, "create table root (id int)");
            }
        }
    }
}
