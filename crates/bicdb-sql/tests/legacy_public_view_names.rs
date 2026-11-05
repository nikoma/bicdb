use bicdb_core::{BicDb, Record};
use bicdb_sql::SqlSession;

#[test]
fn legacy_quoted_public_view_keys_keep_reads_oids_replacement_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE legacy_keys(row_key INTEGER PRIMARY KEY,value INTEGER)")
            .unwrap();
        session
            .execute("INSERT INTO legacy_keys VALUES(1,10)")
            .unwrap();
        session
            .execute(
                "CREATE VIEW \"Legacy.View.With.Dot\" AS SELECT row_key,value FROM legacy_keys",
            )
            .unwrap();
        session
            .execute(
                "CREATE VIEW \"Legacy View With Space\" AS SELECT row_key,value FROM legacy_keys",
            )
            .unwrap();
    }
    // Before public storage-name encoding, these view records used their
    // quoted logical spellings as keys. Preserve every other stored field.
    let collection = "__bicdb_pg_views";
    for record in db.scan_collection(collection).unwrap() {
        let physical = record.id.clone();
        let logical = if record.metadata["query_sql"]
            .as_str()
            .unwrap()
            .contains("legacy_keys")
        {
            if physical.ends_with(&hex::encode("Legacy.View.With.Dot")) {
                "Legacy.View.With.Dot"
            } else {
                "Legacy View With Space"
            }
        } else {
            unreachable!()
        };
        let mut metadata = record.metadata;
        metadata["name"] = logical.into();
        db.delete(collection, &physical).unwrap();
        db.insert(collection, Record::new(logical).with_metadata(metadata))
            .unwrap();
    }
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE ROLE legacy_reader LOGIN NOSUPERUSER")
            .unwrap();
        owner
            .execute("GRANT SELECT ON \"Legacy.View.With.Dot\" TO legacy_reader")
            .unwrap();
    }
    {
        let mut reader = SqlSession::new_unprivileged(&mut db, "legacy_reader");
        assert_eq!(
            reader
                .execute("SELECT value FROM \"Legacy.View.With.Dot\"")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "10"
        );
        for sql in [
            "SELECT * FROM \"Legacy View With Space\"",
            "SELECT * FROM legacy_keys",
        ] {
            assert_eq!(reader.execute(sql).unwrap_err().sqlstate(), "42501");
        }
    }
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("REVOKE SELECT ON \"Legacy.View.With.Dot\" FROM legacy_reader")
            .unwrap();
    }
    {
        let mut reader = SqlSession::new_unprivileged(&mut db, "legacy_reader");
        assert_eq!(
            reader
                .execute("SELECT * FROM \"Legacy.View.With.Dot\"")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
    }
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("GRANT SELECT ON \"Legacy.View.With.Dot\" TO legacy_reader")
            .unwrap();
    }
    let mut session = SqlSession::new(&mut db);
    assert_eq!(session.execute("WITH \"Quoted.CTE\" AS (SELECT 42 AS value) SELECT \"Quoted.CTE\".value FROM \"Quoted.CTE\"").unwrap().rows[0][0].to_cell(), "42");
    let identities = "SELECT oid,relname FROM pg_class WHERE relname IN ('Legacy.View.With.Dot','Legacy View With Space') ORDER BY relname";
    let before = session.execute(identities).unwrap().rows;
    assert_eq!(before.len(), 2);
    for name in ["Legacy.View.With.Dot", "Legacy View With Space"] {
        assert_eq!(
            session
                .execute(&format!("SELECT value FROM \"{name}\""))
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "10"
        );
    }
    assert_eq!(
        session
            .execute("SELECT \"Legacy.View.With.Dot\".value FROM \"Legacy.View.With.Dot\"")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "10"
    );
    session.execute("CREATE OR REPLACE VIEW \"Legacy.View.With.Dot\" AS SELECT row_key,value+1 AS value FROM legacy_keys").unwrap();
    assert_eq!(
        session
            .execute("SELECT value FROM \"Legacy.View.With.Dot\"")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "11"
    );
    assert_eq!(session.execute(identities).unwrap().rows, before);
    session
        .execute("DROP VIEW \"Legacy View With Space\"")
        .unwrap();
    assert_eq!(session.execute(identities).unwrap().rows.len(), 1);
    drop(session);
    assert!(db
        .get(collection, "Legacy.View.With.Dot")
        .unwrap()
        .is_some());
    assert!(db
        .get(collection, "Legacy View With Space")
        .unwrap()
        .is_none());
}
