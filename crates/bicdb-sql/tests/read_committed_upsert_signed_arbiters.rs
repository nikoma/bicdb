//! Native signed SQL must use the same current arbiter winner as PostgreSQL,
//! while consuming exactly one actual-action grant/audit and preserving policies.
use bicdb_core::{
    BicDb, DbConfig, MutationActor, MutationGrantSpec, MutationOperation, MutationPolicy,
    StorageMode,
};
use bicdb_sql::{SqlSession, SqlValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{mpsc, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn actor() -> MutationActor {
    MutationActor {
        actor_id: "signed-arbiter".into(),
        roles: BTreeSet::from(["reader".into(), "writer".into()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "signed-sql-proof".into(),
        originating_resource: None,
        originating_action: None,
        trace_id: "current-arbiter".into(),
        deadline_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 120000,
    }
}
fn grant() -> MutationGrantSpec {
    MutationGrantSpec {
        relation: "signed_arbiters".into(),
        operation: MutationOperation::Bulk,
        record_id: None,
        record_id_prefix: None,
        expected_version: None,
        version_field: None,
        allowed_columns: ["id", "k", "v"].into_iter().map(String::from).collect(),
        bulk: true,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: None,
        workspace_field: None,
        audit_metadata: BTreeMap::from([("schedule".into(), "one actual action".into())]),
    }
}
struct Case {
    schema: &'static str,
    seed: &'static str,
    owner: &'static str,
    waiter: &'static str,
    waits: bool,
    has_id: bool,
    returned: Vec<Vec<SqlValue>>,
    final_rows: Vec<Vec<SqlValue>>,
    moved: bool,
}
fn row(values: &[i64]) -> Vec<SqlValue> {
    values.iter().copied().map(SqlValue::Int).collect()
}
fn case(kind: &str) -> Case {
    let primary = "CREATE TABLE signed_arbiters(id INT PRIMARY KEY,v INT)";
    let secondary = "CREATE TABLE signed_arbiters(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)";
    let mut case=Case{schema:secondary,seed:"",owner:"INSERT INTO signed_arbiters VALUES(1,'same',20)",waiter:"INSERT INTO signed_arbiters VALUES(2,'same',10) ON CONFLICT(k) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING id,v",waits:true,has_id:true,returned:vec![row(&[1,30])],final_rows:vec![row(&[1,30])],moved:false};
    match kind {
        "primary" => {
            case.schema = primary;
            case.owner = "INSERT INTO signed_arbiters VALUES(1,20)";
            case.waiter="INSERT INTO signed_arbiters VALUES(1,10) ON CONFLICT(id) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING id,v";
        }
        "secondary" => {}
        "char" => {
            case.schema = "CREATE TABLE signed_arbiters(k CHAR(4) PRIMARY KEY,v INT)";
            case.owner = "INSERT INTO signed_arbiters VALUES('ab',20)";
            case.waiter="INSERT INTO signed_arbiters VALUES('ab  ',10) ON CONFLICT(k) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING v";
            case.has_id = false;
            case.returned = vec![row(&[30])];
            case.final_rows = vec![row(&[30])];
        }
        "numeric" => {
            case.schema = "CREATE TABLE signed_arbiters(k NUMERIC(10,2) PRIMARY KEY,v INT)";
            case.owner = "INSERT INTO signed_arbiters VALUES(1.0,20)";
            case.waiter="INSERT INTO signed_arbiters VALUES(1.00,10) ON CONFLICT(k) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING v";
            case.has_id = false;
            case.returned = vec![row(&[30])];
            case.final_rows = vec![row(&[30])];
        }
        "nulls_distinct" => {
            case.owner = "INSERT INTO signed_arbiters VALUES(1,NULL,20)";
            case.waiter="INSERT INTO signed_arbiters VALUES(2,NULL,10) ON CONFLICT(k) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING id,v";
            case.waits = false;
            case.returned = vec![row(&[2, 10])];
            case.final_rows = vec![row(&[1, 20]), row(&[2, 10])];
        }
        "nulls_not_distinct" => {
            case.schema="CREATE TABLE signed_arbiters(id INT PRIMARY KEY,k TEXT,v INT,CONSTRAINT null_key UNIQUE NULLS NOT DISTINCT(k))";
            case.owner = "INSERT INTO signed_arbiters VALUES(1,NULL,20)";
            case.waiter="INSERT INTO signed_arbiters VALUES(2,NULL,10) ON CONFLICT(k) DO UPDATE SET v=signed_arbiters.v+EXCLUDED.v RETURNING id,v";
        }
        "delete" => {
            case.seed = "INSERT INTO signed_arbiters VALUES(1,'same',20)";
            case.owner = "DELETE FROM signed_arbiters WHERE id=1";
            case.returned = vec![row(&[2, 10])];
            case.final_rows = vec![row(&[2, 10])];
        }
        "move" => {
            case.seed = "INSERT INTO signed_arbiters VALUES(1,'same',20)";
            case.owner = "UPDATE signed_arbiters SET k='other' WHERE id=1";
            case.returned = vec![row(&[2, 10])];
            case.final_rows = vec![row(&[1, 20]), row(&[2, 10])];
            case.moved = true;
        }
        _ => panic!("unknown case"),
    }
    case
}
fn run(kind: &str) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let c = case(kind);
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        SqlSession::new(&mut db).execute(c.schema).unwrap();
        if !c.seed.is_empty() {
            SqlSession::new(&mut db).execute(c.seed).unwrap();
        }
        db.set_mutation_policy(
            "signed_arbiters",
            MutationPolicy::grants_required().with_audit(),
        )
        .unwrap();
        let db = Arc::new(db);
        let mut owner = db.begin_application_transaction(actor()).unwrap();
        let authority = owner.issue_mutation_grant(grant()).unwrap();
        let mut session = SqlSession::new_shared(&db)
            .with_pending_transaction(owner)
            .with_signed_mutation_grants(BTreeMap::from([("signed_arbiters".into(), authority)]));
        session.execute(c.owner).unwrap();
        let owner = session.take_pending_transaction().unwrap();
        assert_eq!(owner.broker_publish_len(), 1);
        drop(session);
        let mut waiter = db.begin_application_transaction(actor()).unwrap();
        waiter.set_snapshot_isolation(false, 0).unwrap();
        let authority = waiter.issue_mutation_grant(grant()).unwrap();
        let keep = db.clone();
        let (done, received) = mpsc::channel();
        let query = c.waiter;
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<Vec<Vec<SqlValue>>, String> {
                let mut session = SqlSession::new_shared(&keep)
                    .with_pending_transaction(waiter)
                    .with_signed_mutation_grants(BTreeMap::from([(
                        "signed_arbiters".into(),
                        authority,
                    )]));
                let result = session.execute(query).map_err(|error| error.to_string())?;
                let tx = session.take_pending_transaction().unwrap();
                assert_eq!(
                    tx.broker_publish_len(),
                    1,
                    "one actual mutation produces one audit"
                );
                tx.commit().map_err(|error| error.to_string())?;
                Ok(result.rows)
            })();
            done.send(result).unwrap();
        });
        let early = received.recv_timeout(Duration::from_millis(100));
        let waited = early.is_err();
        let owner_result = owner.commit();
        let result = match early {
            Ok(result) => result,
            Err(_) => received.recv_timeout(Duration::from_secs(10)).unwrap(),
        };
        worker.join().unwrap();
        owner_result.unwrap_or_else(|error| panic!("{kind}/{mode:?}: owner failed: {error}"));
        assert_eq!(
            waited, c.waits,
            "{kind}/{mode:?}: wrong pending-winner wait"
        );
        assert_eq!(
            result.unwrap_or_else(|error| panic!("{kind}/{mode:?}: waiter failed: {error}")),
            c.returned,
            "{kind}/{mode:?}: current RETURNING"
        );
        let query = if c.has_id {
            "SELECT id,v FROM signed_arbiters ORDER BY id"
        } else {
            "SELECT v FROM signed_arbiters"
        };
        assert_eq!(
            SqlSession::new_shared(&db).execute(query).unwrap().rows,
            c.final_rows,
            "{kind}/{mode:?}: committed identities/values"
        );
        if c.moved {
            assert_eq!(
                SqlSession::new_shared(&db)
                    .execute("SELECT k FROM signed_arbiters WHERE id=1")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String("other".into())]]
            );
        }
    }
}
macro_rules! schedule {
    ($name:ident,$kind:literal) => {
        #[test]
        fn $name() {
            run($kind)
        }
    };
}
schedule!(signed_primary_current_winner, "primary");
schedule!(signed_secondary_current_winner, "secondary");
schedule!(signed_char_primary_current_winner, "char");
schedule!(signed_numeric_primary_current_winner, "numeric");
schedule!(signed_nulls_distinct_do_not_wait, "nulls_distinct");
schedule!(
    signed_nulls_not_distinct_wait_for_winner,
    "nulls_not_distinct"
);
schedule!(signed_deleted_key_inserts_new_identity, "delete");
schedule!(signed_moved_key_inserts_new_identity, "move");
