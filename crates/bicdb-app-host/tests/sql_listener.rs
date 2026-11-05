use bicdb_app_host::{
    database::Database,
    sql_listener::{SqlListener, CONFIG_FILE},
};
use bicdb_pgwire::PgWireUserIdentity;
use bicdb_sql::SqlSession;
use std::{fs, net::SocketAddr, path::Path};
use tokio_postgres::NoTls;

fn config(root: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    let path = root.join(CONFIG_FILE);
    fs::write(&path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn sql_listeners_require_explicit_private_local_operator_configuration() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(&root.path().join("database")).unwrap();
    assert!(SqlListener::open(root.path(), &db, false)
        .unwrap()
        .is_none());
    for invalid in [
        r#"{"format":2,"listen":"127.0.0.1:0"}"#,
        r#"{"format":1,"listen":"0.0.0.0:0"}"#,
        r#"{"format":1,"listen":"127.0.0.1:0","require_auth":false}"#,
        r#"{"format":1,"listen":"127.0.0.1:0","max_connections":0}"#,
        r#"{"format":1,"listen":"127.0.0.1:0","max_connections":513}"#,
    ] {
        config(root.path(), invalid);
        assert!(SqlListener::open(root.path(), &db, false).is_err());
    }
    config(root.path(), r#"{"format":1,"listen":"127.0.0.1:0"}"#);
    assert!(SqlListener::open(root.path(), &db, true).is_err());
    fs::set_permissions(
        root.path().join(CONFIG_FILE),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(SqlListener::open(root.path(), &db, false).is_err());
    fs::rename(
        root.path().join(CONFIG_FILE),
        root.path().join("other.json"),
    )
    .unwrap();
    symlink("other.json", root.path().join(CONFIG_FILE)).unwrap();
    assert!(SqlListener::open(root.path(), &db, false).is_err());
}

async fn connect(
    address: SocketAddr,
    user: &str,
    password: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(address.port())
        .user(user)
        .password(password)
        .dbname("bicdb")
        .connect(NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, task)
}

#[tokio::test]
async fn listener_uses_the_same_writer_and_enforces_authenticated_identity_and_durable_rls() {
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(&root.path().join("database")).unwrap();
    SqlSession::new(&mut db.db.write()).execute("CREATE ROLE listener_owner LOGIN SUPERUSER; CREATE ROLE alice LOGIN; CREATE ROLE bob LOGIN; CREATE TABLE listener_rows(id TEXT PRIMARY KEY,tenant TEXT,value TEXT); INSERT INTO listener_rows VALUES('a','tenant-a','first'),('b','tenant-b','other'); ALTER TABLE listener_rows ENABLE ROW LEVEL SECURITY; ALTER TABLE listener_rows FORCE ROW LEVEL SECURITY; CREATE POLICY isolate ON listener_rows USING(tenant=current_setting('carrier.current_tenant',true)) WITH CHECK(tenant=current_setting('carrier.current_tenant',true)); GRANT SELECT,INSERT,UPDATE ON listener_rows TO alice,bob").unwrap();
    for (user, tenant) in [("alice", "tenant-a"), ("bob", "tenant-b")] {
        bicdb_pgwire::create_user_with_identity(
            root.path().join("database/data"),
            user,
            "fixture-password",
            PgWireUserIdentity::new(user, tenant),
        )
        .unwrap();
    }
    config(root.path(), r#"{"format":1,"listen":"127.0.0.1:0"}"#);
    let listener = SqlListener::open(root.path(), &db, false).unwrap().unwrap();
    assert!(tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(listener.address().port())
        .user("alice")
        .password("wrong")
        .dbname("bicdb")
        .connect(NoTls)
        .await
        .is_err());
    let (alice, alice_task) = connect(listener.address(), "alice", "fixture-password").await;
    let (bob, bob_task) = connect(listener.address(), "bob", "fixture-password").await;
    assert_eq!(
        alice
            .query("SELECT id FROM listener_rows", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(
        bob.query("SELECT id FROM listener_rows", &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["b"]
    );
    assert!(alice
        .batch_execute("SET carrier.current_tenant='tenant-b'")
        .await
        .is_err());
    assert!(alice
        .execute(
            "INSERT INTO listener_rows VALUES('forbidden','tenant-b','bad')",
            &[]
        )
        .await
        .is_err());
    alice
        .execute(
            "INSERT INTO listener_rows VALUES('saved','tenant-a','persisted')",
            &[],
        )
        .await
        .unwrap();
    let rows=SqlSession::new(&mut db.db.write()).execute("SET SESSION AUTHORIZATION listener_owner; SELECT value FROM listener_rows WHERE id='saved'").unwrap().rows;
    assert_eq!(rows[0][0].to_cell(), "persisted");
    drop(alice);
    drop(bob);
    alice_task.await.unwrap();
    bob_task.await.unwrap();
    drop(listener);
    drop(db);
    let db = Database::open(&root.path().join("database")).unwrap();
    let listener = SqlListener::open(root.path(), &db, false).unwrap().unwrap();
    let (alice, task) = connect(listener.address(), "alice", "fixture-password").await;
    assert_eq!(
        alice
            .query_one("SELECT value FROM listener_rows WHERE id='saved'", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "persisted"
    );
    drop(alice);
    task.await.unwrap();
    drop(listener);
}
