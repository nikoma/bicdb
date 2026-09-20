use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bicdb_core::{BicDb, Record, SecurityContext};
use bicdb_pgwire::{AuthMethod, PgWireCluster, PgWireConfig, PgWireServer, PgWireUserIdentity};
use bicdb_sql::{PgDate, SqlSession, SqlValue, PG_TYPE_REGISTRY};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Connection as _, Row as _};

type HmacSha256 = Hmac<Sha256>;

const TEST_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUKL73D38tL6nPoGBW491LKbwVJOIwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYyMDAyMjkxOFoXDTM2MDYx
NzAyMjkxOFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAuUwhNNcGWvn/PDkGt8yjXfLM/TGcjmtn9a4Up5ycPee/
aoML1Voy/FsgKwolh+MAym395ReNp5NIegroQnHrAQ0hkn3k3mX7KSuI6596FVoy
2O24WXiXZO/kcwRUGZUNLDc/VIeeePShaFfsbRRXzLFsUUCY9z73YcC1n/32lyC7
B3s31OzgN4gA74KQoFefpIioOvRsz3A4B2Cvq4DdxvshdnELMlj/jZXsgQc6iYIK
OWL502vUsJDmH3VmWB2EKG38E1hv58Dj7qe3X7j1mMSu+wYCOdqcMtpe+3Ln1foc
3kiuwNBAW6WSMm2WC/F+Qg/4iIX97FKf5/tOW/f41QIDAQABo1MwUTAdBgNVHQ4E
FgQULMa+xTH6YxTUyLRRYQu7SJ49vIAwHwYDVR0jBBgwFoAULMa+xTH6YxTUyLRR
YQu7SJ49vIAwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAtfjt
GYrVKthERzcwcGNMtg5gYgyWQ0IwxwGCBhFgMS1FLoaUOGQcNnT7VRLKfA47SzWg
J7D4++jSfgZqV6CDKLO+KlngdwEARSR4N1FuQ2meBIwyIIFUdFWl0rUG/ydXjhmg
uGK/Yk3XEYaHASYT0ihztpzCuE9To61zHRfVpo4mh5iqO4VoN34+phmmCcUboYXl
cOs/sG1FCkH14r0SjRq6IOvkhN7Ls9UTWl04orMuC9pZ4Li3LO5cJSjEXJ9QIgKK
au2/cOMC6Pn0u+ylOyt8c8bpe80y9DhAGn6Fv+7Mpbtu60mOvzVRlFNoPgFRcXPs
apvJZmXoYORdG/KsIQ==
-----END CERTIFICATE-----
"#;

const TEST_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC5TCE01wZa+f88
OQa3zKNd8sz9MZyOa2f1rhSnnJw9579qgwvVWjL8WyArCiWH4wDKbf3lF42nk0h6
CuhCcesBDSGSfeTeZfspK4jrn3oVWjLY7bhZeJdk7+RzBFQZlQ0sNz9Uh5549KFo
V+xtFFfMsWxRQJj3PvdhwLWf/faXILsHezfU7OA3iADvgpCgV5+kiKg69GzPcDgH
YK+rgN3G+yF2cQsyWP+NleyBBzqJggo5YvnTa9SwkOYfdWZYHYQobfwTWG/nwOPu
p7dfuPWYxK77BgI52pwy2l77cufV+hzeSK7A0EBbpZIybZYL8X5CD/iIhf3sUp/n
+05b9/jVAgMBAAECggEABXnkq1aa0fp8cA+xYqIyG8yEAWGYzQlgklKu5UCMH4o8
o82z/lNYRBFUszZCuLAymEjwOCKI5bBvyRI5yKjsNzo88+2I44e5CKpINXUfmweb
jsIBG5J5oEU0sloo5XP8ZoJK0APitj695xheHN/SiR1J5GGCg8rBGIr6SHxYGsw9
ynZEP8Nuo98kiqNGIBk7xVk4w6nDAWkuDogcLkyYpOMKoCLC+2xWcQsw1C706rS3
jjRF+xgYNJ2BItizNovxGqA++ZJXXv22pNFu8/y6ghvLMLd7zojwA1rSe9Ja/oCZ
hJyispDtoVf8oW75tOHuKDQnqn1AnepNtVxYmVRhGwKBgQDeUdjN5yCPXkeW7HQ0
/iTXEvalSEQh5zO496OrYrqhSdNKYoxJOV+V6Dcq7ApPKq6ugxri7prbAjix/jmL
FoBHMwUG5JphYvSlxHybMt7jMTc6yEAfiek40GJP5WYyN+F83h8wbEs8oCgf0uVy
Ax9lBY36+F4cdLQayjL/V5KXuwKBgQDVXnUupQi7eSrb7mrNH0xVGnXHAANGofur
RJYKjXJKesYjtyqCG5sUeaWeWiHv4wxsT3NO3X4g3XWDXbmtwXNXRkEo7lTlvjdr
JNlj7zZlecZWaeOIlft+Q/r/x2exHtJfHnnQj2L/bDxl9ZR1YnYDA2lE3809Mjz1
dwl/gILArwKBgQCUeSTJninolZZJ/PA+09vWpxuBlpmp6rZoOTpdIzpwrNUnQFlg
LajgfI0bZTgdVuwCMBysoZ1Z1kn21Umo0gYphrE8wT84+tVYP7jYDUk9gYjZAROR
/JB9GO4PXay6rQcyVUWPGUPF4U/qsPX7BorY9LS1f1mat3XwzkjwrpOAMQKBgQDI
w00da5nQ1IzYTfheM0HenbwOV9u9PTMRjsJjAX51yBnhhzpPG+yKkn+chRCDqC6L
RyKnJU/FWrt0tN6+OFTv3KH5AnANkDKS9SQ7nNyhFLjjnFTEsuLlhs+Iljbh+K9X
YtSZwiETVuNpyG49GT0TTsVhUffKyheDm9LrDp947wKBgA0dORaiD9nRR0NygMK5
ALJg4T6xFpyTRExoXYkBoLH45qXeAEB2w2ZrFwKWP4qM/atkX0ykDqFmArpLigyZ
0iVgY0vGUpZFstT289pXq6Gc0lRrjFK58tSVJhwCBMKYrnTVPQE7zxA9DiU+WrmG
A+8xFVUlq84hEEAuKdP9BbxL
-----END PRIVATE KEY-----
"#;

#[test]
fn cluster_routes_multiple_persistent_databases_on_one_listener() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = PgWireCluster::open(dir.path(), "bicdb", PgWireConfig::default()).unwrap();
    let server = cluster.default_server().unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut root = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut root, "bicdb", "bicdb");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE ROLE tenant_owner CREATEDB LOGIN;");
    assert!(read_tags_until_ready(&mut root).0.contains(&b'C'));
    send_query(&mut root, "CREATE DATABASE tenant_a OWNER tenant_owner;");
    assert!(read_tags_until_ready(&mut root).0.contains(&b'C'));
    send_query(
        &mut root,
        "CREATE TABLE isolation_probe (value TEXT PRIMARY KEY); INSERT INTO isolation_probe VALUES ('root');",
    );
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE DOMAIN root_code AS TEXT;");
    read_until_ready(&mut root);
    send_query(
        &mut root,
        "SELECT oid FROM pg_type WHERE typname = 'root_code';",
    );
    let root_type_oid = read_query_rows(&mut root);

    let mut tenant_a = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut tenant_a, "bicdb", "tenant_a");
    read_until_ready(&mut tenant_a);
    send_query(&mut tenant_a, "SELECT current_database();");
    assert_eq!(
        read_query_rows(&mut tenant_a),
        vec![vec!["tenant_a".to_string()]]
    );
    send_query(
        &mut tenant_a,
        "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = 'tenant_owner';",
    );
    assert_eq!(
        read_query_rows(&mut tenant_a),
        vec![vec!["tenant_owner".to_string()]]
    );
    send_query(
        &mut tenant_a,
        "CREATE TABLE isolation_probe (value TEXT PRIMARY KEY); INSERT INTO isolation_probe VALUES ('tenant_a');",
    );
    read_until_ready(&mut tenant_a);
    send_query(&mut tenant_a, "SELECT value FROM isolation_probe;");
    assert_eq!(
        read_query_rows(&mut tenant_a),
        vec![vec!["tenant_a".to_string()]]
    );
    send_query(&mut tenant_a, "CREATE DOMAIN tenant_code AS TEXT;");
    read_until_ready(&mut tenant_a);
    send_query(
        &mut tenant_a,
        "SELECT oid FROM pg_type WHERE typname = 'tenant_code';",
    );
    assert_eq!(
        read_query_rows(&mut tenant_a),
        root_type_oid,
        "type OID allocation is isolated per cluster database",
    );

    send_query(&mut root, "SELECT value FROM isolation_probe;");
    assert_eq!(read_query_rows(&mut root), vec![vec!["root".to_string()]]);
    send_query(&mut root, "CREATE ROLE cluster_reader NOLOGIN;");
    assert!(read_tags_until_ready(&mut root).0.contains(&b'C'));
    send_query(
        &mut tenant_a,
        "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = 'cluster_reader';",
    );
    assert_eq!(
        read_query_rows(&mut tenant_a),
        vec![vec!["cluster_reader".to_string()]]
    );

    send_query(&mut root, "CREATE ROLE ordinary LOGIN;");
    assert!(read_tags_until_ready(&mut root).0.contains(&b'C'));
    let mut ordinary = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut ordinary, "ordinary", "bicdb");
    read_until_ready(&mut ordinary);
    send_query(&mut ordinary, "CREATE DATABASE forbidden;");
    let error = read_error_response(&mut ordinary);
    assert!(error.contains("42501"), "{error}");
    let database_directories = std::fs::read_dir(dir.path().join("databases"))
        .unwrap()
        .count();
    assert_eq!(database_directories, 2);
    let mut forbidden = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut forbidden, "bicdb", "forbidden");
    let error = read_startup_error(&mut forbidden);
    assert!(error.contains("3D000"), "{error}");

    let mut missing = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut missing, "bicdb", "missing");
    let error = read_startup_error(&mut missing);
    assert!(error.contains("3D000"), "{error}");

    root.write_all(b"X\0\0\0\x04").unwrap();
    tenant_a.write_all(b"X\0\0\0\x04").unwrap();
    ordinary.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    drop(server);
    drop(cluster);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = PgWireCluster::open(dir.path(), "ignored", PgWireConfig::default()).unwrap();
    assert_eq!(cluster.default_database(), "bicdb");
    let server = cluster.default_server().unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut tenant_a = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut tenant_a, "bicdb", "tenant_a");
    read_until_ready(&mut tenant_a);
    send_query(&mut tenant_a, "SELECT value FROM isolation_probe;");
    assert_eq!(
        read_query_rows(&mut tenant_a),
        vec![vec!["tenant_a".to_string()]]
    );
    send_query(
        &mut tenant_a,
        "SELECT oid FROM pg_type WHERE typname = 'tenant_code';",
    );
    assert_eq!(read_query_rows(&mut tenant_a), root_type_oid);
    tenant_a.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn cluster_shares_transactional_roles_but_isolates_user_types() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = PgWireCluster::open(dir.path(), "bicdb", PgWireConfig::default()).unwrap();
    let server = cluster.default_server().unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut root = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut root, "bicdb", "bicdb");
    read_until_ready(&mut root);
    for statement in [
        "CREATE ROLE app_owner CREATEDB LOGIN;",
        "CREATE ROLE app_reader NOLOGIN;",
        "CREATE ROLE app_user LOGIN;",
        "GRANT app_reader TO app_user WITH ADMIN OPTION;",
        "CREATE DATABASE app OWNER app_owner;",
    ] {
        send_query(&mut root, statement);
        read_until_ready(&mut root);
    }

    let mut app = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut app, "bicdb", "app");
    read_until_ready(&mut app);
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_auth_members AS membership \
         JOIN pg_roles AS granted ON granted.oid = membership.roleid \
         JOIN pg_roles AS member ON member.oid = membership.member \
         WHERE granted.rolname = 'app_reader' \
           AND member.rolname = 'app_user' \
           AND membership.admin_option;",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["1".to_string()]]);

    send_query(&mut root, "BEGIN;");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE ROLE rolled_back_role;");
    read_until_ready(&mut root);
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_roles WHERE rolname = 'rolled_back_role';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["0".to_string()]]);
    send_query(&mut root, "ROLLBACK;");
    read_until_ready(&mut root);
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_roles WHERE rolname = 'rolled_back_role';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["0".to_string()]]);

    send_query(&mut root, "BEGIN;");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE ROLE committed_role;");
    read_until_ready(&mut root);
    send_query(&mut root, "SAVEPOINT role_boundary;");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE ROLE savepoint_rolled_back_role;");
    read_until_ready(&mut root);
    send_query(&mut root, "ROLLBACK TO SAVEPOINT role_boundary;");
    read_until_ready(&mut root);
    send_query(&mut root, "COMMIT;");
    read_until_ready(&mut root);
    send_query(
        &mut app,
        "SELECT rolname FROM pg_roles \
         WHERE rolname IN ('committed_role', 'savepoint_rolled_back_role') \
         ORDER BY rolname;",
    );
    assert_eq!(
        read_query_rows(&mut app),
        vec![vec!["committed_role".to_string()]]
    );
    send_query(
        &mut root,
        "SELECT oid FROM pg_roles WHERE rolname = 'committed_role';",
    );
    let committed_role_oid = read_query_rows(&mut root);
    send_query(
        &mut app,
        "SELECT oid FROM pg_roles WHERE rolname = 'committed_role';",
    );
    assert_eq!(read_query_rows(&mut app), committed_role_oid);

    send_query(&mut app, "REVOKE app_reader FROM app_user;");
    read_until_ready(&mut app);
    send_query(
        &mut root,
        "SELECT count(*) FROM pg_auth_members AS membership \
         JOIN pg_roles AS granted ON granted.oid = membership.roleid \
         JOIN pg_roles AS member ON member.oid = membership.member \
         WHERE granted.rolname = 'app_reader' AND member.rolname = 'app_user';",
    );
    assert_eq!(read_query_rows(&mut root), vec![vec!["0".to_string()]]);
    send_query(&mut app, "GRANT app_reader TO app_user;");
    read_until_ready(&mut app);
    send_query(
        &mut root,
        "SELECT count(*) FROM pg_auth_members AS membership \
         JOIN pg_roles AS granted ON granted.oid = membership.roleid \
         JOIN pg_roles AS member ON member.oid = membership.member \
         WHERE granted.rolname = 'app_reader' AND member.rolname = 'app_user';",
    );
    assert_eq!(read_query_rows(&mut root), vec![vec!["1".to_string()]]);
    send_query(&mut app, "BEGIN;");
    read_until_ready(&mut app);
    send_query(&mut app, "REVOKE app_reader FROM app_user;");
    read_until_ready(&mut app);
    send_query(
        &mut root,
        "SELECT count(*) FROM pg_auth_members AS membership \
         JOIN pg_roles AS granted ON granted.oid = membership.roleid \
         JOIN pg_roles AS member ON member.oid = membership.member \
         WHERE granted.rolname = 'app_reader' AND member.rolname = 'app_user';",
    );
    assert_eq!(read_query_rows(&mut root), vec![vec!["1".to_string()]]);
    send_query(&mut app, "ROLLBACK;");
    read_until_ready(&mut app);
    send_query(&mut app, "ALTER ROLE app_reader LOGIN;");
    read_until_ready(&mut app);
    send_query(
        &mut root,
        "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'app_reader';",
    );
    assert_eq!(read_query_rows(&mut root), vec![vec!["t".to_string()]]);
    send_query(&mut root, "CREATE ROLE drop_from_app;");
    read_until_ready(&mut root);
    send_query(&mut app, "DROP ROLE drop_from_app;");
    read_until_ready(&mut app);
    send_query(
        &mut root,
        "SELECT count(*) FROM pg_roles WHERE rolname = 'drop_from_app';",
    );
    assert_eq!(read_query_rows(&mut root), vec![vec!["0".to_string()]]);

    send_query(&mut root, "CREATE DOMAIN root_only AS TEXT;");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE DOMAIN shared_name AS TEXT;");
    read_until_ready(&mut root);
    send_query(&mut app, "CREATE DOMAIN shared_name AS TEXT;");
    read_until_ready(&mut app);
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_type WHERE typname = 'root_only';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["0".to_string()]]);
    send_query(
        &mut root,
        "SELECT oid FROM pg_type WHERE typname = 'shared_name';",
    );
    let root_shared_oid = read_query_rows(&mut root);
    send_query(
        &mut app,
        "SELECT oid FROM pg_type WHERE typname = 'shared_name';",
    );
    let app_shared_oid = read_query_rows(&mut app);
    assert_ne!(root_shared_oid, app_shared_oid);
    send_query(&mut root, "SELECT 'root'::shared_name;");
    let (root_domain_rows, root_domain_oids) = read_query_rows_and_oids(&mut root);
    assert_eq!(root_domain_rows, vec![vec!["root".to_string()]]);
    assert_eq!(
        root_domain_oids,
        vec![root_shared_oid[0][0].parse::<i32>().unwrap()]
    );
    send_query(&mut app, "SELECT 'app'::shared_name;");
    let (app_domain_rows, app_domain_oids) = read_query_rows_and_oids(&mut app);
    assert_eq!(app_domain_rows, vec![vec!["app".to_string()]]);
    assert_eq!(
        app_domain_oids,
        vec![app_shared_oid[0][0].parse::<i32>().unwrap()]
    );
    send_query(&mut root, "DROP DOMAIN shared_name;");
    read_until_ready(&mut root);
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_type WHERE typname = 'shared_name';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["1".to_string()]]);

    root.write_all(b"X\0\0\0\x04").unwrap();
    app.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    drop(server);
    drop(cluster);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = PgWireCluster::open(dir.path(), "ignored", PgWireConfig::default()).unwrap();
    let server = cluster.default_server().unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut app = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut app, "bicdb", "app");
    read_until_ready(&mut app);
    send_query(
        &mut app,
        "SELECT rolname FROM pg_roles WHERE rolname = 'committed_role';",
    );
    assert_eq!(
        read_query_rows(&mut app),
        vec![vec!["committed_role".to_string()]]
    );
    send_query(
        &mut app,
        "SELECT count(*) FROM pg_auth_members AS membership \
         JOIN pg_roles AS granted ON granted.oid = membership.roleid \
         JOIN pg_roles AS member ON member.oid = membership.member \
         WHERE granted.rolname = 'app_reader' AND member.rolname = 'app_user';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["1".to_string()]]);
    send_query(
        &mut app,
        "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'app_reader';",
    );
    assert_eq!(read_query_rows(&mut app), vec![vec!["t".to_string()]]);
    send_query(
        &mut app,
        "SELECT oid FROM pg_type WHERE typname = 'shared_name';",
    );
    assert_eq!(read_query_rows(&mut app), app_shared_oid);
    app.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn protocol_3_0_startup_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup_version(&mut client, 196_608, true);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn postgres_identity_override_reaches_handshake_and_sql_without_faking_bicdb_version() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            postgres_server_version: "16.7".to_string(),
            postgres_server_version_num: "160007".to_string(),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "bicdb");
    let parameters = read_startup_parameters_until_ready(&mut client);
    assert_eq!(
        parameters.get("server_version").map(String::as_str),
        Some("16.7")
    );

    send_query(&mut client, "SELECT version(), bicdb_version();");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            format!(
                "BicDB {} (PostgreSQL 16.7 wire compatible)",
                bicdb_sql::BICDB_VERSION
            ),
            bicdb_sql::BICDB_VERSION.to_string(),
        ]]
    );
    send_query(&mut client, "SHOW server_version;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["16.7".to_string()]]);
    send_query(&mut client, "SHOW server_version_num;");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["160007".to_string()]]
    );
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn postgres_version_banner_mode_supports_compatibility_clients_without_hiding_bicdb_version() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            postgres_version_banner: true,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "bicdb");
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT version(), bicdb_version();");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "PostgreSQL 18.4 (BicDB compatibility mode)".to_string(),
            bicdb_sql::BICDB_VERSION.to_string(),
        ]]
    );
    send_query(&mut client, "BEGIN;");
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        "SELECT set_config('app.request_roles', 'org_admin,provider', true),
                current_setting('app.request_roles', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "org_admin,provider".to_string(),
            "org_admin,provider".to_string(),
        ]]
    );
    send_query(
        &mut client,
        "SELECT set_config('carrier.current_roles', 'provider', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["provider".to_string()]]
    );
    send_query(
        &mut client,
        "SELECT current_setting('carrier.current_roles', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["provider".to_string()]]
    );
    send_query(&mut client, "COMMIT;");
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        "SELECT COALESCE(current_setting('app.request_roles', true), '<null>'),
                COALESCE(current_setting('carrier.current_roles', true), '<null>');",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["<null>".to_string(), "<null>".to_string()]]
    );
    send_query(
        &mut client,
        "SELECT set_config('carrier.current_roles', 'platform_admin', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["platform_admin".to_string()]]
    );
    send_query(
        &mut client,
        "SELECT COALESCE(current_setting('carrier.current_roles', true), '<null>');",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["<null>".to_string()]]
    );
    send_query(
        &mut client,
        "SELECT set_config('carrier.current_roles', 'provider', false);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["provider".to_string()]]
    );
    send_query(
        &mut client,
        "SELECT current_setting('carrier.current_roles', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["provider".to_string()]]
    );
    send_query(&mut client, "RESET carrier.current_roles;");
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        "SELECT set_config('bicdb.postgres_version_banner', 'off', false);",
    );
    assert!(read_error_response(&mut client).contains("protected session attribute"));
    send_query(&mut client, "DISCARD ALL;");
    assert!(read_query_rows(&mut client).is_empty());
    send_query(&mut client, "SELECT version(), bicdb_version();");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "PostgreSQL 18.4 (BicDB compatibility mode)".to_string(),
            bicdb_sql::BICDB_VERSION.to_string(),
        ]]
    );
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn protocol_3_2_startup_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup_version(&mut client, 196_610, true);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn protocol_3_2_unsupported_option_negotiates_then_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup_version_with_options(&mut client, 196_610, true, &[("_pq_.probe", "1")]);
    let (protocol_version, unsupported_options) = read_negotiate_protocol_version(&mut client);
    assert_eq!(protocol_version, 196_610);
    assert_eq!(unsupported_options, vec!["_pq_.probe".to_string()]);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn local_no_auth_accepts_startup_without_auth_challenge() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "bicdb");
    assert_eq!(read_auth_code(&mut client), 0);
    read_until_ready(&mut client);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn remote_no_auth_server_mode_is_refused_unless_explicitly_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let refused = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            host: "0.0.0.0".to_string(),
            ..PgWireConfig::default()
        },
    )
    .unwrap_err();
    assert!(refused.to_string().contains("refusing no-auth server"));

    let allowed = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            host: "0.0.0.0".to_string(),
            allow_remote_no_auth: true,
            ..PgWireConfig::default()
        },
    );
    assert!(allowed.is_ok());
}

#[test]
fn malformed_startup_packets_return_errors_without_poisoning_listener() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut missing_terminator = TcpStream::connect(address).unwrap();
    send_startup_version(&mut missing_terminator, 196_608, false);
    let error = read_startup_error(&mut missing_terminator);
    assert!(error.contains("FATAL"));
    assert!(error.contains("08P01"));
    assert!(error.contains("startup message missing terminator"));

    let mut missing_protocol = TcpStream::connect(address).unwrap();
    missing_protocol.write_all(&4_i32.to_be_bytes()).unwrap();
    let error = read_startup_error(&mut missing_protocol);
    assert!(error.contains("FATAL"));
    assert!(error.contains("08P01"));
    assert!(error.contains("startup message missing protocol code"));

    let mut valid_client = TcpStream::connect(address).unwrap();
    startup(&mut valid_client);
    read_until_ready(&mut valid_client);
    send_query(&mut valid_client, "SELECT 1;");
    assert_eq!(
        read_query_rows(&mut valid_client),
        vec![vec!["1".to_string()]]
    );
    valid_client.write_all(b"X\0\0\0\x04").unwrap();

    for _ in 0..50 {
        if server.stats_snapshot().active_connections == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(server.stats_snapshot().active_connections, 0);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn simple_query_protocol_returns_select_1_and_collection_count() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("patients").unwrap();
    db.batch_insert(
        "patients",
        [
            Record::new("patient-a").with_metadata(json!({"clinic": "rural-7"})),
            Record::new("patient-b").with_metadata(json!({"clinic": "rural-7"})),
        ],
    )
    .unwrap();
    drop(db);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, ";");
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(tags, vec![b'I']);
    assert_eq!(status, b'I');

    send_query(&mut client, "SELECT 1;");
    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    send_query(&mut client, "SELECT COUNT(*) FROM patients;");
    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["2".to_string()]]);

    send_query(&mut client, "SELECT * FROM patients LIMIT 10;");
    let rows = read_query_rows(&mut client);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "patient-a");
    assert_eq!(rows[1][0], "patient-b");

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn segmented_query_survives_pauses_longer_than_the_poll_timeout() {
    // The client loop polls the socket with a 250 ms read timeout so idle
    // connections can receive NOTIFY pushes. A message whose bytes arrive in
    // segments separated by more than that (a slow or CPU-starved client
    // sending a large statement) must still be assembled, not dropped with
    // "Resource temporarily unavailable".
    let dir = tempfile::tempdir().unwrap();
    drop(BicDb::open(dir.path()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let mut payload = b"SELECT 41 + 1 AS answer;".to_vec();
    payload.push(0);
    let len = ((payload.len() as i32) + 4).to_be_bytes();
    let pause = Duration::from_millis(700);
    // Tag alone, then a partial length, then the rest of the length, then
    // the payload in two halves: every boundary a poll timeout can hit.
    client.write_all(b"Q").unwrap();
    thread::sleep(pause);
    client.write_all(&len[..2]).unwrap();
    thread::sleep(pause);
    client.write_all(&len[2..]).unwrap();
    thread::sleep(pause);
    let split = payload.len() / 2;
    client.write_all(&payload[..split]).unwrap();
    thread::sleep(pause);
    client.write_all(&payload[split..]).unwrap();

    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["42".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn simple_query_current_schemas_returns_pg_array_text() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "SELECT pg_catalog.current_schemas(false);");
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![1003]);
    assert_eq!(rows, vec![vec!["{public}".to_string()]]);

    send_query(&mut client, "SELECT pg_catalog.current_schemas(true);");
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![1003]);
    assert_eq!(rows, vec![vec!["{pg_catalog,public}".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn simple_query_sql_prepare_execute_and_deallocate_are_supported() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "PREPARE echo_oid(pg_catalog.oid) AS SELECT $1;",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "EXECUTE echo_oid(123);");
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![26]);
    assert_eq!(rows, vec![vec!["123".to_string()]]);

    send_query(&mut client, "DEALLOCATE PREPARE echo_oid;");
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "EXECUTE echo_oid(123);");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("prepared statement"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn simple_query_sql_prepare_accepts_typed_set_returning_function_alias() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "PREPARE ostat (INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR) AS \
         SELECT * FROM ostat($1,$2,$3,$4,'$5') AS \
         (ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_quantity SMALLINT, \
          ol_amount NUMERIC, ol_delivery_d TIMESTAMP WITH TIME ZONE)",
    );
    assert!(read_query_rows(&mut client).is_empty());

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn standby_server_exposes_ha_status_and_rejects_writes() {
    let root = tempfile::tempdir().unwrap();
    let primary_path = root.path().join("primary");
    let standby_path = root.path().join("standby");
    {
        let mut db = BicDb::open(&primary_path).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    BicDb::configure_standby_from(&standby_path, &primary_path).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(&standby_path, PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "SELECT * FROM bicdb_ha_status;");
    let rows = read_query_rows(&mut client);
    assert_eq!(rows[0][0], "standby");
    assert_eq!(rows[0][1], "t");
    assert_eq!(rows[0][6], "0");

    send_query(&mut client, "INSERT INTO patients (id) VALUES ('p-2');");
    let error = read_error_response(&mut client);
    assert!(error.contains("read-only standby"));
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

/// Regression: `COPY <schema>.<table> FROM STDIN` must load into THAT
/// schema. pgwire collapsed `schema.table` to `table`, so a restore's
/// `COPY carrier_private.context_nonces` silently loaded into
/// `public.context_nonces` (or failed when only the qualified table
/// existed).
#[test]
fn copy_from_stdin_keeps_the_schema_qualifier() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    // Same table name in two schemas: only the qualified target may receive
    // the rows.
    send_query(&mut client, "CREATE SCHEMA carrier_private;");
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE public.context_nonces (id TEXT PRIMARY KEY, nonce TEXT);",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE carrier_private.context_nonces (id TEXT PRIMARY KEY, nonce TEXT);",
    );
    read_query_rows(&mut client);

    send_query(
        &mut client,
        "COPY carrier_private.context_nonces (id, nonce) FROM STDIN CSV;",
    );
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G', "expected CopyInResponse");
    assert_eq!(payload[0], 0);
    send_message(&mut client, b'd', b"n1,alpha\nn2,beta\n");
    send_message(&mut client, b'c', &[]);
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'I');
    assert!(tags.contains(&b'C'), "COPY failed: {tags:?}");

    // The rows landed in carrier_private...
    send_query(
        &mut client,
        "SELECT id, nonce FROM carrier_private.context_nonces ORDER BY id;",
    );
    let rows = read_query_rows(&mut client);
    assert_eq!(
        rows,
        vec![
            vec!["n1".to_string(), "alpha".to_string()],
            vec!["n2".to_string(), "beta".to_string()],
        ],
        "COPY must load into the qualified schema"
    );

    // ...and NOT in public.
    send_query(&mut client, "SELECT count(*) FROM public.context_nonces;");
    let rows = read_query_rows(&mut client);
    assert_eq!(
        rows,
        vec![vec!["0".to_string()]],
        "public must not receive rows addressed to carrier_private"
    );

    // COPY ... TO STDOUT reads back from the qualified table too.
    send_query(
        &mut client,
        "COPY carrier_private.context_nonces TO STDOUT;",
    );
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'I');
    assert!(
        tags.contains(&b'H') || tags.contains(&b'd'),
        "COPY TO failed: {tags:?}"
    );

    send_query(&mut client, "");
    drop(client);
    let _ = server.join();
}

/// Regression (crawl-ingestion incident): non-canonical transaction enders
/// must end the block and leave the connection usable. The dispatcher only
/// string-matched bare `commit`/`rollback`, so `ROLLBACK WORK` / `END` ended
/// the session's transaction while the connection kept `in_transaction` set —
/// ReadyForQuery still claimed 'T' and the next BEGIN failed with
/// `server error: nested transactions are not supported`.
#[test]
fn alternate_transaction_end_spellings_reset_connection_state() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE tx_spellings (id TEXT PRIMARY KEY, v TEXT);",
    );
    read_query_rows(&mut client);

    for ender in [
        "ROLLBACK WORK",
        "COMMIT WORK",
        "END",
        "ROLLBACK TRANSACTION",
    ] {
        send_query(&mut client, "BEGIN;");
        let (_, status) = read_tags_until_ready(&mut client);
        assert_eq!(status, b'T', "BEGIN must open a block before {ender}");

        send_query(&mut client, &format!("{ender};"));
        let (_, status) = read_tags_until_ready(&mut client);
        assert_eq!(status, b'I', "`{ender}` must end the block and report idle");

        // The next BEGIN must not see a phantom open transaction.
        send_query(&mut client, "BEGIN;");
        let (tags, status) = read_tags_until_ready(&mut client);
        assert!(
            !tags.contains(&b'E'),
            "`{ender}` wedged the connection: the following BEGIN errored"
        );
        assert_eq!(
            status, b'T',
            "the connection must still be usable after {ender}"
        );
        send_query(&mut client, "ROLLBACK;");
        read_tags_until_ready(&mut client);
    }

    drop(client);
    let _ = server.join();
}

/// Regression: a client that vanishes must not leave a "phantom" connection
/// registered. Cleanup used to be plain statements after the handler loop, so
/// an early return or a panic skipped unregistration and the entry survived
/// its client — those are the connections that blocked graceful shutdown
/// until SIGKILL.
#[test]
fn abruptly_dropped_clients_do_not_leave_phantom_connections() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // ONE shared server, so every connection lands in the same registry.
    let server =
        bicdb_pgwire::PgWireServer::open(dir.path(), bicdb_pgwire::PgWireConfig::default())
            .unwrap();
    let accept_server = server.clone();
    let acceptor = thread::spawn(move || {
        for _ in 0..3 {
            let Ok((stream, addr)) = listener.accept() else {
                return;
            };
            let server = accept_server.clone();
            thread::spawn(move || {
                let _ = bicdb_pgwire::handle_client_with_server(stream, addr, server);
            });
        }
    });

    // Two clients that vanish right after startup, one that vanishes mid
    // session without terminating.
    for run_query in [false, false, true] {
        let mut client = TcpStream::connect(address).unwrap();
        startup(&mut client);
        read_until_ready(&mut client);
        if run_query {
            send_query(&mut client, "SELECT 1;");
            read_query_rows(&mut client);
        }
        drop(client);
    }

    // Every one of them must unregister without another connection arriving
    // to prompt it.
    let mut live = usize::MAX;
    for _ in 0..200 {
        live = server.connection_snapshots().len();
        if live == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(
        live, 0,
        "dropped clients left {live} phantom connection(s) registered"
    );

    let _ = acceptor.join();
}

fn copy_from_stdin_csv_imports_rows_over_wire() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "COPY public.patients FROM STDIN CSV;");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G');
    assert_eq!(payload[0], 0);

    send_message(&mut client, b'd', b"p1,Ada,36\np2,\"John, Jr.\",45\n");
    send_message(&mut client, b'c', &[]);
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'I');
    assert!(
        tags.contains(&b'C'),
        "unexpected COPY response tags: {tags:?}"
    );

    send_query(
        &mut client,
        "SELECT id, name, age FROM patients ORDER BY id;",
    );
    let rows = read_query_rows(&mut client);
    assert_eq!(
        rows,
        vec![
            vec!["p1".to_string(), "Ada".to_string(), "36".to_string()],
            vec!["p2".to_string(), "John, Jr.".to_string(), "45".to_string()],
        ]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn copy_arrays_roundtrip_quoted_elements_bounds_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server_path = path.clone();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, server_path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE array_copy_wire (
            id TEXT PRIMARY KEY,
            labels TEXT[] NOT NULL,
            values_int INT4[] NOT NULL
         );",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(
        &mut client,
        "COPY array_copy_wire (id, labels, values_int) FROM STDIN;",
    );
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(
        &mut client,
        b'd',
        b"quoted\t{\"alpha,beta\",\"brace{value}\",NULL}\t[0:1]={10,20}\nempty\t{}\t{}\n",
    );
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');

    send_query(
        &mut client,
        "COPY (SELECT id, labels, values_int FROM array_copy_wire ORDER BY id) TO STDOUT;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(
        rows,
        vec![
            "empty\t{}\t{}\n",
            "quoted\t{\"alpha,beta\",\"brace{value}\",NULL}\t[0:1]={10,20}\n",
        ]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();

    let mut reopened = BicDb::open(path).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute(
                "SELECT labels, values_int, array_lower(values_int, 1)
                 FROM array_copy_wire WHERE id = 'quoted'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Json(json!(["alpha,beta", "brace{value}", null])),
            SqlValue::Json(json!({
                "$bicdb_array_input": {
                    "lower_bounds": [0],
                    "value": [10, 20],
                }
            })),
            SqlValue::Int(0),
        ]],
    );
}

#[test]
fn copy_geometric_arrays_roundtrip_with_postgresql_delimiters() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE geometric_array_copy_wire (
            id TEXT PRIMARY KEY,
            points POINT[],
            lines LINE[],
            segments LSEG[],
            boxes BOX[],
            paths PATH[],
            polygons POLYGON[],
            circles CIRCLE[]
         );",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "COPY geometric_array_copy_wire FROM STDIN;");
    assert_eq!(read_message(&mut client).0, b'G');
    let copy_row = concat!(
        "row\t{\"(1,2)\",\"(3,4)\"}",
        "\t{\"{1,2,3}\",\"{4,5,6}\"}",
        "\t{\"[(1,2),(3,4)]\",\"[(5,6),(7,8)]\"}",
        "\t{(1,2),(3,4);(5,6),(7,8)}",
        "\t{\"[(1,2),(3,4)]\",\"((5,6),(7,8))\"}",
        "\t{\"((1,2),(3,4),(5,6))\",\"((7,8),(9,10),(11,12))\"}",
        "\t{\"<(1,2),3>\",\"<(4,5),6>\"}\n",
    );
    send_message(&mut client, b'd', copy_row.as_bytes());
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');

    send_query(
        &mut client,
        "COPY (SELECT * FROM geometric_array_copy_wire) TO STDOUT;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(
        rows,
        vec![concat!(
            "row\t{\"(1,2)\",\"(3,4)\"}",
            "\t{\"{1,2,3}\",\"{4,5,6}\"}",
            "\t{\"[(1,2),(3,4)]\",\"[(5,6),(7,8)]\"}",
            "\t{(3,4),(1,2);(7,8),(5,6)}",
            "\t{\"[(1,2),(3,4)]\",\"((5,6),(7,8))\"}",
            "\t{\"((1,2),(3,4),(5,6))\",\"((7,8),(9,10),(11,12))\"}",
            "\t{\"<(1,2),3>\",\"<(4,5),6>\"}\n",
        )],
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn boolean_copy_accepts_postgresql_prefixes_and_rejects_ambiguous_input() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE boolean_copy (id TEXT PRIMARY KEY, flag BOOLEAN);",
    );
    read_query_rows(&mut client);
    send_query(&mut client, "COPY boolean_copy (id, flag) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"true,tr\nfalse,of\n");
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');

    send_query(
        &mut client,
        "SELECT id, flag FROM boolean_copy ORDER BY flag, id;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![
            vec!["false".to_string(), "f".to_string()],
            vec!["true".to_string(), "t".to_string()],
        ]
    );

    send_query(&mut client, "COPY boolean_copy (id, flag) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"ambiguous,o\n");
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22P02"), "{error}");

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn integer_range_errors_are_consistent_across_query_bind_and_copy() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "SELECT 32767::int2 + 1::int2;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22003"), "{error}");
    assert!(error.contains("smallint out of range"), "{error}");

    send_query(&mut client, "SELECT 9223372036854775807::int8 + 1::int8;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22003"), "{error}");
    assert!(error.contains("bigint out of range"), "{error}");

    send_query(&mut client, "SELECT 1::int8 / 0::int8;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22012"), "{error}");
    assert!(error.contains("division by zero"), "{error}");

    send_parse(&mut client, "narrow_integer", "SELECT $1::int2", &[23]);
    send_bind_binary(
        &mut client,
        "",
        "narrow_integer",
        &[(1, 32768_i32.to_be_bytes().to_vec())],
        &[1],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22003"), "{error}");
    assert!(error.contains("smallint out of range"), "{error}");

    send_query(
        &mut client,
        "CREATE TABLE integer_copy_limits (value INT2);",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "COPY integer_copy_limits (value) FROM STDIN CSV;",
    );
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"32768\n");
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("22003"), "{error}");
    assert!(error.contains("smallint out of range"), "{error}");

    send_query(&mut client, "SELECT COUNT(*) FROM integer_copy_limits;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["0".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn floating_point_text_and_bind_formats_match_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT 0.1::float4, 1e6::float4, 1e-5::float8,
                '-0'::float8, 'Infinity'::float8, 'NaN'::float8;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "0.1".to_string(),
            "1e+06".to_string(),
            "1e-05".to_string(),
            "-0".to_string(),
            "Infinity".to_string(),
            "NaN".to_string(),
        ]]
    );

    send_parse(
        &mut client,
        "float_text_bind",
        "SELECT $1::float4, $2::float8, $3::float8",
        &[700, 701, 701],
    );
    send_bind(&mut client, "", "float_text_bind", &["0.1", "NaN", "-0"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["0.1".to_string(), "NaN".to_string(), "-0".to_string()]]
    );

    send_parse(
        &mut client,
        "float_binary_bind",
        "SELECT $1::float4, $2::float8",
        &[700, 701],
    );
    send_bind_binary(
        &mut client,
        "",
        "float_binary_bind",
        &[
            (1, f32::NAN.to_be_bytes().to_vec()),
            (1, (-0.0_f64).to_be_bytes().to_vec()),
        ],
        &[0],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["NaN".to_string(), "-0".to_string()]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn exact_numeric_text_and_binary_binds_preserve_postgresql_values() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "numeric_text_bind",
        "SELECT $1::numeric + $2::numeric, $3::numeric, $4::numeric",
        &[1700, 1700, 1700, 1700],
    );
    send_bind(
        &mut client,
        "",
        "numeric_text_bind",
        &["12345678901234567890.01", "0.09", "1.2300e2", "NaN"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "12345678901234567890.10".to_string(),
            "123.00".to_string(),
            "NaN".to_string(),
        ]]
    );

    send_parse(
        &mut client,
        "numeric_binary_specials",
        "SELECT $1::numeric, $2::numeric, $3::numeric",
        &[1700, 1700, 1700],
    );
    send_bind_binary(
        &mut client,
        "",
        "numeric_binary_specials",
        &[
            (1, numeric_binary(0, 0, 0xC000, 0, &[])),
            (1, numeric_binary(0, 0, 0xD000, 0, &[])),
            (1, numeric_binary(0, 0, 0xF000, 0, &[])),
        ],
        &[0],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "NaN".to_string(),
            "Infinity".to_string(),
            "-Infinity".to_string(),
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn money_text_and_binary_protocol_preserve_exact_cents() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT '$1,234.56'::money, '(123.45)'::money,
                10::money + 2::money, 10::money / 4::money;",
    );
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![790, 790, 790, 701]);
    assert_eq!(
        rows,
        vec![vec![
            "$1,234.56".to_string(),
            "-$123.45".to_string(),
            "$12.00".to_string(),
            "2.5".to_string(),
        ]]
    );

    send_parse(
        &mut client,
        "money_binary_bind",
        "SELECT $1::money + $2::money",
        &[790, 790],
    );
    send_bind_binary(
        &mut client,
        "",
        "money_binary_bind",
        &[
            (1, 123_456_i64.to_be_bytes().to_vec()),
            (1, (-1_i64).to_be_bytes().to_vec()),
        ],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (formats, rows, status) = read_binary_query_result(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![123_455_i64.to_be_bytes().to_vec()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn timestamp_binary_results_preserve_epoch_offsets_and_infinities() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_parse(
        &mut client,
        "timestamp_binary_limits",
        "SELECT '2000-01-01 00:00:00.000001'::timestamp,
                'infinity'::timestamp, '-infinity'::timestamp,
                '2000-01-01 09:00:00.000001+09'::timestamptz,
                'infinity'::timestamptz, '-infinity'::timestamptz",
        &[],
    );
    send_bind_binary(&mut client, "", "timestamp_binary_limits", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1114, 1114, 1114, 1184, 1184, 1184]);
    assert_eq!(formats, vec![1, 1, 1, 1, 1, 1]);
    assert_eq!(
        rows,
        vec![vec![
            1_i64.to_be_bytes().to_vec(),
            i64::MAX.to_be_bytes().to_vec(),
            i64::MIN.to_be_bytes().to_vec(),
            1_i64.to_be_bytes().to_vec(),
            i64::MAX.to_be_bytes().to_vec(),
            i64::MIN.to_be_bytes().to_vec(),
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn prepared_timestamp_and_timestamptz_paths_keep_distinct_oids() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "timestamp_cast_identity",
        "SELECT $1::timestamp without time zone, $2::timestamp with time zone",
        &[0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "timestamp_cast_identity");
    assert_eq!(read_parameter_description(&mut client), vec![1114, 1184]);
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'T');
    assert_eq!(parse_row_description_oids(&payload), vec![1114, 1184]);
    send_bind_binary(
        &mut client,
        "",
        "timestamp_cast_identity",
        &[
            (1, 1_234_567_i64.to_be_bytes().to_vec()),
            (1, 7_654_321_i64.to_be_bytes().to_vec()),
        ],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1114, 1184]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(
        rows,
        vec![vec![
            1_234_567_i64.to_be_bytes().to_vec(),
            7_654_321_i64.to_be_bytes().to_vec()
        ]]
    );

    send_query(
        &mut client,
        "CREATE TABLE timestamp_identity_probe (
            id TEXT PRIMARY KEY,
            naive_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            aware_at TIMESTAMP WITH TIME ZONE NOT NULL
        )",
    );
    read_query_rows(&mut client);
    send_parse(
        &mut client,
        "timestamp_target_insert",
        "INSERT INTO timestamp_identity_probe (id, naive_at, aware_at) VALUES ($1, $2, $3)",
        &[0, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "timestamp_target_insert");
    assert_eq!(
        read_parameter_description(&mut client),
        vec![25, 1114, 1184]
    );
    assert_eq!(read_message(&mut client).0, b'n');
    send_bind_binary(
        &mut client,
        "",
        "timestamp_target_insert",
        &[
            (1, b"row".to_vec()),
            (1, 10_i64.to_be_bytes().to_vec()),
            (1, 20_i64.to_be_bytes().to_vec()),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "timestamp_predicate_update",
        "UPDATE timestamp_identity_probe
         SET naive_at = $1, aware_at = $2
         WHERE id = $3 AND naive_at <= $4 AND aware_at <= $5",
        &[0, 0, 0, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "timestamp_predicate_update");
    assert_eq!(
        read_parameter_description(&mut client),
        vec![1114, 1184, 25, 1114, 1184]
    );
    assert_eq!(read_message(&mut client).0, b'n');
    send_sync(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "timestamp_column_results",
        "SELECT naive_at, aware_at FROM timestamp_identity_probe WHERE id = 'row'",
        &[],
    );
    send_bind_binary(&mut client, "", "timestamp_column_results", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1114, 1184]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(
        rows,
        vec![vec![
            10_i64.to_be_bytes().to_vec(),
            20_i64.to_be_bytes().to_vec()
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn copy_from_stdin_batches_total_payload_past_connection_memory_limit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            max_request_bytes: 128 * 1024,
            per_connection_memory_limit: 512 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE copy_large (id TEXT PRIMARY KEY, payload TEXT);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "COPY copy_large FROM STDIN CSV;");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G');
    assert_eq!(payload[0], 0);

    let filler = "x".repeat(200);
    for chunk_start in (0..3_000).step_by(250) {
        let mut payload = String::new();
        for idx in chunk_start..(chunk_start + 250).min(3_000) {
            payload.push_str(&format!("r{idx},{filler}\n"));
        }
        assert!(payload.len() < 128 * 1024);
        send_message(&mut client, b'd', payload.as_bytes());
    }
    send_message(&mut client, b'c', &[]);
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'I');
    assert!(tags.contains(&b'C'));

    send_query(&mut client, "SELECT COUNT(*) FROM copy_large;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["3000".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn copy_from_stdin_keeps_one_local_rls_identity_boundary_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            query_timeout: Duration::from_secs(300),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    let backend_key = read_backend_key_until_ready(&mut client);

    for sql in [
        "CREATE SEQUENCE copy_identity_seq;",
        "CREATE FUNCTION copy_identity_default() RETURNS text AS $$
         BEGIN
             IF lastval() <= 10000 THEN
                 RETURN set_config('app.user_id', 'alice', true);
             END IF;
             RETURN current_setting('app.user_id', true);
         END
         $$ LANGUAGE plpgsql;",
        "CREATE TABLE copy_private_docs (
            id TEXT PRIMARY KEY,
            sequence_value BIGINT DEFAULT nextval('copy_identity_seq'),
            owner_id TEXT DEFAULT copy_identity_default()
        );",
        "ALTER TABLE copy_private_docs ENABLE ROW LEVEL SECURITY;",
        "ALTER TABLE copy_private_docs FORCE ROW LEVEL SECURITY;",
        "CREATE POLICY copy_private_docs_policy ON copy_private_docs
         USING (owner_id = current_setting('app.user_id', true))
         WITH CHECK (owner_id = current_setting('app.user_id', true));",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }

    send_query(&mut client, "SELECT setval('copy_identity_seq', 1, false);");
    read_query_rows(&mut client);
    send_query(&mut client, "COPY copy_private_docs (id) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    let mut rows = String::new();
    for idx in 0..10_001 {
        rows.push_str(&format!("doc-{idx}\n"));
    }
    send_message(&mut client, b'd', rows.as_bytes());
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.is_empty(), "unexpected COPY error: {error}");

    send_query(
        &mut client,
        "SELECT current_setting('app.user_id', true), count(*) FROM copy_private_docs;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![String::new(), "0".to_string()]]
    );

    send_query(&mut client, "SELECT setval('copy_identity_seq', 1, false);");
    read_query_rows(&mut client);
    send_query(&mut client, "COPY copy_private_docs (id) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"doc-0\n");
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("duplicate") || error.contains("primary key"));
    assert_eq!(status, b'I');
    send_query(&mut client, "SELECT current_setting('app.user_id', true);");
    assert_eq!(read_query_rows(&mut client), vec![vec![String::new()]]);

    send_query(&mut client, "SELECT setval('copy_identity_seq', 1, false);");
    read_query_rows(&mut client);
    send_query(&mut client, "COPY copy_private_docs (id) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"canceled-row\n");
    server.request_cancel_for_test(backend_key.0, backend_key.1);
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("57014"));
    assert_eq!(status, b'I');
    send_query(&mut client, "SELECT current_setting('app.user_id', true);");
    assert_eq!(read_query_rows(&mut client), vec![vec![String::new()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn copy_from_stdin_inside_transaction_uses_declared_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE COPY_ITEM (I_ID INT PRIMARY KEY, NAME TEXT);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "COPY copy_item (i_id, name) FROM STDIN CSV;");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G');
    assert_eq!(payload[0], 0);

    send_message(&mut client, b'd', b"1,Ada\n2,Grace\n");
    send_message(&mut client, b'c', &[]);
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'T');
    assert!(tags.contains(&b'C'));

    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(
        &mut client,
        "SELECT i_id, name FROM copy_item ORDER BY i_id;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![
            vec!["1".to_string(), "Ada".to_string()],
            vec!["2".to_string(), "Grace".to_string()],
        ]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn copy_to_stdout_csv_exports_rows_over_wire() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT);",
    );
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        "INSERT INTO patients (id, name, age) VALUES ('p1', 'Ada', 36), ('p2', 'John, Jr.', 45);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(
        &mut client,
        "COPY (SELECT id, name, age FROM patients ORDER BY id) TO STDOUT CSV;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec!["p1,Ada,36\n", "p2,\"John, Jr.\",45\n"]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn bytea_copy_csv_roundtrips_exact_bytes_over_wire() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE binary_copy (id TEXT PRIMARY KEY, payload BYTEA);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "COPY binary_copy FROM STDIN CSV;");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'G');
    assert_eq!(payload[0], 0);
    send_message(
        &mut client,
        b'd',
        b"nul-high,\\x00ff415c\nescape,\\001\\\\A\n",
    );
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');

    send_query(
        &mut client,
        "SELECT id, encode(payload, 'hex') FROM binary_copy ORDER BY id;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![
            vec!["escape".to_string(), "015c41".to_string()],
            vec!["nul-high".to_string(), "00ff415c".to_string()],
        ]
    );

    send_query(&mut client, "SET bytea_output = escape;");
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        "SELECT payload FROM binary_copy WHERE id = 'escape';",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["\\001\\\\A".to_string()]]
    );
    send_query(&mut client, "RESET bytea_output;");
    assert!(read_query_rows(&mut client).is_empty());

    send_query(
        &mut client,
        "COPY (SELECT id, payload FROM binary_copy ORDER BY id) TO STDOUT CSV;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec!["escape,\\x015c41\n", "nul-high,\\x00ff415c\n"]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn copy_binary_returns_clear_postgresql_shaped_error() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_query(&mut client, "COPY patients TO STDOUT BINARY;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("ERROR"));
    assert!(error.contains("XX000"));
    assert!(error.contains("COPY BINARY is not supported"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_protocol_supports_prepared_crud_and_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT);",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "insert_patient",
        "INSERT INTO patients (id, name, age) VALUES ($1, $2, $3)",
        &[25, 25, 23],
    );
    send_bind(&mut client, "", "insert_patient", &["p1", "John", "45"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "select_patient",
        "SELECT name, age FROM patients WHERE id = $1",
        &[25],
    );
    send_bind(&mut client, "", "select_patient", &["p1"]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["John".to_string(), "45".to_string()]]);

    send_parse(&mut client, "cast_param", "SELECT $1::int4", &[23]);
    send_bind(&mut client, "", "cast_param", &["42"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["42".to_string()]]);

    send_close(&mut client, b'P', "");
    send_close(&mut client, b'S', "select_patient");
    send_sync(&mut client);
    let (tags, status) = read_tags_until_ready(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(tags.iter().filter(|tag| **tag == b'3').count(), 2);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn parameter_oids_survive_parse_describe_bind_and_execute_with_schema_inference() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE parameter_oid_probe (
            id UUID PRIMARY KEY,
            quantity INT4,
            amount NUMERIC,
            label TEXT,
            active BOOL,
            created_at TIMESTAMPTZ
        )",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "inferred_insert",
        "INSERT INTO parameter_oid_probe VALUES ($1, $2, $3, $4, $5, $6)",
        &[0, 0, 0, 0, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "inferred_insert");
    assert_eq!(
        read_parameter_description(&mut client),
        vec![2950, 23, 1700, 25, 16, 1184]
    );
    assert_eq!(read_message(&mut client).0, b'n');

    send_bind(
        &mut client,
        "",
        "inferred_insert",
        &[
            "00000000-0000-0000-0000-000000000307",
            "42",
            "9007199254740993.01",
            "typed",
            "true",
            "2026-07-17 12:34:56+00",
        ],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "inferred_predicate",
        "SELECT label FROM parameter_oid_probe WHERE id = $1 AND quantity > $2",
        &[],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "inferred_predicate");
    assert_eq!(read_parameter_description(&mut client), vec![2950, 23]);
    assert_eq!(read_message(&mut client).0, b'T');
    let predicate_uuid = "00000000-0000-0000-0000-000000000307"
        .parse::<uuid::Uuid>()
        .unwrap();
    send_bind_binary(
        &mut client,
        "",
        "inferred_predicate",
        &[
            (1, predicate_uuid.as_bytes().to_vec()),
            (1, 40_i32.to_be_bytes().to_vec()),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["typed".to_string()]]
    );

    send_parse(
        &mut client,
        "mixed_parameters",
        "SELECT $1::text, $2 + 1, $3",
        &[25, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "mixed_parameters");
    assert_eq!(read_parameter_description(&mut client), vec![25, 23, 25]);
    assert_eq!(read_message(&mut client).0, b'T');
    send_sync(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "common_type_parameter",
        "SELECT COALESCE($1, 1::int4, 2::int8)",
        &[],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "common_type_parameter");
    assert_eq!(read_parameter_description(&mut client), vec![20]);
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'T');
    assert_eq!(parse_row_description_oids(&payload), vec![20]);
    send_bind(&mut client, "", "common_type_parameter", &["7"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(read_query_rows(&mut client), vec![vec!["7".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgvector_prepared_statements_infer_oids_and_use_native_binary_frames() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, "CREATE EXTENSION IF NOT EXISTS vector");
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE vector_oid_probe (
            id TEXT PRIMARY KEY,
            embedding VECTOR(3)
         );
         INSERT INTO vector_oid_probe VALUES ('row', '[1,2,3]')",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "vector_distance",
        "SELECT embedding, embedding <+> $1 AS l1
         FROM vector_oid_probe
         WHERE id = $2",
        &[0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "vector_distance");
    assert_eq!(read_parameter_description(&mut client), vec![380_200, 25]);
    assert_eq!(read_message(&mut client).0, b'T');

    let vector = vector_binary(&[1.0, 2.0, 3.0]);
    send_bind_binary(
        &mut client,
        "",
        "vector_distance",
        &[(1, vector.clone()), (1, b"row".to_vec())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![380_200, 701]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![vector, 0.0_f64.to_be_bytes().to_vec()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn cognee_grouped_having_infers_and_binds_count_parameter() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE cognee_having_edges (
            edge_id TEXT PRIMARY KEY,
            primary_id TEXT NOT NULL,
            nbr_id TEXT NOT NULL
         );
         INSERT INTO cognee_having_edges VALUES
            ('e1', 'p1', 'n1'),
            ('e2', 'p2', 'n1'),
            ('e3', 'p1', 'n2')",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "cognee_having",
        "WITH matching_neighbors AS (
            SELECT nbr_id AS id
            FROM (
                SELECT primary_id, nbr_id
                FROM cognee_having_edges
            ) sub
            GROUP BY nbr_id
            HAVING COUNT(DISTINCT lower(primary_id)) = $1
         )
         SELECT lhs.id
         FROM matching_neighbors lhs
         JOIN matching_neighbors rhs ON rhs.id = lhs.id
         ORDER BY lhs.id",
        &[0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "cognee_having");
    assert_eq!(read_parameter_description(&mut client), vec![20]);
    assert_eq!(read_message(&mut client).0, b'T');
    send_bind(&mut client, "", "cognee_having", &["2"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(read_query_rows(&mut client), vec![vec!["n1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn inferred_text_and_varchar_arrays_keep_exact_oids_and_wire_formats() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE array_oid_probe (
            id VARCHAR PRIMARY KEY,
            tags VARCHAR[],
            notes TEXT[]
        )",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "inferred_array_insert",
        "INSERT INTO array_oid_probe (id, tags, notes) VALUES ($1, $2, $3)",
        &[0, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "inferred_array_insert");
    assert_eq!(
        read_parameter_description(&mut client),
        vec![1043, 1015, 1009]
    );
    assert_eq!(read_message(&mut client).0, b'n');
    send_bind(
        &mut client,
        "",
        "inferred_array_insert",
        &["text-row", "{alpha,beta}", "{first,second}"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "binary_array_insert",
        "INSERT INTO array_oid_probe (id, tags, notes) VALUES ($1, $2, $3)",
        &[0, 0, 0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    let varchar_values = [b"gamma".to_vec(), b"delta".to_vec()];
    let text_values = [b"third".to_vec(), b"fourth".to_vec()];
    let varchar_array =
        binary_array_payload(1043, &[Some(&varchar_values[0]), Some(&varchar_values[1])]);
    let text_array =
        binary_array_payload(25, &[Some(&text_values[0]), None, Some(&text_values[1])]);
    let mut text_array_input = text_array.clone();
    text_array_input[4..8].copy_from_slice(&0_i32.to_be_bytes());
    send_bind_binary(
        &mut client,
        "",
        "binary_array_insert",
        &[
            (1, b"binary-row".to_vec()),
            (1, varchar_array.clone()),
            (1, text_array_input),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_bind_binary(
        &mut client,
        "",
        "binary_array_insert",
        &[
            (1, b"empty-row".to_vec()),
            (1, binary_array_payload(1043, &[])),
            (1, binary_array_payload(25, &[])),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "inferred_array_select",
        "SELECT tags, notes FROM array_oid_probe WHERE id = $1",
        &[0],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "inferred_array_select");
    assert_eq!(read_parameter_description(&mut client), vec![1043]);
    assert_eq!(read_message(&mut client).0, b'T');
    send_bind_binary(
        &mut client,
        "",
        "inferred_array_select",
        &[(1, b"binary-row".to_vec())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1015, 1009]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![varchar_array, text_array]]);

    send_query(
        &mut client,
        "SELECT tags, notes FROM array_oid_probe WHERE id = 'text-row'",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "{alpha,beta}".to_string(),
            "{first,second}".to_string()
        ]]
    );
    send_query(
        &mut client,
        "SELECT tags, notes FROM array_oid_probe WHERE id = 'empty-row'",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["{}".to_string(), "{}".to_string()]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn polymorphic_array_functions_keep_prepared_oids_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "polymorphic_arrays",
        "SELECT array_append($1::text[], $2),
                array_positions($1::text[], $2),
                cardinality($1::text[])",
        &[],
    );
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "polymorphic_arrays");
    assert_eq!(read_parameter_description(&mut client), vec![1009, 25]);
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'T');
    assert_eq!(parse_row_description_oids(&payload), vec![1009, 1007, 23]);

    send_bind(
        &mut client,
        "",
        "polymorphic_arrays",
        &["{alpha,beta,alpha}", "alpha"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "{alpha,beta,alpha,alpha}".to_string(),
            "{1,3}".to_string(),
            "3".to_string(),
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_count_star_bigint_returns_one_row_for_filtered_empty_input() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patient_portal_invites (
            id UUID PRIMARY KEY,
            token_hash TEXT,
            deleted_at TIMESTAMPTZ
        );",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "count_invites",
        "SELECT COUNT(*)::bigint
         FROM patient_portal_invites
         WHERE token_hash = $1
           AND deleted_at IS NULL",
        &[25],
    );
    send_bind(&mut client, "", "count_invites", &["missing"]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    // PostgreSQL COUNT and the explicit bigint cast both require int8 (OID 20).
    assert_eq!(oids, vec![20]);
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    send_query(
        &mut client,
        "INSERT INTO patient_portal_invites (id, token_hash, deleted_at)
         VALUES ('00000000-0000-0000-0000-000000000001', 'present', NULL);",
    );
    read_query_rows(&mut client);

    send_bind(&mut client, "", "count_invites", &["present"]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![20]);
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_empty_ungrouped_aggregates_keep_cardinality_oids_and_binary_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE empty_wire_aggregates (
            id UUID PRIMARY KEY,
            amount INT,
            enabled BOOLEAN,
            payload JSONB
        )",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "empty_aggregate_matrix",
        "SELECT COUNT(*), COUNT(amount), SUM(amount), AVG(amount),
                MIN(id), MAX(id), BOOL_AND(enabled), BOOL_OR(enabled),
                ARRAY_AGG(id), JSON_AGG(payload), JSONB_AGG(payload)
         FROM empty_wire_aggregates
         WHERE id = $1",
        &[2950],
    );
    let missing_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000999")
        .unwrap()
        .as_bytes()
        .to_vec();
    send_bind_binary(
        &mut client,
        "",
        "empty_aggregate_matrix",
        &[(1, missing_id)],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(
        oids,
        vec![20, 20, 20, 1700, 2950, 2950, 16, 16, 2951, 114, 3802]
    );
    assert_eq!(formats, vec![1; 11]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], 0_i64.to_be_bytes());
    assert_eq!(rows[0][1], 0_i64.to_be_bytes());
    assert!(rows[0][2..].iter().all(Vec::is_empty));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn result_column_oids_come_from_schema_and_expression_not_value_width() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE typed_cols (
            b BIGINT PRIMARY KEY,
            i INT,
            s SMALLINT,
            r REAL,
            d DOUBLE PRECISION,
            n NUMERIC,
            t TEXT
        );",
    );
    read_query_rows(&mut client);
    // Small values that all fit in int4 -> the old value-width heuristic would
    // (wrongly) report these as int4 (OID 23).
    send_query(
        &mut client,
        "INSERT INTO typed_cols (b, i, s, r, d, n, t)
         VALUES (5, 7, 3, 1.5, 2.5, 9, 'x'), (6, 8, 4, 2.5, 3.5, 10, 'y');",
    );
    read_query_rows(&mut client);

    // Column references report the declared schema type, regardless of value width.
    let oids = oids_for(&mut client, "SELECT b, i, s, r, d, n, t FROM typed_cols;");
    assert_eq!(oids, vec![20, 23, 21, 700, 701, 1700, 25]);

    // A lone bigint column with a small value is still int8 (the reported bug).
    let oids = oids_for(&mut client, "SELECT b FROM typed_cols WHERE b = 5;");
    assert_eq!(oids, vec![20]);

    // MAX/MIN preserve the argument type (bigint -> int8, even for small values).
    assert_eq!(
        oids_for(&mut client, "SELECT MAX(b) FROM typed_cols;"),
        vec![20]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT MIN(b) FROM typed_cols;"),
        vec![20]
    );
    // MAX/MIN over int stay int4.
    assert_eq!(
        oids_for(&mut client, "SELECT MAX(i) FROM typed_cols;"),
        vec![23]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT MIN(i) FROM typed_cols;"),
        vec![23]
    );

    // COUNT is always bigint.
    assert_eq!(
        oids_for(&mut client, "SELECT COUNT(*) FROM typed_cols;"),
        vec![20]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT COUNT(b) FROM typed_cols;"),
        vec![20]
    );

    // SUM widens: int -> bigint, bigint -> numeric, real/double -> float8.
    assert_eq!(
        oids_for(&mut client, "SELECT SUM(i) FROM typed_cols;"),
        vec![20]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT SUM(s) FROM typed_cols;"),
        vec![20]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT SUM(b) FROM typed_cols;"),
        vec![1700]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT SUM(d) FROM typed_cols;"),
        vec![701]
    );

    // AVG of an integer column is numeric; of a float column is float8.
    assert_eq!(
        oids_for(&mut client, "SELECT AVG(i) FROM typed_cols;"),
        vec![1700]
    );
    assert_eq!(
        oids_for(&mut client, "SELECT AVG(d) FROM typed_cols;"),
        vec![701]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

fn registry_declaration_name(name: &str) -> String {
    if name == "char" {
        "\"char\"".to_string()
    } else {
        name.to_string()
    }
}

#[test]
fn registry_planner_and_pgwire_oids_agree_exhaustively() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    let mut declarations = Vec::new();
    let mut projections = Vec::new();
    let mut expected_oids = Vec::new();
    for spec in PG_TYPE_REGISTRY.all().iter().filter(|spec| !spec.pseudo) {
        let scalar_column = format!("scalar_{}", spec.oid);
        declarations.push(format!(
            "{scalar_column} {}",
            registry_declaration_name(spec.name)
        ));
        projections.push(scalar_column);
        expected_oids.push(spec.oid);
        if let Some(array_oid) = spec.array_oid {
            let array_column = format!("array_{array_oid}");
            declarations.push(format!(
                "{array_column} {}[]",
                registry_declaration_name(spec.name)
            ));
            projections.push(array_column);
            expected_oids.push(array_oid);
        }
    }

    send_query(
        &mut client,
        &format!(
            "CREATE TABLE registry_wire_consistency (id INTEGER PRIMARY KEY, {})",
            declarations.join(", ")
        ),
    );
    read_query_rows(&mut client);
    assert_eq!(
        oids_for(
            &mut client,
            &format!(
                "SELECT {} FROM registry_wire_consistency LIMIT 0",
                projections.join(", ")
            )
        ),
        expected_oids
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

fn oids_for(client: &mut TcpStream, sql: &str) -> Vec<i32> {
    send_query(client, sql);
    read_query_rows_and_oids(client).1
}

// The same source column must report the same type OID no matter the query shape
// it is read through (direct, WHERE, wildcard, join, CTE, subquery, UNION) and no
// matter the runtime value. This is the core consistency guarantee.
#[test]
fn result_column_oids_are_consistent_across_query_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE shapes (
            big BIGINT PRIMARY KEY,
            num INT,
            label TEXT,
            flag BOOL,
            dbl DOUBLE PRECISION
        );",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE child (id BIGINT PRIMARY KEY, big_id BIGINT, note TEXT);",
    );
    read_query_rows(&mut client);
    // `big` and `num` hold values that all fit in int4 -> the old value-width
    // heuristic would have flip-flopped `big` between int8 and int4.
    send_query(
        &mut client,
        "INSERT INTO shapes (big, num, label, flag, dbl)
         VALUES (5, 7, 'hi', true, 1.5);",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "INSERT INTO child (id, big_id, note) VALUES (1, 5, 'n');",
    );
    read_query_rows(&mut client);

    // Each column's expected OID by its single declared schema type.
    for (col, expected) in [
        ("big", 20),
        ("num", 23),
        ("label", 25),
        ("flag", 16),
        ("dbl", 701),
    ] {
        // direct projection
        assert_eq!(
            oids_for(&mut client, &format!("SELECT {col} FROM shapes;")),
            vec![expected],
            "{col} direct"
        );
        // with a WHERE clause
        assert_eq!(
            oids_for(
                &mut client,
                &format!("SELECT {col} FROM shapes WHERE big = 5;")
            ),
            vec![expected],
            "{col} where"
        );
        // qualified column through a two-table join
        assert_eq!(
            oids_for(
                &mut client,
                &format!("SELECT s.{col} FROM shapes s JOIN child c ON c.big_id = s.big;")
            ),
            vec![expected],
            "{col} join"
        );
        // wrapped in a CTE
        assert_eq!(
            oids_for(
                &mut client,
                &format!("WITH w AS (SELECT {col} FROM shapes) SELECT {col} FROM w;")
            ),
            vec![expected],
            "{col} cte"
        );
        // read through a derived table / subquery
        assert_eq!(
            oids_for(
                &mut client,
                &format!("SELECT d.{col} FROM (SELECT {col} FROM shapes) d;")
            ),
            vec![expected],
            "{col} subquery"
        );
        // first branch of a UNION
        assert_eq!(
            oids_for(
                &mut client,
                &format!("SELECT {col} FROM shapes UNION SELECT {col} FROM shapes;")
            ),
            vec![expected],
            "{col} union"
        );
    }

    // SELECT * must type every expanded column from its source schema, in order,
    // both for a single table and across a join (note: `big` -> 20 even though the
    // value 5 fits in int4).
    assert_eq!(
        oids_for(&mut client, "SELECT * FROM shapes;"),
        vec![20, 23, 25, 16, 701],
        "wildcard single table"
    );
    assert_eq!(
        oids_for(
            &mut client,
            "SELECT * FROM shapes JOIN child ON child.big_id = shapes.big;"
        ),
        vec![20, 23, 25, 16, 701, 20, 20, 25],
        "wildcard join"
    );

    // The bigint column with a value that fits in int4 is int8 (20) in EVERY shape.
    for shape in [
        "SELECT big FROM shapes;",
        "SELECT big FROM shapes WHERE big = 5;",
        "SELECT s.big FROM shapes s JOIN child c ON c.big_id = s.big;",
        "WITH w AS (SELECT big FROM shapes) SELECT big FROM w;",
        "SELECT d.big FROM (SELECT big FROM shapes) d;",
        "SELECT big FROM shapes UNION SELECT big FROM shapes;",
    ] {
        assert_eq!(
            oids_for(&mut client, shape),
            vec![20],
            "bigint fits-int4 {shape}"
        );
    }

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

// A dynamic / schemaless column (no declared SQL type) has no stricter type to
// resolve, so it falls back to the value-INDEPENDENT default: integers are always
// int8 (20), never int4-by-value. The same kind of column must report the same
// OID whether the row's integer is tiny (fits int4) or huge (needs int8).
#[test]
fn dynamic_column_oid_is_value_independent() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Two schemaless collections: their `timestamp` field is an untyped
        // integer column (no declared schema type), so it exercises the wire
        // layer's value-independent fallback. One holds a value that fits in
        // int4, the other one that does not.
        let mut db = BicDb::open(dir.path()).unwrap();
        db.create_collection("dyn_small").unwrap();
        db.create_collection("dyn_large").unwrap();
        db.insert("dyn_small", Record::new("a").with_timestamp(5))
            .unwrap();
        db.insert("dyn_large", Record::new("b").with_timestamp(9_999_999_999))
            .unwrap();
        db.close().unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let small = oids_for(&mut client, "SELECT timestamp FROM dyn_small;");
    let large = oids_for(&mut client, "SELECT timestamp FROM dyn_large;");
    // The small value previously reported int4 (23) under the value-width
    // heuristic; it must now match the large value's int8 (20).
    assert_eq!(small, large, "dynamic column OID must not depend on value");
    assert_eq!(small, vec![20]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_with_select_describes_rows_before_execute() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE batched_background_migrations (
            id TEXT PRIMARY KEY,
            status INT,
            min_value INT,
            max_value INT
        );",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE batched_background_migration_jobs (
            id TEXT PRIMARY KEY,
            batched_background_migration_id TEXT
        );",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "INSERT INTO batched_background_migrations (id, status, min_value, max_value)
         VALUES ('affected', 0, 7, 7), ('has_job', 0, 7, 7);",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "INSERT INTO batched_background_migration_jobs (id, batched_background_migration_id)
         VALUES ('job-1', 'has_job');",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "cte_select",
        "WITH migrations AS (
            SELECT batched_background_migrations.*
            FROM batched_background_migrations
            WHERE batched_background_migrations.status = 0
         )
         SELECT m.*
         FROM migrations m
         LEFT JOIN batched_background_migration_jobs j
           ON m.id = j.batched_background_migration_id
         WHERE j.id IS NULL
           AND m.min_value IS NOT NULL
           AND m.min_value = m.max_value",
        &[],
    );
    send_bind(&mut client, "", "cte_select", &[]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids.len(), 4);
    assert_eq!(rows[0][0], "affected");

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_describes_and_executes_feature_flag_scalar_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE hub_feature_flags (
            org_id TEXT,
            hub_id TEXT,
            feature TEXT,
            is_enabled BOOLEAN
        );",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "INSERT INTO hub_feature_flags (org_id, hub_id, feature, is_enabled)
         VALUES ('org-1', 'hub-1', 'apps', false);",
    );
    read_query_rows(&mut client);

    let sql = "SELECT COALESCE((SELECT is_enabled FROM hub_feature_flags WHERE org_id = $1 AND hub_id = $2 AND feature::text = $3 LIMIT 1), true)";
    send_parse(&mut client, "feature_flag", sql, &[]);
    send_describe_statement(&mut client, "feature_flag");
    send_sync(&mut client);
    assert_eq!(read_query_rows_and_oids(&mut client).1, vec![16]);

    send_bind(
        &mut client,
        "disabled_flag",
        "feature_flag",
        &["org-1", "hub-1", "apps"],
    );
    send_describe_portal(&mut client, "disabled_flag");
    send_execute(&mut client, "disabled_flag");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![16]);
    assert_eq!(rows, vec![vec!["f".to_string()]]);

    send_bind(
        &mut client,
        "default_flag",
        "feature_flag",
        &["org-1", "hub-1", "missing"],
    );
    send_describe_portal(&mut client, "default_flag");
    send_execute(&mut client, "default_flag");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![16]);
    assert_eq!(rows, vec![vec!["t".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_call_describes_out_params_before_data_rows() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE PROCEDURE call_probe(observed OUT INTEGER)
         LANGUAGE plpgsql AS $$ BEGIN observed := 7; END $$;",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "call_probe_stmt",
        "CALL call_probe(NULL);",
        &[],
    );
    send_bind(&mut client, "", "call_probe_stmt", &[]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let mut tags = Vec::new();
    let mut rows = Vec::new();
    loop {
        let (tag, payload) = read_message(&mut client);
        if tag == b'Z' {
            break;
        }
        if tag == b'D' {
            rows.push(parse_data_row(&payload));
        }
        tags.push(tag);
    }
    assert_eq!(tags, vec![b'1', b'2', b'T', b'D', b'C']);
    assert_eq!(rows, vec![vec!["7".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_insert_returning_description_ignores_trailing_marginalia_comment() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        r#"CREATE TABLE "ar_internal_metadata" (
            "key" character varying NOT NULL PRIMARY KEY,
            "value" character varying,
            "created_at" timestamp(6) NOT NULL,
            "updated_at" timestamp(6) NOT NULL
        );"#,
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "insert_metadata",
        r#"INSERT INTO "ar_internal_metadata" ("key", "value", "created_at", "updated_at")
           VALUES ($1, $2, $3, $4)
           RETURNING "key" /*application:web,db_config_database:gitlabhq_development,db_config_name:main,line:/lib/gitlab/database/migrations/pg_backend_pid.rb:14:in `with_advisory_lock'*/"#,
        &[25, 25, 25, 25],
    );
    send_bind(
        &mut client,
        "",
        "insert_metadata",
        &[
            "environment",
            "development",
            "2026-06-21 08:38:04.303549",
            "2026-06-21 08:38:04.303550",
        ],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids.len(), 1);
    assert_eq!(rows, vec![vec!["environment".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn authenticated_pgwire_context_is_immutable_and_isolated_per_connection() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut setup = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE patients (id TEXT PRIMARY KEY, org_id TEXT, name TEXT);",
            "INSERT INTO patients (id, org_id, name) VALUES ('p1', 'org-a', 'Ada'), ('p2', 'org-b', 'Bea');",
            "ALTER TABLE patients ENABLE ROW LEVEL SECURITY;",
            "ALTER TABLE patients FORCE ROW LEVEL SECURITY;",
            "CREATE POLICY patients_select_policy ON patients FOR SELECT USING (coalesce(current_setting('carrier.current_tenant', true), '') <> '' AND org_id::text = current_setting('carrier.current_tenant', true));",
            "GRANT SELECT ON patients TO PUBLIC;",
        ] {
            setup.execute(sql).unwrap();
        }
    }
    bicdb_pgwire::create_user_with_identity(
        dir.path(),
        "alice",
        "alice-password",
        PgWireUserIdentity::new("alice-id", "org-a")
            .with_workspace_id("clinic-1")
            .with_roles(["clinician"])
            .with_scopes(["patients:read"]),
    )
    .unwrap();
    bicdb_pgwire::create_user_with_identity(
        dir.path(),
        "bob",
        "bob-password",
        PgWireUserIdentity::new("bob-id", "org-b")
            .with_workspace_id("clinic-2")
            .with_roles(["clinician"]),
    )
    .unwrap();
    // Catalogs created before trusted identities existed remain login-capable,
    // but receive no tenant authority until an operator binds one.
    bicdb_pgwire::create_user(dir.path(), "legacy", "legacy-password").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            require_auth: true,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let server_thread = thread::spawn(move || {
        bicdb_pgwire::serve_existing_listener(serving, listener).unwrap();
    });

    let mut alice = TcpStream::connect(address).unwrap();
    send_startup_version_with_options_for_user(
        &mut alice,
        196_608,
        "alice",
        true,
        &[("options", "-ccarrier.current_tenant=org-b")],
    );
    send_scram_exchange(&mut alice, "alice", "alice-password", "n,,", true);
    read_until_ready(&mut alice);

    send_query(
        &mut alice,
        "SELECT current_setting('carrier.current_user', true), current_trusted_tenant(), current_setting('carrier.current_workspace', true), current_setting('carrier.current_roles', true), current_setting('carrier.current_scopes', true);",
    );
    assert_eq!(
        read_query_rows(&mut alice),
        vec![vec![
            "alice-id".to_string(),
            "org-a".to_string(),
            "clinic-1".to_string(),
            "clinician".to_string(),
            "patients:read".to_string(),
        ]]
    );

    for attack in [
        "SET carrier.current_tenant = 'org-b';",
        "SELECT set_config('carrier.current_workspace', 'clinic-2', false);",
        "SET carrier.current_roles = 'platform_admin';",
    ] {
        send_query(&mut alice, attack);
        assert!(read_error_response(&mut alice).contains("protected session attribute"));
    }
    send_query(&mut alice, "BEGIN;");
    read_query_rows(&mut alice);
    send_query(&mut alice, "SET LOCAL carrier.current_tenant = 'other';");
    let (error, status) = read_error_response_with_status(&mut alice);
    assert!(error.contains("protected session attribute"));
    assert_eq!(status, b'E');
    send_query(&mut alice, "ROLLBACK;");
    read_query_rows(&mut alice);
    for attack in [
        "SELECT set_config('carrier.current_tenant', 'other', true);",
        "RESET carrier.current_tenant;",
    ] {
        send_query(&mut alice, attack);
        assert!(read_error_response(&mut alice).contains("protected session attribute"));
    }
    send_query(&mut alice, "RESET ALL;");
    read_query_rows(&mut alice);
    send_query(&mut alice, "DISCARD ALL;");
    read_query_rows(&mut alice);
    send_query(
        &mut alice,
        "SELECT current_trusted_tenant(), id FROM patients ORDER BY id;",
    );
    assert_eq!(
        read_query_rows(&mut alice),
        vec![vec!["org-a".to_string(), "p1".to_string()]]
    );

    send_query(
        &mut alice,
        "PREPARE attack AS SELECT set_config('carrier.current_tenant', 'tenant-b', false);",
    );
    read_query_rows(&mut alice);
    send_query(&mut alice, "EXECUTE attack;");
    assert!(read_error_response(&mut alice).contains("protected session attribute"));
    send_query(&mut alice, "DISCARD ALL;");
    read_query_rows(&mut alice);
    send_query(
        &mut alice,
        "SELECT current_trusted_tenant(), id FROM patients ORDER BY id;",
    );
    assert_eq!(
        read_query_rows(&mut alice),
        vec![vec!["org-a".to_string(), "p1".to_string()]]
    );

    send_parse(
        &mut alice,
        "select_patients",
        "SELECT id FROM patients ORDER BY id",
        &[],
    );
    send_bind(&mut alice, "", "select_patients", &[]);
    send_execute(&mut alice, "");
    send_sync(&mut alice);
    assert_eq!(read_query_rows(&mut alice), vec![vec!["p1".to_string()]]);

    let mut bob = TcpStream::connect(address).unwrap();
    startup_with_scram(&mut bob, "bob", "bob-password");
    read_until_ready(&mut bob);
    send_query(&mut bob, "SELECT id FROM patients ORDER BY id;");
    assert_eq!(read_query_rows(&mut bob), vec![vec!["p2".to_string()]]);
    send_parse(
        &mut bob,
        "select_patients",
        "SELECT id FROM patients ORDER BY id",
        &[],
    );
    send_bind(&mut bob, "", "select_patients", &[]);
    send_execute(&mut bob, "");
    send_sync(&mut bob);
    assert_eq!(read_query_rows(&mut bob), vec![vec!["p2".to_string()]]);

    let mut legacy = TcpStream::connect(address).unwrap();
    startup_with_scram(&mut legacy, "legacy", "legacy-password");
    read_until_ready(&mut legacy);
    send_query(
        &mut legacy,
        "SELECT current_trusted_tenant() IS NULL, count(*) FROM patients;",
    );
    assert_eq!(
        read_query_rows(&mut legacy),
        vec![vec!["t".to_string(), "0".to_string()]]
    );

    alice.write_all(b"X\0\0\0\x04").unwrap();
    bob.write_all(b"X\0\0\0\x04").unwrap();
    legacy.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap();
}

#[test]
fn pgwire_prepared_set_config_cannot_replace_host_identity() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            security_context: Some(
                SecurityContext::new("user-1", "clinic-a").with_roles(["staff"]),
            ),
            postgres_version_banner: true,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let server_thread = thread::spawn(move || {
        bicdb_pgwire::serve_existing_listener(serving, listener).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "carrier_prime",
        "SELECT
            set_config('carrier.current_roles', $1, false),
            set_config('carrier.current_tenant', $2, false),
            set_config('carrier.current_user', $3, false)",
        &[0, 0, 0],
    );
    send_bind(
        &mut client,
        "",
        "carrier_prime",
        &["org_admin,staff", "clinic-a", "user-1"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_error_response(&mut client).contains("protected session attribute"));

    send_query(
        &mut client,
        "SELECT set_config('bicdb.postgres_version_banner', 'on', false);",
    );
    assert!(read_error_response(&mut client).contains("protected session attribute"));

    send_query(
        &mut client,
        "SELECT current_setting('carrier.current_tenant', true), current_setting('carrier.current_roles', true), current_setting('carrier.current_user', true);",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "clinic-a".to_string(),
            "staff".to_string(),
            "user-1".to_string()
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap();
}

#[test]
fn pgwire_read_paths_use_committed_and_rolled_back_typed_settings() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "SET bicdb.vector_search = 'ann';",
        "SET bicdb.ef_search = 77;",
        "BEGIN;",
        "SET bicdb.vector_search = 'exact';",
        "SET bicdb.ef_search = 91;",
        "ROLLBACK;",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }

    send_query(&mut client, "SHOW bicdb.vector_search;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["ann".to_string()]]);

    send_parse(&mut client, "show_ef_search", "SHOW bicdb.ef_search", &[]);
    send_bind(&mut client, "", "show_ef_search", &[]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(read_query_rows(&mut client), vec![vec!["77".to_string()]]);

    send_query(
        &mut client,
        "COPY (SHOW bicdb.vector_search) TO STDOUT CSV;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec!["ann\n".to_string()]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_currval_and_lastval_survive_rollback_and_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "CREATE SEQUENCE pgwire_session_seq;",
        "SELECT nextval('pgwire_session_seq');",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(
        &mut client,
        "SELECT currval('pgwire_session_seq'), lastval();",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["1".to_string(), "1".to_string()]]
    );

    for sql in [
        "BEGIN;",
        "SAVEPOINT before_nextval;",
        "SELECT nextval('pgwire_session_seq');",
        "ROLLBACK TO SAVEPOINT before_nextval;",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(
        &mut client,
        "SELECT currval('pgwire_session_seq'), lastval();",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["2".to_string(), "2".to_string()]]
    );

    for sql in ["SELECT nextval('pgwire_session_seq');", "ROLLBACK;"] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(
        &mut client,
        "SELECT currval('pgwire_session_seq'), lastval();",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["3".to_string(), "3".to_string()]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_transaction_local_guc_does_not_leak_rls_identity() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "CREATE TABLE private_docs (id TEXT PRIMARY KEY, owner_id TEXT);",
        "INSERT INTO private_docs VALUES ('a', 'alice'), ('b', 'bob');",
        "ALTER TABLE private_docs ENABLE ROW LEVEL SECURITY;",
        "ALTER TABLE private_docs FORCE ROW LEVEL SECURITY;",
        "CREATE POLICY own_docs ON private_docs USING (owner_id = current_setting('app.user_id', true));",
        "BEGIN;",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_parse(
        &mut client,
        "set_local_identity",
        "SELECT set_config('app.user_id', $1, true)",
        &[0],
    );
    send_bind(&mut client, "", "set_local_identity", &["alice"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["alice".to_string()]]
    );

    send_query(&mut client, "SELECT id FROM private_docs ORDER BY id;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["a".to_string()]]);
    send_query(&mut client, "COMMIT;");
    read_query_rows(&mut client);
    send_query(&mut client, "SELECT id FROM private_docs ORDER BY id;");
    assert!(read_query_rows(&mut client).is_empty());

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_regular_guc_commits_and_rolls_back_with_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "SET app.mode = 'base';",
        "BEGIN;",
        "SET app.mode = 'committed';",
        "COMMIT;",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(&mut client, "SELECT current_setting('app.mode', true);");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["committed".to_string()]]
    );

    for sql in ["BEGIN;", "SET app.mode = 'rolled-back';", "ROLLBACK;"] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(&mut client, "SELECT current_setting('app.mode', true);");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["committed".to_string()]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_transaction_local_guc_rolls_back_to_savepoint() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "SET app.mode = 'base';",
        "BEGIN;",
        "SET LOCAL app.mode = 'before-savepoint';",
        "SAVEPOINT guc_mark;",
        "SET LOCAL app.mode = 'after-savepoint';",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(&mut client, "SELECT current_setting('app.mode', true);");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["after-savepoint".to_string()]]
    );

    send_query(&mut client, "ROLLBACK TO SAVEPOINT guc_mark;");
    read_query_rows(&mut client);
    send_query(&mut client, "SELECT current_setting('app.mode', true);");
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["before-savepoint".to_string()]]
    );

    send_query(&mut client, "COMMIT;");
    read_query_rows(&mut client);
    send_query(&mut client, "SELECT current_setting('app.mode', true);");
    assert_eq!(read_query_rows(&mut client), vec![vec!["base".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_savepoint_guc_snapshots_respect_connection_memory_limit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 256 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    let value = "x".repeat(32 * 1024);
    send_query(&mut client, &format!("SET app.large = '{value}';"));
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    for name in ["first", "second"] {
        send_query(&mut client, &format!("SAVEPOINT {name};"));
        assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    }
    send_query(&mut client, "SAVEPOINT rejected;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("connection memory estimate"));
    assert_eq!(status, b'E');
    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    client.write_all(b"X\0\0\0\x04").unwrap();

    let mut recovery = TcpStream::connect(address).unwrap();
    startup(&mut recovery);
    read_until_ready(&mut recovery);
    send_query(&mut recovery, "SELECT 1;");
    assert_eq!(read_query_rows(&mut recovery), vec![vec!["1".to_string()]]);
    recovery.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_begin_snapshot_crossing_memory_limit_is_rejected_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 128 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let value = "b".repeat(64 * 1024);
    send_query(&mut client, &format!("SET app.begin_memory = '{value}';"));
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "BEGIN;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert_eq!(status, b'E');

    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_copy_start_snapshot_crossing_memory_limit_is_rejected_before_copy_mode() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 128 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE copy_memory_limit (id TEXT PRIMARY KEY);",
    );
    read_query_rows(&mut client);
    let value = "c".repeat(64 * 1024);
    send_query(&mut client, &format!("SET app.copy_memory = '{value}';"));
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "COPY copy_memory_limit (id) FROM STDIN;");
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'E', "COPY mode started before memory preflight");
    let error = String::from_utf8_lossy(&payload).to_string();
    let (_, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert_eq!(status, b'I');

    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_set_local_restore_crossing_memory_limit_is_rejected_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            // Above the BEGIN snapshot, below the fourth local-restore growth.
            per_connection_memory_limit: 102_200,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let value = "l".repeat(8 * 1024);
    for index in 0..6 {
        send_query(&mut client, &format!("SET app.local_{index} = '{value}';"));
        assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    }
    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    let mut rejection = None;
    for index in 0..6 {
        send_query(
            &mut client,
            &format!("SET LOCAL app.local_{index} = 'request';"),
        );
        let (error, status) = read_error_response_with_status(&mut client);
        if !error.is_empty() {
            rejection = Some((error, status));
            break;
        }
        assert_eq!(status, b'T');
    }
    let (error, status) = rejection.expect("SET LOCAL must cross the memory cap");
    assert!(error.contains("connection memory estimate"), "{error}");
    assert_eq!(status, b'E');

    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_autocommit_guc_overage_reports_error_then_closes_without_ready() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = bicdb_sql::SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE guc_memory_source (id TEXT PRIMARY KEY, value TEXT)")
            .unwrap();
        let value = "g".repeat(64 * 1024);
        session
            .execute(&format!(
                "INSERT INTO guc_memory_source VALUES ('large', '{value}')"
            ))
            .unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 32 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT set_config('app.fatal_memory', (SELECT value FROM guc_memory_source WHERE id = 'large'), false);",
    );
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'E');
    let error = String::from_utf8_lossy(&payload);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert!(error.contains("FATAL"), "{error}");
    assert!(error.contains("53200"), "{error}");
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_currval_map_overage_reports_error_then_closes_without_ready() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = bicdb_sql::SqlSession::new(&mut db);
        let sequence = format!("currval_memory_seq_{}", "s".repeat(64 * 1024));
        session
            .execute(&format!("CREATE SEQUENCE {sequence}"))
            .unwrap();
        session
            .execute("CREATE TABLE currval_memory_source (name TEXT PRIMARY KEY)")
            .unwrap();
        session
            .execute(&format!(
                "INSERT INTO currval_memory_source VALUES ('{sequence}')"
            ))
            .unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 48 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT nextval((SELECT name FROM currval_memory_source));",
    );
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'E');
    let error = String::from_utf8_lossy(&payload);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert!(error.contains("FATAL"), "{error}");
    assert!(error.contains("53200"), "{error}");
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_portal_describe_persistent_guc_overage_closes_without_ready() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = bicdb_sql::SqlSession::new(&mut db);
        let value = "d".repeat(64 * 1024);
        session
            .execute(&format!(
                "CREATE PROCEDURE describe_memory_proc(seed IN TEXT, observed OUT TEXT)
                 LANGUAGE plpgsql AS $$
                 BEGIN
                     observed := set_config('app.describe_memory', '{value}', false);
                 END
                 $$"
            ))
            .unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 32 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "describe_memory_stmt",
        "CALL describe_memory_proc($1, NULL);",
        &[25],
    );
    send_bind(
        &mut client,
        "describe_memory_portal",
        "describe_memory_stmt",
        &["small"],
    );
    send_describe_portal(&mut client, "describe_memory_portal");

    assert_eq!(read_message(&mut client).0, b'1');
    assert_eq!(read_message(&mut client).0, b'2');
    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'E');
    let error = String::from_utf8_lossy(&payload);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert!(error.contains("FATAL"), "{error}");
    assert!(error.contains("53200"), "{error}");
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_copy_done_persistent_guc_overage_closes_without_complete_or_ready() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = bicdb_sql::SqlSession::new(&mut db);
        let value = "c".repeat(64 * 1024);
        session
            .execute(&format!(
                "CREATE TABLE copy_done_memory (
                    id TEXT PRIMARY KEY,
                    installed TEXT DEFAULT set_config('app.copy_done_memory', '{value}', false)
                )"
            ))
            .unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            per_connection_memory_limit: 32 * 1024,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "COPY copy_done_memory (id) FROM STDIN;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"row-1\n");
    send_message(&mut client, b'c', &[]);

    let (tag, payload) = read_message(&mut client);
    assert_eq!(tag, b'E');
    let error = String::from_utf8_lossy(&payload);
    assert!(error.contains("connection memory estimate"), "{error}");
    assert!(error.contains("FATAL"), "{error}");
    assert!(error.contains("53200"), "{error}");
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn extended_cursor_fetch_streams_past_max_result_rows() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            max_result_rows: 3,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE cursor_rows (id TEXT PRIMARY KEY, n INT);",
    );
    read_query_rows(&mut client);
    for idx in 0..8 {
        send_query(
            &mut client,
            &format!("INSERT INTO cursor_rows (id, n) VALUES ('r{idx}', {idx});"),
        );
        read_query_rows(&mut client);
    }

    send_parse(
        &mut client,
        "cursor_select",
        "SELECT id FROM cursor_rows ORDER BY id",
        &[],
    );
    send_bind(&mut client, "cursor_portal", "cursor_select", &[]);
    send_describe_portal(&mut client, "cursor_portal");
    send_execute_max(&mut client, "cursor_portal", 3);
    assert_eq!(
        read_execute_rows(&mut client),
        (
            vec![
                vec!["r0".to_string()],
                vec!["r1".to_string()],
                vec!["r2".to_string()]
            ],
            true
        )
    );
    send_execute_max(&mut client, "cursor_portal", 3);
    assert_eq!(read_execute_rows(&mut client).0.len(), 3);
    send_execute_max(&mut client, "cursor_portal", 10);
    let (last_rows, suspended) = read_execute_rows(&mut client);
    assert_eq!(
        last_rows,
        vec![vec!["r6".to_string()], vec!["r7".to_string()]]
    );
    assert!(!suspended);
    send_sync(&mut client);
    read_until_ready(&mut client);

    let stats = server.stats_snapshot();
    assert_eq!(stats.rows_streamed, 8);
    assert!(stats.bytes_streamed > 0);
    assert_eq!(stats.cursor_count, 0);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn copy_to_stdout_streams_past_max_result_rows() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            max_result_rows: 3,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE copy_rows (id TEXT PRIMARY KEY, n INT);",
    );
    read_query_rows(&mut client);
    for idx in 0..8 {
        send_query(
            &mut client,
            &format!("INSERT INTO copy_rows (id, n) VALUES ('r{idx}', {idx});"),
        );
        read_query_rows(&mut client);
    }

    send_query(
        &mut client,
        "COPY (SELECT id FROM copy_rows ORDER BY id) TO STDOUT CSV;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows.len(), 8);
    assert_eq!(rows[0], "r0\n");
    assert_eq!(server.stats_snapshot().rows_streamed, 8);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn disconnect_during_cursor_stream_releases_resources() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            max_result_rows: 3,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE drop_cursor_rows (id TEXT PRIMARY KEY, n INT);",
    );
    read_query_rows(&mut client);
    for idx in 0..8 {
        send_query(
            &mut client,
            &format!("INSERT INTO drop_cursor_rows (id, n) VALUES ('r{idx}', {idx});"),
        );
        read_query_rows(&mut client);
    }

    send_parse(
        &mut client,
        "drop_cursor_select",
        "SELECT id FROM drop_cursor_rows ORDER BY id",
        &[],
    );
    send_bind(&mut client, "drop_cursor_portal", "drop_cursor_select", &[]);
    send_execute_max(&mut client, "drop_cursor_portal", 2);
    assert!(read_execute_rows(&mut client).1);
    assert_eq!(server.stats_snapshot().cursor_count, 1);
    drop(client);

    for _ in 0..100 {
        if server.stats_snapshot().cursor_count == 0 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(server.stats_snapshot().cursor_count, 0);
    assert_eq!(server.stats_snapshot().cursor_memory_bytes, 0);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn suspended_cursor_does_not_starve_short_query() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            max_result_rows: 3,
            max_active_queries: 1,
            max_active_reads: 1,
            max_queued_queries: 1,
            max_queued_reads: 1,
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut report = TcpStream::connect(address).unwrap();
    startup(&mut report);
    read_until_ready(&mut report);
    send_query(
        &mut report,
        "CREATE TABLE report_rows (id TEXT PRIMARY KEY, n INT);",
    );
    read_query_rows(&mut report);
    for idx in 0..8 {
        send_query(
            &mut report,
            &format!("INSERT INTO report_rows (id, n) VALUES ('r{idx}', {idx});"),
        );
        read_query_rows(&mut report);
    }

    send_parse(
        &mut report,
        "report_select",
        "SELECT id FROM report_rows ORDER BY id",
        &[],
    );
    send_bind(&mut report, "report_portal", "report_select", &[]);
    send_execute_max(&mut report, "report_portal", 2);
    assert!(read_execute_rows(&mut report).1);
    assert_eq!(server.stats_snapshot().cursor_count, 1);

    let mut oltp = TcpStream::connect(address).unwrap();
    startup(&mut oltp);
    read_until_ready(&mut oltp);
    send_query(&mut oltp, "SELECT 1;");
    assert_eq!(read_query_rows(&mut oltp), vec![vec!["1".to_string()]]);

    send_execute_max(&mut report, "report_portal", 0);
    assert!(!read_execute_rows(&mut report).1);
    send_sync(&mut report);
    read_until_ready(&mut report);

    report.write_all(b"X\0\0\0\x04").unwrap();
    oltp.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn extended_query_pipeline_runs_multiple_prepared_flows_before_sync() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE pipeline_patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "pipeline_insert",
        "INSERT INTO pipeline_patients (id, name) VALUES ($1, $2)",
        &[25, 25],
    );
    send_bind(
        &mut client,
        "insert_portal",
        "pipeline_insert",
        &["p1", "Pipelined"],
    );
    send_execute(&mut client, "insert_portal");
    send_parse(
        &mut client,
        "pipeline_select",
        "SELECT name FROM pipeline_patients WHERE id = $1",
        &[25],
    );
    send_bind(&mut client, "select_portal", "pipeline_select", &["p1"]);
    send_describe_portal(&mut client, "select_portal");
    send_execute(&mut client, "select_portal");
    send_sync(&mut client);

    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["Pipelined".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn cancel_request_interrupts_cancellable_query_and_connection_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            query_timeout: Duration::from_secs(5),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    let backend_key = read_backend_key_until_ready(&mut client);

    send_query(&mut client, "SELECT pg_sleep(2);");
    thread::sleep(Duration::from_millis(100));

    server.request_cancel_for_test(backend_key.0, backend_key.1);

    let (error, status) = read_error_response_with_status(&mut client);

    send_query(&mut client, "SELECT 1;");
    let recovery_rows = read_query_rows(&mut client);
    let stats = server.stats_snapshot();

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();

    assert_eq!(status, b'I');
    assert!(error.contains("57014"));
    assert!(error.contains("canceling statement due to user request"));
    assert_eq!(recovery_rows, vec![vec!["1".to_string()]]);
    assert_eq!(stats.canceled_queries, 1);
    assert_eq!(stats.last_cancel_connection_id, Some(1));
    assert_eq!(stats.last_cancel_reason.as_deref(), Some("cancel"));
    assert_eq!(stats.last_cancel_sqlstate.as_deref(), Some("57014"));
}

#[test]
fn cancel_request_interrupts_vector_ordering_and_short_query_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    db.create_collection("vector_cancel").unwrap();
    let records = (0..120_000)
        .map(|idx| {
            let x = (idx % 100) as f32 / 100.0;
            Record::new(format!("v{idx:05}")).with_vector(vec![x, 1.0 - x, 0.5])
        })
        .collect::<Vec<_>>();
    db.batch_insert("vector_cancel", records).unwrap();
    drop(db);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            query_timeout: Duration::from_secs(5),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    startup(&mut client);
    let backend_key = read_backend_key_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT id FROM vector_cancel ORDER BY embedding <=> '[1,0,0]' LIMIT 5;",
    );
    thread::sleep(Duration::from_millis(20));

    server.request_cancel_for_test(backend_key.0, backend_key.1);

    let (error, status) = read_error_response_with_status(&mut client);

    send_query(&mut client, "SELECT 1;");
    let recovery_rows = read_query_rows(&mut client);
    let stats = server.stats_snapshot();

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();

    assert_eq!(status, b'I');
    assert!(error.contains("57014"));
    assert_eq!(recovery_rows, vec![vec!["1".to_string()]]);
    assert!(stats.canceled_queries >= 1);
}

#[test]
fn cancel_inside_transaction_sets_failed_status_and_rolls_back_writes() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            query_timeout: Duration::from_secs(5),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    let backend_key = read_backend_key_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE cancel_tx (id TEXT PRIMARY KEY, value INT);",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(
        &mut client,
        "INSERT INTO cancel_tx (id, value) VALUES ('kept-out', 1);",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(&mut client, "SELECT pg_sleep(2);");
    thread::sleep(Duration::from_millis(100));
    server.request_cancel_for_test(backend_key.0, backend_key.1);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("57014"));

    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "SELECT COUNT(*) FROM cancel_tx;");
    let rows = read_query_rows(&mut client);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();

    assert_eq!(rows, vec![vec!["0".to_string()]]);
}

#[test]
fn extended_query_binary_parameters_and_results_cover_common_types() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let mut covered_oids = vec![
        21, 23, 20, 16, 700, 701, 25, 114, 3802, 2950, 17, 1114, 1184, 380_200, 1082, 1083, 1186,
        1700, 1043, 790, 1266, 18, 19, 142, 1042, 1560, 1562, 4072, 3614, 3615,
    ];
    covered_oids.extend(
        structured_binary_codec_cases()
            .into_iter()
            .map(|(oid, _)| oid),
    );
    let mut registered_oids = PG_TYPE_REGISTRY
        .all()
        .iter()
        .filter(|spec| spec.binary_codec().is_some())
        .map(|spec| spec.oid)
        .collect::<Vec<_>>();
    let mut covered_registered_oids = covered_oids
        .iter()
        .copied()
        .filter(|oid| {
            PG_TYPE_REGISTRY
                .by_oid(*oid)
                .is_some_and(|spec| spec.binary_codec().is_some())
        })
        .collect::<Vec<_>>();
    registered_oids.sort_unstable();
    covered_registered_oids.sort_unstable();
    assert_eq!(covered_registered_oids, registered_oids);

    send_parse(
        &mut client,
        "binary_common",
        "SELECT $1::int2, $2::int4, $3::int8, $4::bool, $5::float4, $6::float8, $7::text, $8::json, $9::jsonb, $10::uuid, $11::bytea, $12::timestamp, $13::timestamptz, $14::vector, $15::date, $16::time, $17::interval, $18::numeric, $19::varchar, $20::money, $21::timetz, $22::\"char\", $23::name, $24::xml, $25::bpchar, $26::bit, $27::varbit, $28::jsonpath",
        &[
            21, 23, 20, 16, 700, 701, 25, 114, 3802, 2950, 17, 1114, 1184, 380_200, 1082, 1083,
            1186, 1700, 1043, 790, 1266, 18, 19, 142, 1042, 1560, 1562, 4072,
        ],
    );
    let uuid = "550e8400-e29b-41d4-a716-446655440000"
        .parse::<uuid::Uuid>()
        .unwrap();
    send_bind_binary(
        &mut client,
        "",
        "binary_common",
        &[
            (1, 123_i16.to_be_bytes().to_vec()),
            (1, 456_i32.to_be_bytes().to_vec()),
            (1, 789_i64.to_be_bytes().to_vec()),
            (1, vec![1]),
            (1, 1.5_f32.to_be_bytes().to_vec()),
            (1, 2.25_f64.to_be_bytes().to_vec()),
            (1, b"hello".to_vec()),
            (1, br#"{"a":1}"#.to_vec()),
            (1, [vec![1], br#"{"b":2}"#.to_vec()].concat()),
            (1, uuid.as_bytes().to_vec()),
            (1, vec![0, 255]),
            (1, 42_i64.to_be_bytes().to_vec()),
            (1, 43_i64.to_be_bytes().to_vec()),
            (1, vector_binary(&[1.0, 2.0, 3.0])),
            (1, 8_767_i32.to_be_bytes().to_vec()),
            (1, 11_045_000_000_i64.to_be_bytes().to_vec()),
            (1, interval_binary(0, 2, 0)),
            (1, numeric_binary(2, 0, 0x0000, 2, &[12, 3000])),
            (1, b"world".to_vec()),
            (1, 12_345_i64.to_be_bytes().to_vec()),
            (1, timetz_binary(11_045_000_000, -7_200)),
            (1, vec![0xc3]),
            (1, vec![b'a'; 70]),
            (1, b"<value/>".to_vec()),
            (1, b"p".to_vec()),
            (1, bit_string_binary("1")),
            (1, bit_string_binary("101001")),
            (1, [vec![1], br#"$.value ? (@ > 2)"#.to_vec()].concat()),
        ],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (formats, rows, status) = read_binary_query_result(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(formats, vec![1; 28]);
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row[0], 123_i16.to_be_bytes().to_vec());
    assert_eq!(row[1], 456_i32.to_be_bytes().to_vec());
    assert_eq!(row[2], 789_i64.to_be_bytes().to_vec());
    assert_eq!(row[3], vec![1]);
    assert_eq!(row[4], 1.5_f32.to_be_bytes().to_vec());
    assert_eq!(row[5], 2.25_f64.to_be_bytes().to_vec());
    assert_eq!(row[6], b"hello".to_vec());
    assert_eq!(row[7], br#"{"a":1}"#.to_vec());
    assert_eq!(row[8], [vec![1], br#"{"b": 2}"#.to_vec()].concat());
    assert_eq!(row[9], uuid.as_bytes().to_vec());
    assert_eq!(row[10], vec![0, 255]);
    assert_eq!(row[11], 42_i64.to_be_bytes().to_vec());
    assert_eq!(row[12], 43_i64.to_be_bytes().to_vec());
    assert_eq!(row[13], vector_binary(&[1.0, 2.0, 3.0]));
    assert_eq!(row[14], 8_767_i32.to_be_bytes().to_vec());
    assert_eq!(row[15], 11_045_000_000_i64.to_be_bytes().to_vec());
    assert_eq!(row[16], interval_binary(0, 2, 0));
    assert_eq!(row[17], numeric_binary(2, 0, 0x0000, 2, &[12, 3000]));
    assert_eq!(row[18], b"world".to_vec());
    assert_eq!(row[19], 12_345_i64.to_be_bytes().to_vec());
    assert_eq!(row[20], timetz_binary(11_045_000_000, -7_200));
    assert_eq!(row[21], vec![0xc3]);
    assert_eq!(row[22], vec![b'a'; 63]);
    assert_eq!(row[23], b"<value/>".to_vec());
    assert_eq!(row[24], b"p".to_vec());
    assert_eq!(row[25], bit_string_binary("1"));
    assert_eq!(row[26], bit_string_binary("101001"));
    assert_eq!(
        row[27],
        [vec![1], br#"$."value"?(@ > 2)"#.to_vec()].concat()
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn xml_wire_binary_and_copy_paths_validate_and_canonicalize() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(&mut client, "xml_binary", "SELECT $1::xml", &[142]);
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "xml_binary");
    assert_eq!(read_parameter_description(&mut client), vec![142]);
    assert_eq!(read_message(&mut client).0, b'T');
    send_sync(&mut client);
    read_until_ready(&mut client);

    send_bind_binary(
        &mut client,
        "",
        "xml_binary",
        &[(
            1,
            br#"<?xml version="1.0" encoding="UTF-8"?><root><value>wire</value></root>"#.to_vec(),
        )],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![142]);
    assert_eq!(formats, vec![1]);
    assert_eq!(
        rows,
        vec![vec![b"<root><value>wire</value></root>".to_vec()]]
    );

    send_query(
        &mut client,
        "CREATE TABLE xml_copy_wire (id TEXT PRIMARY KEY, payload XML NOT NULL);",
    );
    assert!(read_query_rows(&mut client).is_empty());
    send_query(&mut client, "COPY xml_copy_wire (id, payload) FROM STDIN;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(
        &mut client,
        b'd',
        b"row\t<?xml version=\"1.0\" encoding=\"LATIN1\"?><root>copied</root>\n",
    );
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');
    send_query(
        &mut client,
        "COPY (SELECT id, payload FROM xml_copy_wire) TO STDOUT;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec!["row\t<root>copied</root>\n"]);

    send_query(&mut client, "COPY xml_copy_wire (id, payload) FROM STDIN;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"invalid\t<root>\n");
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("2200N"), "{error}");

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_enum_parameters_and_results_use_persisted_oids() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TYPE wire_priority AS ENUM ('medium', 'low', 'high'); \
         CREATE TABLE wire_enum_rows (id TEXT PRIMARY KEY, status wire_priority, \
         history wire_priority[]);",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_priority';",
    );
    let oid_row = read_query_rows(&mut client).remove(0);
    let type_oid = oid_row[0].parse::<i32>().unwrap();
    let array_oid = oid_row[1].parse::<i32>().unwrap();

    send_parse(
        &mut client,
        "enum_binary",
        "INSERT INTO wire_enum_rows (id, status, history) \
         VALUES ('row', $1::wire_priority, $2::wire_priority[]) \
         RETURNING status, history",
        &[0, 0],
    );
    let low = b"low".to_vec();
    let high = b"high".to_vec();
    let array = binary_array_payload(type_oid, &[Some(&low), None, Some(&high)]);
    send_bind_binary(
        &mut client,
        "",
        "enum_binary",
        &[(1, low.clone()), (1, array.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![low, array]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_domain_parameters_and_results_use_base_binary_codec() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE DOMAIN wire_positive AS INT4 CHECK (VALUE > 0); \
         CREATE TABLE wire_domain_rows (id TEXT PRIMARY KEY, amount wire_positive, \
         history wire_positive[]);",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_positive';",
    );
    let oid_row = read_query_rows(&mut client).remove(0);
    let type_oid = oid_row[0].parse::<i32>().unwrap();
    let array_oid = oid_row[1].parse::<i32>().unwrap();

    send_parse(
        &mut client,
        "domain_binary",
        "INSERT INTO wire_domain_rows (id, amount, history) \
         VALUES ('row', $1::wire_positive, $2::wire_positive[]) \
         RETURNING amount, history",
        &[0, 0],
    );
    let forty_two = 42_i32.to_be_bytes().to_vec();
    let seven = 7_i32.to_be_bytes().to_vec();
    let array = binary_array_payload(type_oid, &[Some(&forty_two), None, Some(&seven)]);
    send_bind_binary(
        &mut client,
        "",
        "domain_binary",
        &[(1, forty_two.clone()), (1, array.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![forty_two, array]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_registered_base_types_keep_custom_oids_and_binary_codecs() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TYPE wire_exact_text; \
         CREATE FUNCTION wire_exact_text_in(cstring) RETURNS wire_exact_text AS 'textin' LANGUAGE internal IMMUTABLE STRICT; \
         CREATE FUNCTION wire_exact_text_out(wire_exact_text) RETURNS cstring AS 'textout' LANGUAGE internal IMMUTABLE STRICT; \
         CREATE FUNCTION wire_exact_text_recv(internal) RETURNS wire_exact_text AS 'textrecv' LANGUAGE internal IMMUTABLE STRICT; \
         CREATE FUNCTION wire_exact_text_send(wire_exact_text) RETURNS bytea AS 'textsend' LANGUAGE internal IMMUTABLE STRICT; \
         CREATE TYPE wire_exact_text (INPUT=wire_exact_text_in, OUTPUT=wire_exact_text_out, \
           RECEIVE=wire_exact_text_recv, SEND=wire_exact_text_send, INTERNALLENGTH=variable, \
           ALIGNMENT=int4, STORAGE=extended, CATEGORY='S', DELIMITER=';', COLLATABLE=true); \
         CREATE TABLE wire_base_rows (id TEXT PRIMARY KEY, value wire_exact_text, \
           history wire_exact_text[]);",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_exact_text';",
    );
    let oid_row = read_query_rows(&mut client).remove(0);
    let type_oid = oid_row[0].parse::<i32>().unwrap();
    let array_oid = oid_row[1].parse::<i32>().unwrap();

    send_parse(
        &mut client,
        "base_binary",
        "INSERT INTO wire_base_rows (id, value, history) \
         VALUES ('row', $1::wire_exact_text, $2::wire_exact_text[]) \
         RETURNING value, history",
        &[0, 0],
    );
    let alpha = b"alpha".to_vec();
    let beta = b"beta".to_vec();
    let array = binary_array_payload(type_oid, &[Some(&alpha), None, Some(&beta)]);
    send_bind_binary(
        &mut client,
        "",
        "base_binary",
        &[(1, alpha.clone()), (1, array.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![alpha, array]]);

    send_query(
        &mut client,
        "SELECT history FROM wire_base_rows WHERE id = 'row';",
    );
    let (text_rows, text_oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(text_oids, vec![array_oid]);
    assert_eq!(text_rows, vec![vec!["{alpha;NULL;beta}".to_string()]]);

    send_query(
        &mut client,
        "COPY wire_base_rows (id, value, history) FROM STDIN;",
    );
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"copy\tomega\t{x;y}\n");
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');
    send_query(
        &mut client,
        "COPY (SELECT id, value, history FROM wire_base_rows WHERE id = 'copy') TO STDOUT;",
    );
    let (copy_rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(copy_rows, vec!["copy\tomega\t{x;y}\n"]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_table_rows_use_composite_oid_and_binary_codec() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE wire_composite_rows (id INT PRIMARY KEY, name TEXT NOT NULL); \
         INSERT INTO wire_composite_rows VALUES (1, 'Ada');",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid FROM pg_type WHERE typname = 'wire_composite_rows' AND typtype = 'c';",
    );
    let type_oid = read_query_rows(&mut client)[0][0].parse::<i32>().unwrap();

    send_parse(
        &mut client,
        "composite_binary",
        "SELECT p FROM wire_composite_rows p",
        &[],
    );
    send_bind_binary(&mut client, "", "composite_binary", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let mut expected = Vec::new();
    expected.extend_from_slice(&2_i32.to_be_bytes());
    expected.extend_from_slice(&23_i32.to_be_bytes());
    expected.extend_from_slice(&4_i32.to_be_bytes());
    expected.extend_from_slice(&1_i32.to_be_bytes());
    expected.extend_from_slice(&25_i32.to_be_bytes());
    expected.extend_from_slice(&3_i32.to_be_bytes());
    expected.extend_from_slice(b"Ada");
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![expected]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn table_row_type_oids_and_codecs_survive_rename_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let (type_oid, array_oid) = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_path = path.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            bicdb_pgwire::handle_client(stream, server_path).unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        startup(&mut client);
        read_until_ready(&mut client);
        send_query(
            &mut client,
            "CREATE TABLE wire_row_source (id INT, name TEXT); \
             CREATE TABLE wire_row_holder (\
                key INT PRIMARY KEY, \
                item wire_row_source, \
                items wire_row_source[]\
             ); \
             INSERT INTO wire_row_holder VALUES (\
                1, \
                ROW(7, 'Ada')::wire_row_source, \
                ARRAY[ROW(8, 'Bob')::wire_row_source]\
             );",
        );
        read_until_ready(&mut client);
        send_query(
            &mut client,
            "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_row_source';",
        );
        let rows = read_query_rows(&mut client);
        let type_oid = rows[0][0].parse::<i32>().unwrap();
        let array_oid = rows[0][1].parse::<i32>().unwrap();
        send_query(
            &mut client,
            "ALTER TABLE wire_row_source RENAME COLUMN name TO label; \
             ALTER TABLE wire_row_source RENAME TO wire_row_renamed;",
        );
        read_until_ready(&mut client);
        client.write_all(b"X\0\0\0\x04").unwrap();
        server.join().unwrap();
        (type_oid, array_oid)
    };

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_row_renamed';",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![type_oid.to_string(), array_oid.to_string()]]
    );
    send_query(
        &mut client,
        "SELECT item, items FROM wire_row_holder WHERE key = 1;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["(7,Ada)".to_string(), "{\"(8,Bob)\"}".to_string()]]
    );

    let composite = |id: i32, name: &str| {
        let mut output = Vec::new();
        output.extend_from_slice(&2_i32.to_be_bytes());
        output.extend_from_slice(&23_i32.to_be_bytes());
        output.extend_from_slice(&4_i32.to_be_bytes());
        output.extend_from_slice(&id.to_be_bytes());
        output.extend_from_slice(&25_i32.to_be_bytes());
        output.extend_from_slice(&(name.len() as i32).to_be_bytes());
        output.extend_from_slice(name.as_bytes());
        output
    };
    let ada = composite(7, "Ada");
    let bob = composite(8, "Bob");
    let items = binary_array_payload(type_oid, &[Some(&bob)]);
    send_parse(
        &mut client,
        "renamed_table_row_binary",
        "SELECT item, items FROM wire_row_holder WHERE key = 1",
        &[],
    );
    send_bind_binary(&mut client, "", "renamed_table_row_binary", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![ada, items]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn named_composites_and_arrays_round_trip_text_and_binary_codecs() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TYPE wire_named_contact AS (id integer, name text); \
         CREATE TABLE wire_named_values (\
            id integer PRIMARY KEY, \
            contact wire_named_contact, \
            contacts wire_named_contact[]\
         ); \
         INSERT INTO wire_named_values VALUES (\
            1, \
            ROW(1, 'Ada')::wire_named_contact, \
            ARRAY[\
                ROW(1, 'Ada')::wire_named_contact, \
                ROW(2, 'Bob')::wire_named_contact\
            ]\
         );",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'wire_named_contact';",
    );
    let type_rows = read_query_rows(&mut client);
    let type_oid = type_rows[0][0].parse::<i32>().unwrap();
    let array_oid = type_rows[0][1].parse::<i32>().unwrap();

    send_query(
        &mut client,
        "SELECT contact, contacts FROM wire_named_values WHERE id = 1;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "(1,Ada)".to_string(),
            "{\"(1,Ada)\",\"(2,Bob)\"}".to_string(),
        ]]
    );

    let composite = |id: i32, name: &str| {
        let mut output = Vec::new();
        output.extend_from_slice(&2_i32.to_be_bytes());
        output.extend_from_slice(&23_i32.to_be_bytes());
        output.extend_from_slice(&4_i32.to_be_bytes());
        output.extend_from_slice(&id.to_be_bytes());
        output.extend_from_slice(&25_i32.to_be_bytes());
        output.extend_from_slice(&(name.len() as i32).to_be_bytes());
        output.extend_from_slice(name.as_bytes());
        output
    };
    let ada = composite(1, "Ada");
    let bob = composite(2, "Bob");
    let contacts = binary_array_payload(type_oid, &[Some(&ada), Some(&bob)]);

    send_parse(
        &mut client,
        "named_composite_binary",
        "SELECT contact, contacts FROM wire_named_values WHERE id = 1",
        &[],
    );
    send_bind_binary(&mut client, "", "named_composite_binary", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![ada.clone(), contacts.clone()]]);

    send_parse(
        &mut client,
        "named_composite_parameters",
        "SELECT $1::wire_named_contact, $2::wire_named_contact[]",
        &[type_oid, array_oid],
    );
    send_bind_binary(
        &mut client,
        "",
        "named_composite_parameters",
        &[(1, ada.clone()), (1, contacts.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![type_oid, array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![ada, contacts]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn enum_and_domain_arrays_round_trip_text_and_binary_codecs() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TYPE wire_array_mood AS ENUM ('calm', 'busy'); \
         CREATE DOMAIN wire_array_code AS integer CHECK (VALUE > 0); \
         CREATE TABLE wire_user_type_arrays (\
            id integer PRIMARY KEY, \
            moods wire_array_mood[], \
            codes wire_array_code[]\
         ); \
         INSERT INTO wire_user_type_arrays VALUES (\
            1, \
            ARRAY['calm'::wire_array_mood, NULL, 'busy'::wire_array_mood], \
            ARRAY[1::wire_array_code, NULL, 2::wire_array_code]\
         );",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT typname, oid, typarray FROM pg_type \
         WHERE typname IN ('wire_array_mood', 'wire_array_code') ORDER BY typname;",
    );
    let type_rows = read_query_rows(&mut client);
    let code_oid = type_rows[0][1].parse::<i32>().unwrap();
    let code_array_oid = type_rows[0][2].parse::<i32>().unwrap();
    let mood_oid = type_rows[1][1].parse::<i32>().unwrap();
    let mood_array_oid = type_rows[1][2].parse::<i32>().unwrap();

    send_query(
        &mut client,
        "SELECT moods, codes FROM wire_user_type_arrays WHERE id = 1;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "{calm,NULL,busy}".to_string(),
            "{1,NULL,2}".to_string(),
        ]]
    );

    let calm = b"calm".to_vec();
    let busy = b"busy".to_vec();
    let moods = binary_array_payload(mood_oid, &[Some(&calm), None, Some(&busy)]);
    let one = 1_i32.to_be_bytes().to_vec();
    let two = 2_i32.to_be_bytes().to_vec();
    let codes = binary_array_payload(code_oid, &[Some(&one), None, Some(&two)]);
    send_parse(
        &mut client,
        "user_type_array_parameters",
        "SELECT $1::wire_array_mood[], $2::wire_array_code[]",
        &[mood_array_oid, code_array_oid],
    );
    send_bind_binary(
        &mut client,
        "",
        "user_type_array_parameters",
        &[(1, moods.clone()), (1, codes.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![mood_array_oid, code_array_oid]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![moods, codes]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_fixed_bit_enforces_typmod_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE fixed_bit_wire (id INT PRIMARY KEY, flags BIT(5));",
    );
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "insert_fixed_bit",
        "INSERT INTO fixed_bit_wire VALUES ($1, $2)",
        &[23, 1560],
    );
    send_bind_binary(
        &mut client,
        "",
        "insert_fixed_bit",
        &[
            (1, 1_i32.to_be_bytes().to_vec()),
            (1, bit_string_binary("10101")),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "select_fixed_bit",
        "SELECT flags FROM fixed_bit_wire WHERE id = 1",
        &[],
    );
    send_bind_binary(&mut client, "", "select_fixed_bit", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1560]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![bit_string_binary("10101")]]);

    for (statement, bits) in [("insert_short_bit", "101"), ("insert_long_bit", "101010")] {
        send_parse(
            &mut client,
            statement,
            "INSERT INTO fixed_bit_wire VALUES (2, $1)",
            &[1560],
        );
        send_bind_binary(
            &mut client,
            "",
            statement,
            &[(1, bit_string_binary(bits))],
            &[],
        );
        send_execute(&mut client, "");
        send_sync(&mut client);
        assert!(read_error_response(&mut client).contains("22026"));
    }

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_varbit_enforces_maximum_and_result_oids() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE varbit_wire (id INT PRIMARY KEY, flags VARBIT(5));",
    );
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "insert_varbit",
        "INSERT INTO varbit_wire VALUES ($1, $2)",
        &[23, 1562],
    );
    send_bind_binary(
        &mut client,
        "",
        "insert_varbit",
        &[
            (1, 1_i32.to_be_bytes().to_vec()),
            (1, bit_string_binary("101")),
        ],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "select_varbit",
        "SELECT flags, flags || B'1', ~flags FROM varbit_wire WHERE id = 1",
        &[],
    );
    send_bind_binary(&mut client, "", "select_varbit", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1562, 1562, 1560]);
    assert_eq!(formats, vec![1; 3]);
    assert_eq!(
        rows,
        vec![vec![
            bit_string_binary("101"),
            bit_string_binary("1011"),
            bit_string_binary("010"),
        ]]
    );

    send_parse(
        &mut client,
        "insert_long_varbit",
        "INSERT INTO varbit_wire VALUES (2, $1)",
        &[1562],
    );
    send_bind_binary(
        &mut client,
        "",
        "insert_long_varbit",
        &[(1, bit_string_binary("101010"))],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_error_response(&mut client).contains("22001"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_date_roundtrips_boundaries_and_rejects_invalid_days() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let bc_days = PgDate::from_postgres_text("0001-01-01 BC")
        .unwrap()
        .epoch_days()
        .unwrap();
    send_parse(
        &mut client,
        "binary_dates",
        "SELECT $1::date, $2::date, $3::date, $4::date",
        &[1082, 1082, 1082, 1082],
    );
    send_bind_binary(
        &mut client,
        "",
        "binary_dates",
        &[
            (1, 0_i32.to_be_bytes().to_vec()),
            (1, bc_days.to_be_bytes().to_vec()),
            (1, i32::MIN.to_be_bytes().to_vec()),
            (1, i32::MAX.to_be_bytes().to_vec()),
        ],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1082; 4]);
    assert_eq!(formats, vec![1; 4]);
    assert_eq!(
        rows,
        vec![vec![
            0_i32.to_be_bytes().to_vec(),
            bc_days.to_be_bytes().to_vec(),
            i32::MIN.to_be_bytes().to_vec(),
            i32::MAX.to_be_bytes().to_vec(),
        ]]
    );

    send_query(
        &mut client,
        "CREATE TABLE date_copy_wire (id TEXT PRIMARY KEY, day DATE UNIQUE);",
    );
    read_until_ready(&mut client);
    send_query(&mut client, "COPY date_copy_wire (id, day) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(
        &mut client,
        b'd',
        b"bc,0001-01-01 BC\nnamed,\"February 3, 2024\"\ninfinity,infinity\n",
    );
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');
    send_query(
        &mut client,
        "SELECT id, day FROM date_copy_wire ORDER BY day;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![
            vec!["bc".to_string(), "0001-01-01 BC".to_string()],
            vec!["named".to_string(), "2024-02-03".to_string()],
            vec!["infinity".to_string(), "infinity".to_string()],
        ]
    );

    // JSON drivers commonly serialize a SQL DATE as a UTC-midnight ISO
    // timestamp. BicDB accepts that lossless text bind and stores/returns the
    // canonical date so a sync outbox can drain instead of retrying forever.
    send_parse(
        &mut client,
        "insert_iso_midnight_date",
        "INSERT INTO date_copy_wire VALUES ('iso-midnight', $1)",
        &[1082],
    );
    send_bind(
        &mut client,
        "",
        "insert_iso_midnight_date",
        &["2026-08-01T00:00:00.000Z"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT day FROM date_copy_wire WHERE id = 'iso-midnight';",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["2026-08-01".to_string()]]
    );

    send_parse(
        &mut client,
        "invalid_binary_date",
        "SELECT $1::date",
        &[1082],
    );
    send_bind_binary(
        &mut client,
        "",
        "invalid_binary_date",
        &[(1, (i32::MAX - 1).to_be_bytes().to_vec())],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_error_response(&mut client).contains("08P01"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_time_applies_typmod_and_rejects_out_of_range_values() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE binary_times (id TEXT PRIMARY KEY, value TIME(3) UNIQUE);",
    );
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "insert_binary_time",
        "INSERT INTO binary_times (id, value) VALUES ($1, $2) RETURNING value",
        &[25, 1083],
    );
    let input = 45_296_555_500_i64;
    let rounded = 45_296_556_000_i64;
    send_bind_binary(
        &mut client,
        "",
        "insert_binary_time",
        &[(1, b"rounded".to_vec()), (1, input.to_be_bytes().to_vec())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1083]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![rounded.to_be_bytes().to_vec()]]);

    send_query(&mut client, "COPY binary_times (id, value) FROM STDIN CSV;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(
        &mut client,
        b'd',
        b"midnight,allballs\ncompact,040506.789\nend,24:00:00\n",
    );
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');
    send_query(
        &mut client,
        "SELECT id, value FROM binary_times ORDER BY value;",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![
            vec!["midnight".to_string(), "00:00:00".to_string()],
            vec!["compact".to_string(), "04:05:06.789".to_string()],
            vec!["rounded".to_string(), "12:34:56.556".to_string()],
            vec!["end".to_string(), "24:00:00".to_string()],
        ]
    );

    send_parse(
        &mut client,
        "invalid_binary_time",
        "SELECT $1::time",
        &[1083],
    );
    send_bind_binary(
        &mut client,
        "",
        "invalid_binary_time",
        &[(1, (86_400_000_001_i64).to_be_bytes().to_vec())],
        &[],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert!(read_error_response(&mut client).contains("08P01"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn row_description_matches_postgres_column_origin_and_type_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE row_description_probe (
            id INTEGER PRIMARY KEY,
            amount NUMERIC(10, 2),
            label VARCHAR(12)
        );
        CREATE TABLE row_description_owner (
            id INTEGER PRIMARY KEY,
            description VARCHAR(8)
        );
        INSERT INTO row_description_probe VALUES (1, 12.34, 'ready');
        INSERT INTO row_description_owner VALUES (1, 'owner');",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT id, amount AS total, label, id + 1 AS computed
         FROM row_description_probe;",
    );

    let mut fields = Vec::new();
    loop {
        let (tag, payload) = read_message(&mut client);
        match tag {
            b'T' => fields = parse_row_description_fields(&payload),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => break,
            _ => {}
        }
    }

    let table_oid = fields[0].table_oid;
    assert!(table_oid > 0);
    assert_eq!(
        fields,
        vec![
            RowDescriptionField::new("id", table_oid, 1, 23, 4, -1, 0),
            RowDescriptionField::new("total", table_oid, 2, 1700, -1, ((10_i32 << 16) | 2) + 4, 0,),
            RowDescriptionField::new("label", table_oid, 3, 1043, -1, 16, 0),
            RowDescriptionField::new("computed", 0, 0, 23, 4, -1, 0),
        ]
    );

    send_query(
        &mut client,
        "INSERT INTO row_description_probe VALUES (2, 56.78, 'inserted')
         RETURNING id, amount AS total, id + 1 AS computed;",
    );
    let mut returning_fields = Vec::new();
    loop {
        let (tag, payload) = read_message(&mut client);
        match tag {
            b'T' => returning_fields = parse_row_description_fields(&payload),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => break,
            _ => {}
        }
    }
    assert_eq!(
        returning_fields,
        vec![
            RowDescriptionField::new("id", table_oid, 1, 23, 4, -1, 0),
            RowDescriptionField::new("total", table_oid, 2, 1700, -1, ((10_i32 << 16) | 2) + 4, 0,),
            RowDescriptionField::new("computed", 0, 0, 23, 4, -1, 0),
        ]
    );

    send_query(
        &mut client,
        "SELECT probe.id, owner.description, probe.amount + 1 AS computed
         FROM row_description_probe AS probe
         JOIN row_description_owner AS owner ON owner.id = probe.id;",
    );
    let mut join_fields = Vec::new();
    loop {
        let (tag, payload) = read_message(&mut client);
        match tag {
            b'T' => join_fields = parse_row_description_fields(&payload),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => break,
            _ => {}
        }
    }
    let owner_table_oid = join_fields[1].table_oid;
    assert!(owner_table_oid > 0);
    assert_ne!(owner_table_oid, table_oid);
    assert_eq!(
        join_fields,
        vec![
            RowDescriptionField::new("id", table_oid, 1, 23, 4, -1, 0),
            RowDescriptionField::new("description", owner_table_oid, 2, 1043, -1, 12, 0,),
            RowDescriptionField::new("computed", 0, 0, 1700, -1, -1, 0),
        ]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_parameters_and_results_cover_structured_types() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let cases = structured_binary_codec_cases();
    let casts = cases
        .iter()
        .enumerate()
        .map(|(index, (oid, _))| {
            let type_name = PG_TYPE_REGISTRY.by_oid(*oid).unwrap().name;
            format!("${}::{}", index + 1, registry_declaration_name(type_name))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parameter_oids = cases.iter().map(|(oid, _)| *oid).collect::<Vec<_>>();
    send_parse(
        &mut client,
        "binary_structured",
        &format!("SELECT {casts}"),
        &parameter_oids,
    );
    let parameters = cases
        .iter()
        .map(|(_, payload)| (1, payload.clone()))
        .collect::<Vec<_>>();
    send_bind_binary(&mut client, "", "binary_structured", &parameters, &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, parameter_oids);
    assert_eq!(formats, vec![1; cases.len()]);
    assert_eq!(
        rows,
        vec![cases
            .into_iter()
            .map(|(_, payload)| payload)
            .collect::<Vec<_>>()]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_array_matrix_covers_every_registered_scalar_codec() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let uuid = "550e8400-e29b-41d4-a716-446655440000"
        .parse::<uuid::Uuid>()
        .unwrap();
    let mut cases = vec![
        (1000, 16, vec![1]),
        (1001, 17, vec![0, 255]),
        (1016, 20, 789_i64.to_be_bytes().to_vec()),
        (1005, 21, 123_i16.to_be_bytes().to_vec()),
        (1007, 23, 456_i32.to_be_bytes().to_vec()),
        (1009, 25, b"hello".to_vec()),
        (199, 114, br#"{"a":1}"#.to_vec()),
        (1021, 700, 1.5_f32.to_be_bytes().to_vec()),
        (1022, 701, 2.25_f64.to_be_bytes().to_vec()),
        (1015, 1043, b"varchar".to_vec()),
        (2201, 1790, b"portal name".to_vec()),
        (1182, 1082, 8_767_i32.to_be_bytes().to_vec()),
        (1183, 1083, 11_045_000_000_i64.to_be_bytes().to_vec()),
        (1115, 1114, 42_i64.to_be_bytes().to_vec()),
        (1185, 1184, 43_i64.to_be_bytes().to_vec()),
        (1187, 1186, interval_binary(0, 2, 0)),
        (1231, 1700, numeric_binary(2, 0, 0x0000, 2, &[12, 3000])),
        (2951, 2950, uuid.as_bytes().to_vec()),
        (3807, 3802, [vec![1], br#"{"b":2}"#.to_vec()].concat()),
        (791, 790, 12_345_i64.to_be_bytes().to_vec()),
        (1270, 1266, timetz_binary(11_045_000_000, -7_200)),
        (1002, 18, b"Z".to_vec()),
        (1003, 19, b"identifier".to_vec()),
        (143, 142, b"<value/>".to_vec()),
        (1014, 1042, b"p".to_vec()),
        (1561, 1560, bit_string_binary("1")),
        (1563, 1562, bit_string_binary("101001")),
        (380_201, 380_200, vector_binary(&[1.0, 2.0, 3.0])),
        (
            4073,
            4072,
            [vec![1], br#"$.value ? (@ > 2)"#.to_vec()].concat(),
        ),
        (
            3643,
            3614,
            decode_hex("00000002666174000002c00180037261740000010002"),
        ),
        (
            3645,
            3615,
            decode_hex("00000003020400030100007261740001000066617400"),
        ),
    ];
    cases.extend(
        structured_binary_codec_cases()
            .into_iter()
            .map(|(element_oid, value)| {
                let array_oid = PG_TYPE_REGISTRY
                    .by_oid(element_oid)
                    .and_then(|spec| spec.array_oid)
                    .unwrap();
                (array_oid, element_oid, value)
            }),
    );
    let registered_array_oids = PG_TYPE_REGISTRY
        .all()
        .iter()
        .filter(|spec| spec.binary_codec().is_some())
        .filter_map(|spec| spec.array_oid)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        cases
            .iter()
            .map(|(array_oid, _, _)| *array_oid)
            .collect::<std::collections::BTreeSet<_>>(),
        registered_array_oids
    );

    let casts = cases
        .iter()
        .enumerate()
        .map(|(index, (array_oid, _, _))| {
            let type_name = PG_TYPE_REGISTRY.by_array_oid(*array_oid).unwrap().name;
            let type_name = registry_declaration_name(type_name);
            format!("${}::{type_name}[]", index + 1)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let parameter_oids = cases
        .iter()
        .map(|(array_oid, _, _)| *array_oid)
        .collect::<Vec<_>>();
    send_parse(
        &mut client,
        "binary_arrays",
        &format!("SELECT {casts}"),
        &parameter_oids,
    );

    let array_payloads = cases
        .iter()
        .map(|(_, element_oid, value)| binary_array_payload(*element_oid, &[Some(value), None]))
        .collect::<Vec<_>>();
    let parameters = array_payloads
        .iter()
        .cloned()
        .map(|payload| (1, payload))
        .collect::<Vec<_>>();
    send_bind_binary(&mut client, "", "binary_arrays", &parameters, &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, parameter_oids);
    assert_eq!(formats, vec![1; cases.len()]);
    let mut expected_payloads = array_payloads;
    for (index, (array_oid, _, _)) in cases.iter().enumerate() {
        expected_payloads[index] = match *array_oid {
            3807 => binary_array_payload(
                3802,
                &[Some(&[vec![1], br#"{"b": 2}"#.to_vec()].concat()), None],
            ),
            4073 => binary_array_payload(
                4072,
                &[
                    Some(&[vec![1], br#"$."value"?(@ > 2)"#.to_vec()].concat()),
                    None,
                ],
            ),
            _ => expected_payloads[index].clone(),
        };
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), expected_payloads.len());
    for (index, expected) in expected_payloads.iter().enumerate() {
        assert_eq!(
            &rows[0][index], expected,
            "binary array payload mismatch for array oid {}",
            parameter_oids[index]
        );
    }

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_binary_text_search_parameters_and_results_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let vector = decode_hex("00000002666174000002c00180037261740000010002");
    let query = decode_hex("00000004020202010100007261740001000066617400");
    send_parse(
        &mut client,
        "binary_text_search",
        "SELECT $1::tsvector, $2::tsquery",
        &[3614, 3615],
    );
    send_bind_binary(
        &mut client,
        "",
        "binary_text_search",
        &[(1, vector.clone()), (1, query.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![3614, 3615]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![vector, query]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_multidimensional_binary_array_preserves_bounds_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let one = 1_i32.to_be_bytes().to_vec();
    let three = 3_i32.to_be_bytes().to_vec();
    let four = 4_i32.to_be_bytes().to_vec();
    let payload = binary_array_payload_with_dimensions(
        23,
        &[(2, 0), (2, 5)],
        &[Some(&one), None, Some(&three), Some(&four)],
    );
    send_parse(&mut client, "binary_matrix", "SELECT $1::int4[]", &[1007]);
    send_bind_binary(
        &mut client,
        "",
        "binary_matrix",
        &[(1, payload.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1007]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![payload]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn uuidv7_has_uuid_metadata_and_binary_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(&mut client, "uuidv7_binary", "SELECT uuidv7()", &[]);
    send_bind_binary(&mut client, "", "uuidv7_binary", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![2950]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 16);
    let id = uuid::Uuid::from_slice(&rows[0][0]).unwrap();
    assert_eq!(id.get_version_num(), 7);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn range_functions_and_operators_preserve_oids_and_binary_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "range_operations_binary",
        "SELECT int4range(1, 5), lower('[1,5)'::int4range),
                isempty('empty'::int4range),
                '[1,5)'::int4range + '[5,8)'::int4range,
                int4multirange('[8,10)'::int4range, '[1,8)'::int4range)",
        &[],
    );
    send_bind_binary(
        &mut client,
        "",
        "range_operations_binary",
        &[],
        &[1, 1, 1, 1, 1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let range = |lower: i32, upper: i32| {
        [
            vec![0x02],
            4_i32.to_be_bytes().to_vec(),
            lower.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            upper.to_be_bytes().to_vec(),
        ]
        .concat()
    };
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![3904, 23, 16, 3904, 4451]);
    assert_eq!(formats, vec![1, 1, 1, 1, 1]);
    let merged = range(1, 10);
    let multirange = [
        1_i32.to_be_bytes().to_vec(),
        (merged.len() as i32).to_be_bytes().to_vec(),
        merged,
    ]
    .concat();
    assert_eq!(
        rows,
        vec![vec![
            range(1, 5),
            1_i32.to_be_bytes().to_vec(),
            vec![1],
            range(1, 8),
            multirange,
        ]]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn range_and_multirange_arrays_round_trip_text_and_binary_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_parse(
        &mut client,
        "range_array_round_trip",
        "SELECT $1::int4range[], $2::int4multirange[]",
        &[3905, 6150],
    );
    send_bind(
        &mut client,
        "",
        "range_array_round_trip",
        &["{\"[1,3)\",empty}", "{\"{[1,3)}\",\"{}\"}"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "{\"[1,3)\",empty}".to_string(),
            "{\"{[1,3)}\",\"{}\"}".to_string(),
        ]]
    );

    let range = [
        vec![0x02],
        4_i32.to_be_bytes().to_vec(),
        1_i32.to_be_bytes().to_vec(),
        4_i32.to_be_bytes().to_vec(),
        3_i32.to_be_bytes().to_vec(),
    ]
    .concat();
    let empty_range = vec![0x01];
    let multirange = [
        1_i32.to_be_bytes().to_vec(),
        (range.len() as i32).to_be_bytes().to_vec(),
        range.clone(),
    ]
    .concat();
    let empty_multirange = 0_i32.to_be_bytes().to_vec();
    let range_array = binary_array_payload(3904, &[Some(&range), Some(&empty_range)]);
    let multirange_array =
        binary_array_payload(4451, &[Some(&multirange), Some(&empty_multirange)]);
    send_bind_binary(
        &mut client,
        "",
        "range_array_round_trip",
        &[(1, range_array.clone()), (1, multirange_array.clone())],
        &[1, 1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![3905, 6150]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows, vec![vec![range_array, multirange_array]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn user_defined_ranges_use_persisted_oids_and_native_binary_frames() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TYPE wire_span AS RANGE (SUBTYPE = int4);",
    );
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "SELECT typname, oid, typarray FROM pg_type
         WHERE typname IN ('wire_span', 'wire_span_multirange')
         ORDER BY typname;",
    );
    let oid_rows = read_query_rows(&mut client);
    let range_row = oid_rows.iter().find(|row| row[0] == "wire_span").unwrap();
    let multirange_row = oid_rows
        .iter()
        .find(|row| row[0] == "wire_span_multirange")
        .unwrap();
    let range_oid = range_row[1].parse::<i32>().unwrap();
    let range_array_oid = range_row[2].parse::<i32>().unwrap();
    let multirange_oid = multirange_row[1].parse::<i32>().unwrap();

    send_parse(
        &mut client,
        "user_range_binary",
        "SELECT $1::wire_span, $2::wire_span_multirange, $3::wire_span[]",
        &[range_oid, multirange_oid, range_array_oid],
    );
    let range = [
        vec![0x06],
        4_i32.to_be_bytes().to_vec(),
        1_i32.to_be_bytes().to_vec(),
        4_i32.to_be_bytes().to_vec(),
        3_i32.to_be_bytes().to_vec(),
    ]
    .concat();
    let multirange = [
        1_i32.to_be_bytes().to_vec(),
        (range.len() as i32).to_be_bytes().to_vec(),
        range.clone(),
    ]
    .concat();
    let range_array = binary_array_payload(range_oid, &[Some(&range), None]);
    send_bind_binary(
        &mut client,
        "",
        "user_range_binary",
        &[
            (1, range.clone()),
            (1, multirange.clone()),
            (1, range_array.clone()),
        ],
        &[1, 1, 1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![range_oid, multirange_oid, range_array_oid]);
    assert_eq!(formats, vec![1, 1, 1]);
    assert_eq!(rows, vec![vec![range, multirange, range_array]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pg_stats_range_histograms_use_anyarray_wire_metadata_and_element_oids() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE wire_range_stats (
                    id integer PRIMARY KEY,
                    spans int4multirange
                )",
            )
            .unwrap();
        session
            .execute(
                "INSERT INTO wire_range_stats
                 SELECT g, int4multirange(int4range(g, g + 10))
                 FROM generate_series(0, 99) AS s(g)",
            )
            .unwrap();
        session.execute("ANALYZE wire_range_stats").unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    let query = "SELECT range_bounds_histogram, range_length_histogram
                 FROM pg_catalog.pg_stats
                 WHERE tablename = 'wire_range_stats' AND attname = 'spans'";
    send_parse(&mut client, "range_stats", query, &[]);
    send_bind(&mut client, "", "range_stats", &[]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    let text_rows = read_query_rows(&mut client);
    assert_eq!(text_rows.len(), 1);
    assert!(text_rows[0][0].starts_with("{\"[0,10)\""));
    assert!(!text_rows[0][0].contains("{[0,10)}"));
    assert!(text_rows[0][1].starts_with("{10"));

    send_bind_binary(&mut client, "", "range_stats", &[], &[1, 1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![2277, 2277]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        i32::from_be_bytes(rows[0][0][8..12].try_into().unwrap()),
        3904
    );
    assert_eq!(
        i32::from_be_bytes(rows[0][1][8..12].try_into().unwrap()),
        701
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn uuid_functions_have_postgresql_binary_metadata_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "uuid_functions_binary",
        "SELECT uuidv4(),
                uuid_extract_version(uuidv7()),
                uuid_extract_timestamp('01890f3b-9e80-7000-8000-000000000000'::uuid)",
        &[],
    );
    send_bind_binary(&mut client, "", "uuid_functions_binary", &[], &[1, 1, 1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![2950, 21, 1184]);
    assert_eq!(formats, vec![1, 1, 1]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 16);
    assert_eq!(
        uuid::Uuid::from_slice(&rows[0][0])
            .unwrap()
            .get_version_num(),
        4
    );
    assert_eq!(
        i16::from_be_bytes(rows[0][1].clone().try_into().unwrap()),
        7
    );
    assert_eq!(
        i64::from_be_bytes(rows[0][2].clone().try_into().unwrap()),
        741_492_912_768_000
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn restricted_role_uuid_acl_join_preserves_binary_uuid_contract() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    for sql in [
        "CREATE ROLE acl_reader LOGIN",
        "CREATE TABLE acl_organizations (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL
        )",
        "CREATE TABLE acl_memberships (
            id UUID PRIMARY KEY,
            organization_id UUID NOT NULL REFERENCES acl_organizations(id),
            status TEXT NOT NULL
        )",
        "INSERT INTO acl_organizations VALUES (
            '00000000-0000-0000-0000-000000000101', 'North'
        )",
        "INSERT INTO acl_memberships VALUES (
            '00000000-0000-0000-0000-000000000201',
            '00000000-0000-0000-0000-000000000101',
            'active'
        )",
        "GRANT SELECT ON acl_organizations, acl_memberships TO acl_reader",
        "SET SESSION AUTHORIZATION acl_reader",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }

    send_parse(
        &mut client,
        "uuid_acl_join",
        "SELECT memberships.id, organizations.id
         FROM acl_memberships memberships
         JOIN acl_organizations organizations
           ON organizations.id = memberships.organization_id
         WHERE memberships.id = $1",
        &[2950],
    );
    let matched_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000201")
        .unwrap()
        .as_bytes()
        .to_vec();
    send_bind_binary(
        &mut client,
        "",
        "uuid_acl_join",
        &[(1, matched_id.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![2950, 2950]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], matched_id);
    assert_eq!(rows[0][1].len(), 16);

    let missing_id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000299")
        .unwrap()
        .as_bytes()
        .to_vec();
    send_bind_binary(&mut client, "", "uuid_acl_join", &[(1, missing_id)], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![2950, 2950]);
    assert_eq!(formats, vec![1, 1]);
    assert!(rows.is_empty());

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pg_has_role_uses_boolean_oid_and_binary_encoding_for_present_and_empty_rows() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    for sql in [
        "CREATE ROLE role_parent NOLOGIN",
        "CREATE ROLE role_child LOGIN",
        "GRANT role_parent TO role_child",
        "SET SESSION AUTHORIZATION role_child",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }

    send_parse(
        &mut client,
        "has_role_from_catalog",
        "SELECT pg_has_role(current_user, role_row.oid, 'MEMBER')
         FROM pg_roles role_row
         WHERE role_row.rolname = $1",
        &[25],
    );
    send_bind_binary(
        &mut client,
        "",
        "has_role_from_catalog",
        &[(0, b"role_parent".to_vec())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![16]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![vec![1]]]);

    send_bind_binary(
        &mut client,
        "",
        "has_role_from_catalog",
        &[(0, b"missing_role".to_vec())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![16]);
    assert_eq!(formats, vec![1]);
    assert!(rows.is_empty());

    send_query(
        &mut client,
        "SELECT pg_has_role(NULL::name, 'MEMBER') IS NULL FROM pg_roles LIMIT 1",
    );
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![16]);
    assert_eq!(rows, vec![vec!["t".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn snapshot_functions_use_postgres_oids_and_binary_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_parse(
        &mut client,
        "snapshot_binary_functions",
        "SELECT pg_current_snapshot(), txid_current_snapshot(),
                pg_snapshot_xmin('10:20:10,14,15'),
                txid_snapshot_xmax('10:20:10,14,15'),
                pg_visible_in_snapshot('11'::xid8, '10:20:10,14,15')",
        &[],
    );
    send_bind_binary(&mut client, "", "snapshot_binary_functions", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![5038, 2970, 5069, 20, 16]);
    assert_eq!(formats, vec![1; 5]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 20);
    assert_eq!(rows[0][0], rows[0][1]);
    assert_eq!(rows[0][2], 10_u64.to_be_bytes());
    assert_eq!(rows[0][3], 20_i64.to_be_bytes());
    assert_eq!(rows[0][4], vec![1]);

    send_parse(
        &mut client,
        "snapshot_xip_binary",
        "SELECT pg_snapshot_xip('10:20:10,14,15'::pg_snapshot)",
        &[],
    );
    send_bind_binary(&mut client, "", "snapshot_xip_binary", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![5069]);
    assert_eq!(formats, vec![1]);
    assert_eq!(
        rows,
        vec![10_u64, 14, 15]
            .into_iter()
            .map(|xid| vec![xid.to_be_bytes().to_vec()])
            .collect::<Vec<_>>()
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn json_has_postgresql_oid_and_exact_binary_text_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "json_binary",
        r#"SELECT '{  "z" : 1, "z" : 2 }'::json,
                  json_build_object('a', 1, 'b', NULL)"#,
        &[],
    );
    send_bind_binary(&mut client, "", "json_binary", &[], &[1, 1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![114, 114]);
    assert_eq!(formats, vec![1, 1]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], br#"{  "z" : 1, "z" : 2 }"#);
    assert_eq!(rows[0][1], br#"{"a" : 1, "b" : null}"#);

    send_parse(&mut client, "json_parameter", "SELECT $1::json", &[114]);
    assert_eq!(read_message(&mut client).0, b'1');
    send_describe_statement(&mut client, "json_parameter");
    assert_eq!(read_parameter_description(&mut client), vec![114]);
    assert_eq!(read_message(&mut client).0, b'T');
    let exact = br#"{  "z" : 1, "z" : 2 }"#.to_vec();
    send_bind_binary(
        &mut client,
        "",
        "json_parameter",
        &[(1, exact.clone())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![114]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![exact]]);

    send_query(
        &mut client,
        "CREATE TABLE json_copy_wire (id TEXT PRIMARY KEY, payload JSON NOT NULL);",
    );
    assert!(read_query_rows(&mut client).is_empty());
    send_query(&mut client, "COPY json_copy_wire (id, payload) FROM STDIN;");
    assert_eq!(read_message(&mut client).0, b'G');
    send_message(&mut client, b'd', b"row\t{  \"z\" : 1, \"z\" : 2 }\n");
    send_message(&mut client, b'c', &[]);
    assert_eq!(read_tags_until_ready(&mut client).1, b'I');
    send_query(
        &mut client,
        "COPY (SELECT id, payload FROM json_copy_wire) TO STDOUT;",
    );
    let (rows, status) = read_copy_out(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec!["row\t{  \"z\" : 1, \"z\" : 2 }\n"]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_jsonb_expression_oids_and_binary_results_match_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "jsonb_typeof_comparison",
        "SELECT jsonb_typeof($1::jsonb) = 'object'",
        &[3802],
    );
    send_bind_binary(
        &mut client,
        "",
        "jsonb_typeof_comparison",
        &[(1, [vec![1], br#"{}"#.to_vec()].concat())],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![16]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![vec![1]]]);

    send_query(
        &mut client,
        "CREATE TABLE pgwire_jsonb_paths (id TEXT PRIMARY KEY, value JSONB)",
    );
    assert!(read_query_rows(&mut client).is_empty());
    send_query(
        &mut client,
        r#"INSERT INTO pgwire_jsonb_paths (id, value) VALUES ('row', '{"arr":[10,20]}'::jsonb)"#,
    );
    assert!(read_query_rows(&mut client).is_empty());

    send_parse(
        &mut client,
        "jsonb_arrow",
        "SELECT value->'arr' FROM pgwire_jsonb_paths WHERE id = 'row'",
        &[],
    );
    send_bind_binary(&mut client, "", "jsonb_arrow", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![3802]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![[vec![1], b"[10, 20]".to_vec()].concat()]]);

    send_parse(
        &mut client,
        "jsonb_lateral_metadata",
        "SELECT COALESCE(catalog.elements, '[]'::jsonb) AS elements
         FROM pgwire_jsonb_paths app
         LEFT JOIN LATERAL (
             WITH extracted AS (
                 SELECT app.value->'arr' AS elements
             )
             SELECT (SELECT elements FROM extracted) AS elements
         ) catalog ON true
         WHERE app.id = 'row'",
        &[],
    );
    send_bind_binary(&mut client, "", "jsonb_lateral_metadata", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![3802]);
    assert_eq!(formats, vec![1]);
    assert_eq!(rows, vec![vec![[vec![1], b"[10, 20]".to_vec()].concat()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_text_array_parameters_use_array_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(
        &mut client,
        "array_eq",
        "SELECT 1 WHERE ARRAY['partition_id', 'build_id'] = $1",
        &[1009],
    );
    send_bind(
        &mut client,
        "",
        "array_eq",
        &[r#"{"partition_id","build_id"}"#],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    let rows = read_query_rows(&mut client);
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_network_array_codecs_match_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SELECT ARRAY['192.0.2.1/32','2001:db8::1/128']::inet[],
                ARRAY['192.0.2.0/24']::cidr[]",
    );
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec![
            "{192.0.2.1,2001:db8::1}".to_string(),
            "{192.0.2.0/24}".to_string(),
        ]],
    );

    let binary_arrays = [
        decode_hex("00000001000000000000036500000003000000010000000802200004c0000201000000140380001020010db80000000000000000000000010000000802180004c0000201"),
        decode_hex("00000001000000000000028a00000002000000010000000802180104c0000200000000140320011020010db8000000000000000000000000"),
        decode_hex("00000001000000000000033d00000002000000010000000608002b0102030000000608002b010204"),
        decode_hex("00000001000000000000030600000002000000010000000808002b01020304050000000808002b0102030406"),
    ];
    send_parse(
        &mut client,
        "network_binary_roundtrip",
        "SELECT $1::inet[], $2::cidr[], $3::macaddr[], $4::macaddr8[]",
        &[1041, 651, 1040, 775],
    );
    send_bind_binary(
        &mut client,
        "",
        "network_binary_roundtrip",
        &binary_arrays
            .iter()
            .cloned()
            .map(|value| (1, value))
            .collect::<Vec<_>>(),
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);

    let (oids, formats, rows, status) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(oids, vec![1041, 651, 1040, 775]);
    assert_eq!(formats, vec![1; 4]);
    assert_eq!(rows, vec![binary_arrays.to_vec()]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_query_rejects_unsupported_binary_formats_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_parse(&mut client, "bad_param_oid", "SELECT $1", &[999_999]);
    send_bind_binary(&mut client, "", "bad_param_oid", &[(1, vec![1, 2, 3])], &[]);
    send_sync(&mut client);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("unsupported binary parameter oid 999999"));

    send_parse(&mut client, "bad_result_format", "SELECT 1", &[]);
    send_bind_binary(&mut client, "", "bad_result_format", &[], &[2]);
    send_sync(&mut client);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("unsupported result format code 2"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[tokio::test]
async fn tokio_postgres_basic_orm_client_gauntlet() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (mut client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE TABLE tokio_gauntlet (id TEXT PRIMARY KEY, label TEXT NOT NULL, seen INT NOT NULL);",
        )
        .await
        .unwrap();

    let insert = client
        .prepare(
            "INSERT INTO tokio_gauntlet (id, label, seen) VALUES ($1::text, $2::text, $3::int4)",
        )
        .await
        .unwrap();
    client
        .execute(&insert, &[&"t1", &"Iris", &1_i32])
        .await
        .unwrap();
    client
        .execute(
            "UPDATE tokio_gauntlet SET seen = $1::int4 WHERE id = $2::text",
            &[&2_i32, &"t1"],
        )
        .await
        .unwrap();
    let rows = client
        .simple_query("SELECT seen FROM tokio_gauntlet WHERE id = 't1';")
        .await
        .unwrap();
    let seen = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get("seen"),
        _ => None,
    });
    assert_eq!(seen, Some("2"));

    let transaction = client.transaction().await.unwrap();
    transaction
        .execute(
            "INSERT INTO tokio_gauntlet (id, label, seen) VALUES ($1::text, $2::text, $3::int4)",
            &[&"rolled", &"Rollback", &9_i32],
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    let rows = client
        .simple_query("SELECT COUNT(*) FROM tokio_gauntlet WHERE id = 'rolled';")
        .await
        .unwrap();
    let rolled_count = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(rolled_count, Some("0"));

    let bytes = vec![0_u8, 255_u8];
    let row = client
        .query_one(
            "SELECT $1::int2, $2::int4, $3::int8, $4::bool, $5::float4, $6::float8, $7::text, $8::bytea",
            &[&123_i16, &456_i32, &789_i64, &true, &1.5_f32, &2.25_f64, &"hello", &bytes],
        )
        .await
        .unwrap();

    assert_eq!(row.get::<_, i16>(0), 123);
    assert_eq!(row.get::<_, i32>(1), 456);
    assert_eq!(row.get::<_, i64>(2), 789);
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, f32>(4), 1.5);
    assert_eq!(row.get::<_, f64>(5), 2.25);
    assert_eq!(row.get::<_, String>(6), "hello");
    assert_eq!(row.get::<_, Vec<u8>>(7), bytes);

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn sqlx_text_and_binary_type_client_gauntlet() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let url = format!("postgres://bicdb@{address}/bicdb");
    let mut connection = sqlx::postgres::PgConnection::connect(&url).await.unwrap();

    let text = sqlx::query(
        "SELECT TRUE::text, (-12345)::int2::text, 123456789::int4::text, \
         9007199254740993::int8::text, 12345678901234567890.125::numeric::text, \
         '\\x00ff10'::bytea::text",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(text.try_get::<String, _>(0).unwrap(), "true");
    assert_eq!(text.try_get::<String, _>(1).unwrap(), "-12345");
    assert_eq!(text.try_get::<String, _>(2).unwrap(), "123456789");
    assert_eq!(text.try_get::<String, _>(3).unwrap(), "9007199254740993");
    assert_eq!(
        text.try_get::<String, _>(4).unwrap(),
        "12345678901234567890.125"
    );
    assert_eq!(text.try_get::<String, _>(5).unwrap(), "\\x00ff10");

    let binary = sqlx::query(
        "SELECT TRUE::bool, (-12345)::int2, 123456789::int4, \
         9007199254740993::int8, 1.5::float4, (-2.25)::float8, \
         'client-text'::text, '\\x00ff10'::bytea",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert!(binary.try_get::<bool, _>(0).unwrap());
    assert_eq!(binary.try_get::<i16, _>(1).unwrap(), -12345);
    assert_eq!(binary.try_get::<i32, _>(2).unwrap(), 123456789);
    assert_eq!(binary.try_get::<i64, _>(3).unwrap(), 9007199254740993);
    assert_eq!(binary.try_get::<f32, _>(4).unwrap(), 1.5);
    assert_eq!(binary.try_get::<f64, _>(5).unwrap(), -2.25);
    assert_eq!(binary.try_get::<String, _>(6).unwrap(), "client-text");
    assert_eq!(binary.try_get::<Vec<u8>, _>(7).unwrap(), vec![0, 255, 16]);

    connection.close().await.unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_uncast_parameter_reports_parameter_description() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("CREATE TABLE uncast_params (id TEXT PRIMARY KEY, label TEXT);")
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO uncast_params (id, label) VALUES ($1, $2)",
            &[&"p1", &"Ada"],
        )
        .await
        .unwrap();
    let row = client
        .query_one("SELECT label FROM uncast_params WHERE id = $1", &[&"p1"])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "Ada");

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_runs_cognee_jsonb_srf_query_with_text_array_parameter() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            r#"CREATE TABLE "Entity_name" (
                   id text PRIMARY KEY,
                   payload jsonb NOT NULL
               );
               INSERT INTO "Entity_name" VALUES
                   ('a', '{"belongs_to_set":["alpha","beta"]}'::jsonb),
                   ('b', '{"belongs_to_set":["gamma"]}'::jsonb),
                   ('c', '{}'::jsonb);"#,
        )
        .await
        .unwrap();

    let wanted = vec!["beta".to_string()];
    let rows = client
        .query(
            r#"SELECT id
               FROM "Entity_name"
               WHERE payload::jsonb ? 'belongs_to_set'
                 AND EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements_text(
                     payload::jsonb -> 'belongs_to_set'
                   ) v
                   WHERE v = ANY($1::text[])
                 )
               ORDER BY id"#,
            &[&wanted],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "a");

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_preserves_quoted_relation_owner_identity() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            r#"CREATE ROLE cognee LOGIN;
               SET ROLE cognee;
               CREATE TABLE "QuotedProbe" (id integer PRIMARY KEY);
               INSERT INTO "QuotedProbe" VALUES (1);"#,
        )
        .await
        .unwrap();
    let row = client
        .query_one(r#"SELECT id FROM "QuotedProbe""#, &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    let error = client
        .query_one("SELECT id FROM quotedprobe", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE)
    );

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_advisory_locks_match_postgres_connection_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (mut first, first_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let first_task = tokio::spawn(async move {
        let _ = first_connection.await;
    });
    let (second, second_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let second_task = tokio::spawn(async move {
        let _ = second_connection.await;
    });

    // Provisioning catalog literals must not be mistaken for lock calls.
    first.batch_execute("DO $provision$ DECLARE item RECORD; BEGIN FOR item IN SELECT 'pg_advisory_xact_lock' AS name LOOP NULL; END LOOP; END $provision$;").await.unwrap();
    first.batch_execute(r#"
        CREATE ROLE provisioned_reader;
        CREATE FUNCTION public.visible_routine(x text, suffix text DEFAULT '!') RETURNS text LANGUAGE SQL AS $$ SELECT x || suffix $$;
        REVOKE ALL ON FUNCTION public.visible_routine(text,text) FROM PUBLIC;
        DO $grant_functions$
        DECLARE requested RECORD; implementation RECORD;
        BEGIN
          FOR requested IN SELECT * FROM (VALUES
            ('public', 'visible_routine', 1::smallint),
            ('pg_catalog', 'pg_advisory_xact_lock', 1::smallint)
          ) AS required(schema_name, function_name, argument_count)
          LOOP
            FOR implementation IN
              SELECT procedure.oid::regprocedure AS identity
              FROM pg_catalog.pg_proc AS procedure
              JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=procedure.pronamespace
              WHERE namespace.nspname=requested.schema_name
                AND procedure.proname=requested.function_name
                AND requested.argument_count >= procedure.pronargs-procedure.pronargdefaults
                    - CASE WHEN procedure.provariadic=0 THEN 0 ELSE 1 END
                AND (requested.argument_count <= procedure.pronargs OR procedure.provariadic<>0)
            LOOP
              EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO provisioned_reader', implementation.identity);
            END LOOP;
          END LOOP;
        END
        $grant_functions$;
    "#).await.unwrap();
    assert!(first.query_one("SELECT has_function_privilege('provisioned_reader', 'public.visible_routine(text,text)', 'EXECUTE')", &[]).await.unwrap().get::<_,bool>(0));
    first.batch_execute(r#"
        CREATE TABLE key_effects (id int PRIMARY KEY);
        CREATE FUNCTION write_key() RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO key_effects VALUES (1); RETURN 7; END $$;
        CREATE FUNCTION reject_key() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'invalid lock key'; END $$;
    "#).await.unwrap();
    let error = first
        .query("SELECT pg_advisory_lock(write_key(), reject_key())", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&tokio_postgres::error::SqlState::RAISE_EXCEPTION)
    );
    assert_eq!(
        first
            .query_one("SELECT count(*) FROM key_effects", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // SQL expressions (including session settings and bound parameters) are
    // evaluated in the caller's transaction, then acquire a real server lock.
    first
        .batch_execute("BEGIN; SET LOCAL app.lock_namespace = 'tenant';")
        .await
        .unwrap();
    first.query("SELECT pg_advisory_xact_lock(hashtextextended(current_setting('app.lock_namespace') || ':' || $1::text, 0))", &[&"key"]).await.unwrap();
    assert!(!second
        .query_one(
            "SELECT pg_try_advisory_xact_lock(2119148298895563973::bigint)",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(second
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('another:key', 0))",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));
    first.batch_execute("ROLLBACK").await.unwrap();
    assert!(second
        .query_one(
            "SELECT pg_try_advisory_xact_lock(2119148298895563973::bigint)",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0));

    let session_lock = first.prepare("SELECT pg_advisory_lock($1)").await.unwrap();
    assert_eq!(session_lock.params(), &[tokio_postgres::types::Type::INT8]);
    assert_eq!(
        session_lock.columns()[0].type_(),
        &tokio_postgres::types::Type::VOID
    );
    let pair_try = first
        .prepare("SELECT pg_try_advisory_lock($1, $2)")
        .await
        .unwrap();
    assert_eq!(
        pair_try.params(),
        &[
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::INT4
        ]
    );
    assert_eq!(
        pair_try.columns()[0].type_(),
        &tokio_postgres::types::Type::BOOL
    );

    first.query(&session_lock, &[&41_i64]).await.unwrap();
    first.query(&session_lock, &[&41_i64]).await.unwrap();
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(first
        .query_one("SELECT pg_advisory_unlock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(first
        .query_one("SELECT pg_advisory_unlock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!first
        .query_one("SELECT pg_advisory_unlock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(41::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    second
        .query_one("SELECT pg_advisory_unlock(41::bigint)", &[])
        .await
        .unwrap();

    first
        .query_one("SELECT pg_advisory_lock_shared(51::bigint)", &[])
        .await
        .unwrap();
    assert!(second
        .query_one("SELECT pg_try_advisory_lock_shared(51::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(51::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    first
        .query_one("SELECT pg_advisory_unlock_shared(51::bigint)", &[])
        .await
        .unwrap();
    second
        .query_one("SELECT pg_advisory_unlock_shared(51::bigint)", &[])
        .await
        .unwrap();

    first
        .query_one("SELECT pg_advisory_lock(7::bigint)", &[])
        .await
        .unwrap();
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(0, 7)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    first
        .query_one("SELECT pg_advisory_unlock(7::bigint)", &[])
        .await
        .unwrap();
    second
        .query_one("SELECT pg_advisory_unlock(0, 7)", &[])
        .await
        .unwrap();

    let transaction = first.transaction().await.unwrap();
    assert!(transaction
        .query_one("SELECT pg_try_advisory_xact_lock(61::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(61::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    transaction.commit().await.unwrap();
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(61::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    second
        .query_one("SELECT pg_advisory_unlock(61::bigint)", &[])
        .await
        .unwrap();

    let transaction = first.transaction().await.unwrap();
    transaction
        .query_one("SELECT pg_advisory_xact_lock_shared(62::bigint)", &[])
        .await
        .unwrap();
    assert!(second
        .query_one("SELECT pg_try_advisory_lock_shared(62::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(62::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    transaction.rollback().await.unwrap();
    second
        .query_one("SELECT pg_advisory_unlock_shared(62::bigint)", &[])
        .await
        .unwrap();

    first
        .query_one("SELECT pg_advisory_lock(63::bigint)", &[])
        .await
        .unwrap();
    first
        .query_one("SELECT pg_advisory_lock_shared(64::bigint)", &[])
        .await
        .unwrap();
    first
        .query_one("SELECT pg_advisory_unlock_all()", &[])
        .await
        .unwrap();
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(63::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(64::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    second
        .query_one("SELECT pg_advisory_unlock(63::bigint)", &[])
        .await
        .unwrap();
    second
        .query_one("SELECT pg_advisory_unlock(64::bigint)", &[])
        .await
        .unwrap();

    let (disconnecting, disconnecting_connection) =
        tokio_postgres::connect(&config, tokio_postgres::NoTls)
            .await
            .unwrap();
    let disconnecting_task = tokio::spawn(async move {
        let _ = disconnecting_connection.await;
    });
    disconnecting
        .query_one("SELECT pg_advisory_lock(65::bigint)", &[])
        .await
        .unwrap();
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(65::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    drop(disconnecting);
    disconnecting_task.await.unwrap();
    // The client connection future can finish just before the server task has
    // completed its disconnect cleanup. Wait for that cleanup instead of
    // assuming the two independently scheduled tasks finish in lockstep.
    let cleanup_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let released_after_disconnect = loop {
        let acquired = second
            .query_one("SELECT pg_try_advisory_lock(65::bigint)", &[])
            .await
            .unwrap()
            .get::<_, bool>(0);
        if acquired {
            break true;
        }
        if tokio::time::Instant::now() >= cleanup_deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        released_after_disconnect,
        "session advisory lock was not released after disconnect cleanup"
    );
    second
        .query_one("SELECT pg_advisory_unlock(65::bigint)", &[])
        .await
        .unwrap();

    first
        .query_one("SELECT pg_advisory_lock(71::bigint)", &[])
        .await
        .unwrap();
    let blocked = tokio::spawn(async move {
        second
            .query_one("SELECT pg_advisory_lock(71::bigint)", &[])
            .await
            .map(|_| second)
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!blocked.is_finished());
    first
        .query_one("SELECT pg_advisory_unlock(71::bigint)", &[])
        .await
        .unwrap();
    let second = blocked.await.unwrap().unwrap();
    second
        .query_one("SELECT pg_advisory_unlock(71::bigint)", &[])
        .await
        .unwrap();

    first
        .query_one("SELECT pg_advisory_lock(81::bigint)", &[])
        .await
        .unwrap();
    second
        .query_one("SELECT pg_advisory_lock(82::bigint)", &[])
        .await
        .unwrap();
    let first_wait = tokio::spawn(async move {
        first
            .query_one("SELECT pg_advisory_lock(82::bigint)", &[])
            .await
            .map(|_| first)
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let deadlock = second
        .query_one("SELECT pg_advisory_lock(81::bigint)", &[])
        .await
        .unwrap_err();
    assert_eq!(
        deadlock.code(),
        Some(&tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED)
    );
    second
        .query_one("SELECT pg_advisory_unlock(82::bigint)", &[])
        .await
        .unwrap();
    let first = first_wait.await.unwrap().unwrap();

    drop(first);
    drop(second);
    server.request_shutdown();
    first_task.await.unwrap();
    second_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_nested_advisory_locks_are_visible_in_pg_locks() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (first, first_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let first_task = tokio::spawn(async move {
        let _ = first_connection.await;
    });
    let (second, second_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let second_task = tokio::spawn(async move {
        let _ = second_connection.await;
    });
    let (observer, observer_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let observer_task = tokio::spawn(async move {
        let _ = observer_connection.await;
    });

    first
        .batch_execute(
            r#"CREATE FUNCTION acquire_nested(integer, integer) RETURNS boolean
               LANGUAGE plpgsql AS $$
               BEGIN
                 RETURN pg_try_advisory_lock($1, $2);
               END
               $$;"#,
        )
        .await
        .unwrap();
    let backend_pid = first.prepare("SELECT pg_backend_pid()").await.unwrap();
    assert_eq!(
        backend_pid.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );
    let first_pid = first
        .query_one(&backend_pid, &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    let second_pid = second
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get::<_, i32>(0);
    assert_ne!(first_pid, second_pid);

    assert!(first
        .query_one("SELECT acquire_nested(-123456789, 987654321)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(-123456789, 987654321)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));

    let database_oid = first
        .query_one(
            "SELECT oid::text FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await
        .unwrap()
        .get::<_, String>(0);
    let lock = first
        .query_one(
            r#"SELECT locktype, database::text, relation::text, page::text, tuple::text,
                      virtualxid::text, transactionid::text, classid::text, objid::text,
                      objsubid::text, pid::text, mode, granted, fastpath, waitstart::text
               FROM pg_locks
               WHERE locktype = 'advisory'
                 AND pid = pg_backend_pid()
                 AND classid = 4171510507
                 AND objid = 987654321
                 AND objsubid = 2"#,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(lock.get::<_, &str>(0), "advisory");
    assert_eq!(lock.get::<_, &str>(1), database_oid);
    for index in 2..7 {
        assert_eq!(lock.get::<_, Option<&str>>(index), None);
    }
    assert_eq!(lock.get::<_, &str>(7), "4171510507");
    assert_eq!(lock.get::<_, &str>(8), "987654321");
    assert_eq!(lock.get::<_, &str>(9), "2");
    assert_eq!(lock.get::<_, &str>(10), first_pid.to_string());
    assert_eq!(lock.get::<_, &str>(11), "ExclusiveLock");
    assert!(lock.get::<_, bool>(12));
    assert!(!lock.get::<_, bool>(13));
    assert_eq!(lock.get::<_, Option<&str>>(14), None);

    let blocked = tokio::spawn(async move {
        second
            .query_one("SELECT pg_advisory_lock(-123456789, 987654321)", &[])
            .await
            .map(|_| second)
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let waiting = observer
        .query_one(
            r#"SELECT waitstart IS NOT NULL
               FROM pg_locks
               WHERE locktype = 'advisory'
                 AND pid = $1
                 AND classid = 4171510507
                 AND objid = 987654321
                 AND objsubid = 2
                 AND NOT granted"#,
            &[&second_pid],
        )
        .await
        .unwrap();
    assert!(waiting.get::<_, bool>(0));

    assert!(first
        .query_one("SELECT pg_advisory_unlock(-123456789, 987654321)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    let second = blocked.await.unwrap().unwrap();
    second
        .query_one("SELECT pg_advisory_unlock(-123456789, 987654321)", &[])
        .await
        .unwrap();

    drop(first);
    drop(second);
    drop(observer);
    server.request_shutdown();
    first_task.abort();
    second_task.abort();
    observer_task.abort();
    let _ = first_task.await;
    let _ = second_task.await;
    let _ = observer_task.await;
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_advisory_lock_wait_honors_query_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        query_timeout: std::time::Duration::from_millis(100),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let connection_config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (holder, holder_connection) =
        tokio_postgres::connect(&connection_config, tokio_postgres::NoTls)
            .await
            .unwrap();
    let holder_task = tokio::spawn(async move {
        let _ = holder_connection.await;
    });
    let (waiter, waiter_connection) =
        tokio_postgres::connect(&connection_config, tokio_postgres::NoTls)
            .await
            .unwrap();
    let waiter_task = tokio::spawn(async move {
        let _ = waiter_connection.await;
    });

    holder
        .query_one("SELECT pg_advisory_lock(101::bigint)", &[])
        .await
        .unwrap();
    let timeout = waiter
        .query_one("SELECT pg_advisory_lock(101::bigint)", &[])
        .await
        .unwrap_err();
    assert_eq!(
        timeout.code(),
        Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
    );
    assert!(waiter
        .query_one("SELECT pg_try_advisory_lock(102::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    holder
        .query_one("SELECT pg_advisory_unlock(101::bigint)", &[])
        .await
        .unwrap();
    assert!(waiter
        .query_one("SELECT pg_try_advisory_lock(101::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));

    drop(holder);
    drop(waiter);
    server.request_shutdown();
    holder_task.await.unwrap();
    waiter_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_runs_asyncpg_jit_and_recursive_type_discovery_contract() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let jit = client
        .query_one(
            "SELECT current_setting('jit') AS cur, set_config('jit', 'off', false) AS new",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(jit.get::<_, String>("cur"), "off");
    assert_eq!(jit.get::<_, String>("new"), "off");

    let typeinfo = r#"(
        SELECT
            t.oid AS oid,
            ns.nspname AS ns,
            t.typname AS name,
            t.typtype AS kind,
            (CASE WHEN t.typtype = 'd' THEN
                (WITH RECURSIVE typebases(oid, depth) AS (
                    SELECT t2.typbasetype AS oid, 0 AS depth
                    FROM pg_type t2
                    WHERE t2.oid = t.oid

                    UNION ALL

                    SELECT t2.typbasetype AS oid, tb.depth + 1 AS depth
                    FROM pg_type t2, typebases tb
                    WHERE tb.oid = t2.oid AND t2.typbasetype != 0
                ) SELECT oid FROM typebases ORDER BY depth DESC LIMIT 1)
                ELSE NULL
            END) AS basetype,
            t.typelem AS elemtype,
            elem_t.typdelim AS elemdelim,
            range_t.rngsubtype AS range_subtype,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT array_agg(ia.atttypid ORDER BY ia.attnum)
                 FROM pg_attribute ia
                 INNER JOIN pg_class c ON ia.attrelid = c.oid
                 WHERE ia.attnum > 0 AND NOT ia.attisdropped
                   AND c.reltype = t.oid)
                ELSE NULL
            END) AS attrtypoids,
            (CASE WHEN t.typtype = 'c' THEN
                (SELECT array_agg(ia.attname::text ORDER BY ia.attnum)
                 FROM pg_attribute ia
                 INNER JOIN pg_class c ON ia.attrelid = c.oid
                 WHERE ia.attnum > 0 AND NOT ia.attisdropped
                   AND c.reltype = t.oid)
                ELSE NULL
            END) AS attrnames
        FROM pg_catalog.pg_type AS t
        INNER JOIN pg_catalog.pg_namespace ns ON ns.oid = t.typnamespace
        LEFT JOIN pg_type elem_t ON (
            t.typlen = -1 AND t.typelem != 0 AND t.typelem = elem_t.oid
        )
        LEFT JOIN pg_range range_t ON t.oid = range_t.rngtypid
    )"#;
    let discovery_sql = format!(
        r#"WITH RECURSIVE typeinfo_tree(
                oid, ns, name, kind, basetype, elemtype, elemdelim,
                range_subtype, attrtypoids, attrnames, depth)
            AS (
                SELECT
                    ti.oid, ti.ns, ti.name, ti.kind, ti.basetype,
                    ti.elemtype, ti.elemdelim, ti.range_subtype,
                    ti.attrtypoids, ti.attrnames, 0
                FROM {typeinfo} AS ti
                WHERE ti.oid = ANY($1::oid[])

                UNION ALL

                SELECT
                    ti.oid, ti.ns, ti.name, ti.kind, ti.basetype,
                    ti.elemtype, ti.elemdelim, ti.range_subtype,
                    ti.attrtypoids, ti.attrnames, tt.depth + 1
                FROM {typeinfo} ti,
                typeinfo_tree tt
                WHERE (tt.elemtype IS NOT NULL AND ti.oid = tt.elemtype)
                   OR (tt.attrtypoids IS NOT NULL AND ti.oid = ANY(tt.attrtypoids))
                   OR (tt.range_subtype IS NOT NULL AND ti.oid = tt.range_subtype)
                   OR (tt.basetype IS NOT NULL AND ti.oid = tt.basetype)
            )
            SELECT DISTINCT
                *,
                basetype::regtype::text AS basetype_name,
                elemtype::regtype::text AS elemtype_name,
                range_subtype::regtype::text AS range_subtype_name
            FROM typeinfo_tree
            ORDER BY depth DESC"#
    );
    let discovery = client.prepare(&discovery_sql).await.unwrap();
    let discovery_oids = discovery
        .columns()
        .iter()
        .map(|column| column.type_().oid())
        .collect::<Vec<_>>();
    assert_eq!(
        discovery_oids[0], 26,
        "typeinfo oid must remain pg_catalog.oid"
    );
    assert_eq!(
        discovery_oids[3], 18,
        "typeinfo kind must remain internal char"
    );
    assert_eq!(
        discovery_oids[5], 26,
        "typeinfo elemtype must remain pg_catalog.oid"
    );
    assert_eq!(
        discovery_oids[6], 18,
        "typeinfo delimiter must remain internal char"
    );
    assert_eq!(
        discovery_oids[7], 26,
        "range subtype must remain pg_catalog.oid"
    );
    let requested_oids = vec![1015_u32];
    let rows = client.query(&discovery, &[&requested_oids]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "varchar");
    assert_eq!(rows[1].get::<_, String>("name"), "_varchar");

    let requested_range_oids = vec![3904_u32];
    let range_rows = client
        .query(&discovery, &[&requested_range_oids])
        .await
        .unwrap();
    assert_eq!(range_rows.len(), 2);
    let range_row = range_rows
        .iter()
        .find(|row| row.get::<_, String>("name") == "int4range")
        .expect("range type discovery omitted int4range");
    assert_eq!(range_row.get::<_, Option<u32>>("range_subtype"), Some(23));
    assert_eq!(
        range_row.get::<_, Option<String>>("range_subtype_name"),
        Some("integer".to_string())
    );
    assert!(range_rows
        .iter()
        .any(|row| row.get::<_, String>("name") == "int4"));

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_parameterized_insert_returning_describes_portal() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE TABLE auth_rate_limits (
                bucket_key TEXT PRIMARY KEY,
                count INTEGER NOT NULL,
                reset_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );",
        )
        .await
        .unwrap();
    let query = "
        INSERT INTO auth_rate_limits (bucket_key, count, reset_at, updated_at)
        VALUES ($1, $2::int, NOW() + INTERVAL '60 seconds', NOW())
        ON CONFLICT (bucket_key) DO UPDATE
        SET count = CASE
              WHEN auth_rate_limits.reset_at <= NOW() THEN 1
              ELSE auth_rate_limits.count + 1
            END,
            reset_at = CASE
              WHEN auth_rate_limits.reset_at <= NOW()
                THEN NOW() + INTERVAL '60 seconds'
              ELSE auth_rate_limits.reset_at
            END,
            updated_at = NOW()
        RETURNING count,
                  EXTRACT(EPOCH FROM (reset_at - NOW()))::bigint AS retry_after_seconds";

    let first = client
        .query_one(query, &[&"metadata:127.0.0.1", &1_i32])
        .await
        .unwrap();
    assert_eq!(first.get::<_, i32>(0), 1);
    assert_eq!(first.columns()[1].type_().name(), "int8");
    assert!(first.get::<_, i64>(1) >= 0);

    let second = client
        .query_one(query, &[&"metadata:127.0.0.1", &1_i32])
        .await
        .unwrap();
    assert_eq!(second.get::<_, i32>(0), 2);
    assert!(second.get::<_, i64>(1) >= 0);

    client
        .batch_execute(
            "CREATE TABLE audit_returning (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                metadata JSONB NOT NULL,
                row_hash TEXT NOT NULL
            );",
        )
        .await
        .unwrap();
    let audit = client
        .query_one(
            "INSERT INTO audit_returning (id, action, metadata, row_hash)
             VALUES ($1, $2, '{\"outcome\":\"denied\"}'::jsonb, $3)
             RETURNING *",
            &[&"audit-1", &"auth.oauth.token.failure", &"hash"],
        )
        .await
        .unwrap();
    assert_eq!(audit.get::<_, String>("id"), "audit-1");
    assert_eq!(audit.get::<_, String>("action"), "auth.oauth.token.failure");

    client
        .batch_execute(
            r#"CREATE TABLE "ar_internal_metadata" (
                "key" character varying NOT NULL PRIMARY KEY,
                "value" character varying,
                "created_at" timestamp(6) NOT NULL,
                "updated_at" timestamp(6) NOT NULL
            );"#,
        )
        .await
        .unwrap();
    let metadata = client
        .query_one(
            r#"INSERT INTO "ar_internal_metadata" ("key", "value", "created_at", "updated_at")
               VALUES ($1, $2, $3::text::timestamp, $4::text::timestamp)
               RETURNING "key""#,
            &[
                &"gitlab_db_config_name",
                &"embedding",
                &"2026-06-21 08:09:54.990578",
                &"2026-06-21 08:09:54.990580",
            ],
        )
        .await
        .unwrap();
    assert_eq!(metadata.get::<_, String>("key"), "gitlab_db_config_name");

    drop(client);
    server.request_shutdown();
    connection_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn extended_dml_returning_uses_schema_expression_oids() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE app_overlays (
            id UUID PRIMARY KEY,
            org_id TEXT NOT NULL,
            hub_id UUID NOT NULL,
            app_name TEXT NOT NULL,
            patch JSONB NOT NULL,
            is_enabled BOOLEAN NOT NULL,
            updated_by UUID NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            UNIQUE (hub_id, app_name)
        );",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "insert_overlay",
        "INSERT INTO app_overlays (
            id, org_id, hub_id, app_name, patch, is_enabled,
            updated_by, created_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (hub_id, app_name) DO UPDATE
         SET patch = excluded.patch,
             is_enabled = excluded.is_enabled,
             updated_by = excluded.updated_by,
             updated_at = excluded.updated_at
         RETURNING app_overlays.id, org_id, hub_id, app_name, patch,
                   is_enabled, updated_by, created_at, updated_at",
        &[2950, 25, 2950, 25, 3802, 16, 2950, 1184, 1184],
    );
    let insert_params = [
        "00000000-0000-0000-0000-000000000001",
        "org-jsonb-probe",
        "00000000-0000-0000-0000-000000000002",
        "jsonb-probe",
        r#"{"title":"BicDB JSONB audit"}"#,
        "true",
        "00000000-0000-0000-0000-000000000003",
        "2026-07-09T12:00:00Z",
        "2026-07-09T12:00:01Z",
    ];
    send_bind(&mut client, "", "insert_overlay", &insert_params);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![2950, 25, 2950, 25, 3802, 16, 2950, 1184, 1184]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "00000000-0000-0000-0000-000000000001");
    assert_eq!(rows[0][4], r#"{"title": "BicDB JSONB audit"}"#);
    assert_eq!(rows[0][5], "t");

    let binary_params = insert_params
        .iter()
        .map(|value| (0, value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    send_bind_binary(&mut client, "", "insert_overlay", &binary_params, &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, _) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(oids, vec![2950, 25, 2950, 25, 3802, 16, 2950, 1184, 1184]);
    assert_eq!(formats, vec![1; 9]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 16);
    assert_eq!(rows[0][4].first(), Some(&1));
    assert_eq!(rows[0][5], vec![1]);
    assert_eq!(rows[0][7].len(), 8);

    send_parse(
        &mut client,
        "update_overlay",
        "UPDATE app_overlays AS overlay
         SET patch = $1
         WHERE id = $2
         RETURNING overlay.id,
                   overlay.patch::jsonb AS typed_patch,
                   overlay.patch ? 'title' AS has_title,
                   overlay.updated_at::text AS updated_text",
        &[3802, 2950],
    );
    send_bind(
        &mut client,
        "",
        "update_overlay",
        &[
            r#"{"title":"Updated"}"#,
            "00000000-0000-0000-0000-000000000001",
        ],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![2950, 3802, 16, 25]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], "t");

    send_parse(
        &mut client,
        "delete_overlay",
        "DELETE FROM app_overlays
         WHERE id = $1
         RETURNING app_overlays.id, app_overlays.patch,
                   app_overlays.is_enabled, app_overlays.updated_at",
        &[2950],
    );
    send_bind(
        &mut client,
        "",
        "delete_overlay",
        &["00000000-0000-0000-0000-000000000001"],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![2950, 3802, 16, 1184]);
    assert_eq!(rows.len(), 1);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_dml_returning_wildcards_keep_schema_oids_for_binary_results() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE returning_wildcard_probe (
            id UUID PRIMARY KEY,
            payload JSONB NOT NULL,
            enabled BOOLEAN NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )",
    );
    read_query_rows(&mut client);

    send_parse(
        &mut client,
        "insert_returning_star",
        r#"INSERT INTO returning_wildcard_probe (id, payload, enabled, updated_at)
           VALUES (
             '00000000-0000-0000-0000-000000000101',
             '{"items":[1,2]}'::jsonb,
             true,
             '2026-07-09T12:00:00Z'::timestamptz
           )
           RETURNING *"#,
        &[],
    );
    send_bind_binary(&mut client, "", "insert_returning_star", &[], &[1]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, _) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(oids, vec![2950, 3802, 16, 1184]);
    assert_eq!(formats, vec![1; 4]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 16);
    assert_eq!(rows[0][1].first(), Some(&1));
    assert_eq!(rows[0][2], vec![1]);
    assert_eq!(rows[0][3].len(), 8);

    send_parse(
        &mut client,
        "update_returning_qualified_star",
        r#"UPDATE returning_wildcard_probe AS probe
           SET payload = '{"items":[3,4]}'::jsonb
           WHERE id = '00000000-0000-0000-0000-000000000101'
           RETURNING probe.*"#,
        &[],
    );
    send_bind_binary(
        &mut client,
        "",
        "update_returning_qualified_star",
        &[],
        &[1],
    );
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (oids, formats, rows, _) = read_binary_query_result_with_oids(&mut client);
    assert_eq!(oids, vec![2950, 3802, 16, 1184]);
    assert_eq!(formats, vec![1; 4]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 16);
    assert_eq!(rows[0][1].first(), Some(&1));
    assert_eq!(rows[0][2], vec![1]);
    assert_eq!(rows[0][3].len(), 8);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn repeated_application_migration_count_bigint_returns_zero_row() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    let migration = "CREATE TABLE IF NOT EXISTS invitation_probe (\
        id UUID PRIMARY KEY, token_hash TEXT NOT NULL, deleted_at TIMESTAMPTZ\
    ); CREATE INDEX IF NOT EXISTS invitation_probe_token_idx \
        ON invitation_probe (token_hash)";
    send_query(&mut client, migration);
    read_until_ready(&mut client);
    send_query(&mut client, migration);
    read_until_ready(&mut client);

    // Regression: generated routes commonly execute this scalar aggregate
    // after an idempotent migration has been applied more than once.
    send_parse(
        &mut client,
        "count_invites",
        "SELECT COUNT(*)::bigint
         FROM invitation_probe
         WHERE token_hash = $1
           AND deleted_at IS NULL",
        &[25],
    );
    send_bind(&mut client, "", "count_invites", &["nonexistent-token"]);
    send_describe_portal(&mut client, "");
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (rows, oids) = read_query_rows_and_oids(&mut client);
    assert_eq!(oids, vec![20]);
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_reports_transaction_status_and_runs_prepared_inside_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_parse(
        &mut client,
        "insert_patient",
        "INSERT INTO patients (id, name) VALUES ($1, $2)",
        &[25, 25],
    );
    send_bind(&mut client, "", "insert_patient", &["p1", "Ada"]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "SELECT name FROM patients WHERE id = 'p1';");
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["Ada".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_transaction_foreign_keys_see_prior_pending_writes() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE categories (
            id SERIAL PRIMARY KEY,
            parent_id INTEGER REFERENCES categories ON DELETE SET NULL,
            name JSONB NOT NULL
        );",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(
        &mut client,
        "INSERT INTO categories (name, parent_id) VALUES ('{\"en_US\":\"Accounting\"}', NULL) RETURNING id;",
    );
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["1".to_string()]]);
    send_query(
        &mut client,
        "INSERT INTO categories (name, parent_id) VALUES ('{\"en_US\":\"Accounting\"}', 1) RETURNING id;",
    );
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["2".to_string()]]);
    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(
        &mut client,
        "SELECT id, parent_id FROM categories ORDER BY id;",
    );
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), String::new()],
            vec!["2".to_string(), "1".to_string()],
        ]
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_buffered_transaction_replays_split_schema_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(
        &mut client,
        r#"
        CREATE TABLE analytics_cycle_analytics_issue_stage_events (
            id bigint PRIMARY KEY,
            partition_id bigint NOT NULL
        ) PARTITION BY HASH (partition_id);

        ALTER INDEX analytics_cycle_analytics_issue_stage_events_pkey
            ATTACH PARTITION gitlab_partitions_static.analytics_cycle_analytics_issue_stage_events_00_pkey;

        CREATE TABLE gitlab_schema_probe (id text PRIMARY KEY);
        /*application:web,db_config_database:gitlabhq_development,line:/db/migrate/20211202041233_init_schema.rb:8*/
        "#,
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(
        &mut client,
        "COMMIT /*application:web,line:/lib/gitlab/database.rb:431*/;",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "SELECT COUNT(*) FROM gitlab_schema_probe;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["0".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_transaction_isolation_commands_accept_read_only_repeatable_read() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(
        &mut client,
        "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(
        &mut client,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(
        &mut client,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE;",
    );
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("0A000"));
    assert!(error.contains("transaction isolation level REPEATABLE READ requires READ ONLY"));

    send_query(&mut client, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("0A000"));
    assert!(error.contains("transaction isolation level SERIALIZABLE is not supported"));

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_accepts_discard_all_for_client_pool_resets() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "DISCARD ALL;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_read_committed_uses_transaction_snapshot_for_concurrent_commits() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut reader = TcpStream::connect(address).unwrap();
    startup(&mut reader);
    read_until_ready(&mut reader);
    let mut writer = TcpStream::connect(address).unwrap();
    startup(&mut writer);
    read_until_ready(&mut writer);

    send_query(
        &mut reader,
        "CREATE TABLE rc_patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut reader).1, b'I');

    send_query(&mut reader, "BEGIN ISOLATION LEVEL READ COMMITTED;");
    assert_eq!(read_query_rows_with_status(&mut reader).1, b'T');
    send_query(&mut reader, "SELECT COUNT(*) FROM rc_patients;");
    let (rows, status) = read_query_rows_with_status(&mut reader);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    send_query(&mut writer, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'T');
    send_query(
        &mut writer,
        "INSERT INTO rc_patients (id, name) VALUES ('p1', 'Committed');",
    );
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'T');

    send_query(&mut reader, "SELECT COUNT(*) FROM rc_patients;");
    let (rows, status) = read_query_rows_with_status(&mut reader);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    send_query(&mut writer, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'I');
    send_query(&mut reader, "SELECT COUNT(*) FROM rc_patients;");
    let (rows, status) = read_query_rows_with_status(&mut reader);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    send_query(&mut reader, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut reader).1, b'I');

    reader.write_all(b"X\0\0\0\x04").unwrap();
    writer.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_read_committed_updates_wait_for_the_row_owner_and_serialize() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut first = TcpStream::connect(address).unwrap();
    startup(&mut first);
    read_until_ready(&mut first);
    let mut second = TcpStream::connect(address).unwrap();
    startup(&mut second);
    read_until_ready(&mut second);

    send_query(
        &mut first,
        "CREATE TABLE erp_accounts (id TEXT PRIMARY KEY, balance INT);",
    );
    assert_eq!(read_query_rows_with_status(&mut first).1, b'I');
    send_query(
        &mut first,
        "INSERT INTO erp_accounts (id, balance) VALUES ('cash', 100);",
    );
    assert_eq!(read_query_rows_with_status(&mut first).1, b'I');

    send_query(&mut first, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut first).1, b'T');
    send_query(&mut second, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut second).1, b'T');

    send_query(
        &mut first,
        "UPDATE erp_accounts SET balance = 70 WHERE id = 'cash';",
    );
    assert_eq!(read_query_rows_with_status(&mut first).1, b'T');

    let waiting_update = thread::spawn(move || {
        send_query(
            &mut second,
            "UPDATE erp_accounts SET balance = 60 WHERE id = 'cash';",
        );
        let status = read_query_rows_with_status(&mut second).1;
        (second, status)
    });
    thread::sleep(Duration::from_millis(100));
    assert!(
        !waiting_update.is_finished(),
        "the competing UPDATE must wait while the first transaction owns the row"
    );

    send_query(&mut first, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut first).1, b'I');
    let (mut second, status) = waiting_update.join().unwrap();
    assert_eq!(status, b'T');
    send_query(&mut second, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut second).1, b'I');
    send_query(
        &mut first,
        "SELECT balance FROM erp_accounts WHERE id = 'cash';",
    );
    let (rows, status) = read_query_rows_with_status(&mut first);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["60".to_string()]]);

    first.write_all(b"X\0\0\0\x04").unwrap();
    second.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn pgwire_autocommit_select_function_waits_without_blocking_row_owner_commit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut owner = TcpStream::connect(address).unwrap();
    startup(&mut owner);
    read_until_ready(&mut owner);
    let mut cleanup = TcpStream::connect(address).unwrap();
    startup(&mut cleanup);
    read_until_ready(&mut cleanup);

    send_query(
        &mut owner,
        "CREATE TABLE context_nonces (nonce TEXT PRIMARY KEY, expires_at INT);",
    );
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'I');
    send_query(
        &mut owner,
        r#"CREATE FUNCTION clear_expired_context_nonces() RETURNS boolean
           LANGUAGE plpgsql AS $$
           BEGIN
             DELETE FROM context_nonces WHERE expires_at < 100;
             RETURN true;
           END
           $$;"#,
    );
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'I');
    send_query(
        &mut owner,
        "INSERT INTO context_nonces (nonce, expires_at) VALUES ('held', 1);",
    );
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'I');

    send_query(&mut owner, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'T');
    send_query(
        &mut owner,
        "DELETE FROM context_nonces WHERE nonce = 'held';",
    );
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'T');

    let waiting_cleanup = thread::spawn(move || {
        send_query(&mut cleanup, "SELECT clear_expired_context_nonces();");
        let result = read_query_rows_with_status(&mut cleanup);
        (cleanup, result)
    });
    thread::sleep(Duration::from_millis(100));
    assert!(
        !waiting_cleanup.is_finished(),
        "the cleanup function must wait while the explicit transaction owns the row"
    );

    // The waiting SELECT must hold only a shared database guard, otherwise this
    // COMMIT deadlocks behind it and eventually trips the global writer timeout.
    send_query(&mut owner, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut owner).1, b'I');
    let (mut cleanup, (rows, status)) = waiting_cleanup.join().unwrap();
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["t".to_string()]]);

    send_query(&mut owner, "SELECT COUNT(*) FROM context_nonces;");
    let (rows, status) = read_query_rows_with_status(&mut owner);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    owner.write_all(b"X\0\0\0\x04").unwrap();
    cleanup.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn failed_transaction_reports_error_status_and_recovers_on_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(&mut client, "SELECT * FROM missing_table;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("ERROR"));
    assert!(error.contains("42P01"));
    assert!(error.contains("collection not found"));

    send_query(&mut client, "SELECT 1;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("25P02"));
    assert!(error.contains("current transaction is aborted"));

    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "SELECT 1;");
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn failed_buffered_ddl_transaction_does_not_leak_created_tables() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(
        &mut client,
        "CREATE TABLE tx_leak_parent (id bigint PRIMARY KEY);",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(
        &mut client,
        "CREATE TABLE tx_leak_parent (id bigint PRIMARY KEY);",
    );
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("already exists"));

    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(
        &mut client,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_name LIKE 'tx_leak%' ORDER BY table_name;",
    );
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(
        rows.is_empty(),
        "failed transaction leaked tables: {rows:?}"
    );

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_savepoints_recover_failed_transaction_status() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "SAVEPOINT s1;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(&mut client, "SELECT * FROM missing_table;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("collection not found"));

    send_query(&mut client, "ROLLBACK TO SAVEPOINT s1;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "SELECT 1;");
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'T');
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    send_query(&mut client, "RELEASE SAVEPOINT s1;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "ROLLBACK TO SAVEPOINT s1;");
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'E');
    assert!(error.contains("3B001"));
    assert!(error.contains("savepoint \"s1\" does not exist"));

    send_query(&mut client, "ROLLBACK;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn pgwire_prepared_statement_inside_savepoint_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(
        &mut client,
        "CREATE TABLE savepoint_patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
    send_query(&mut client, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "SAVEPOINT before_insert;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_parse(
        &mut client,
        "insert_savepoint_patient",
        "INSERT INTO savepoint_patients (id, name) VALUES ($1, $2)",
        &[25, 25],
    );
    send_bind(
        &mut client,
        "",
        "insert_savepoint_patient",
        &["p1", "Rollback"],
    );
    send_execute(&mut client, "");
    send_sync(&mut client);
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');

    send_query(&mut client, "ROLLBACK TO SAVEPOINT before_insert;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'T');
    send_query(&mut client, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');

    send_query(&mut client, "SELECT COUNT(*) FROM savepoint_patients;");
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["0".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn extended_protocol_error_response_recovers_cleanly_on_sync() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let path = dir.path().to_path_buf();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        bicdb_pgwire::handle_client(stream, path).unwrap();
    });

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_bind(&mut client, "", "missing_statement", &[]);
    send_execute(&mut client, "");
    send_sync(&mut client);
    let (error, status) = read_error_response_with_status(&mut client);
    assert_eq!(status, b'I');
    assert!(error.contains("ERROR"));
    assert!(error.contains("08P01"));
    assert!(error.contains("prepared statement"));

    send_query(&mut client, "SELECT 1;");
    let (rows, status) = read_query_rows_with_status(&mut client);
    assert_eq!(status, b'I');
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.join().unwrap();
}

#[test]
fn server_runtime_handles_multiple_clients_and_status_tables() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 7,
        max_active_queries: 3,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    read_query_rows(&mut admin);

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT * FROM bicdb_server_connections;");
    let connections = read_query_rows(&mut client);
    assert!(connections.len() >= 2);
    assert!(connections.iter().all(|row| row.len() == 9));

    send_query(&mut client, "SELECT * FROM bicdb_server_stats;");
    let stats = read_query_rows(&mut client);
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0][0], "7");
    assert!(stats[0][1].parse::<usize>().unwrap() >= 2);
    assert_eq!(stats[0][5], "3");
    assert!(stats[0].len() >= 37);
    for value in &stats[0][22..=36] {
        value.parse::<u64>().unwrap();
    }
    let wal_write_calls = stats[0][32].parse::<u64>().unwrap();
    let wal_sync_calls = stats[0][33].parse::<u64>().unwrap();
    let wal_bytes_written = stats[0][34].parse::<u64>().unwrap();
    assert!(wal_write_calls > 0);
    assert_eq!(wal_sync_calls, wal_write_calls);
    assert!(wal_bytes_written > 0);

    send_query(
        &mut client,
        "SELECT sum(xact_commit + xact_rollback) FROM pg_stat_database;",
    );
    let xact_count = read_query_rows(&mut client);
    assert_eq!(xact_count.len(), 1);
    assert_eq!(xact_count[0].len(), 1);
    assert!(xact_count[0][0].parse::<usize>().is_ok());

    admin.write_all(b"X\0\0\0\x04").unwrap();
    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn concurrent_selects_use_shared_read_locks() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 16,
        max_active_queries: 16,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE read_patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');
    for id in 0..20 {
        send_query(
            &mut admin,
            &format!("INSERT INTO read_patients (id, name) VALUES ('p{id}', 'Patient {id}');"),
        );
        assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');
    }

    // Fast snapshot counts can finish before any other reader is scheduled.
    // Establish overlap with a session lock, not the incidental cost of COUNT.
    send_query(&mut admin, "SELECT pg_advisory_lock(980217);");
    read_query_rows(&mut admin);
    let before = server.stats_snapshot();
    let readers = 8;
    let barrier = Arc::new(Barrier::new(readers));
    let handles = (0..readers)
        .map(|_| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut client = TcpStream::connect(address).unwrap();
                startup(&mut client);
                read_until_ready(&mut client);
                barrier.wait();
                send_query(&mut client, "SELECT pg_advisory_lock_shared(980217);");
                read_query_rows(&mut client);
                send_query(&mut client, "SELECT COUNT(*) FROM read_patients;");
                let rows = read_query_rows(&mut client);
                client.write_all(b"X\0\0\0\x04").unwrap();
                rows
            })
        })
        .collect::<Vec<_>>();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut overlapped = false;
    while std::time::Instant::now() < deadline {
        if server.stats_snapshot().active_queries >= 2 {
            overlapped = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    // Disconnect releases the holder without needing a free query-admission
    // slot, even when every active reader is waiting for this lock.
    admin.write_all(b"X\0\0\0\x04").unwrap();
    assert!(overlapped, "reader admission never overlapped");
    for handle in handles {
        assert_eq!(handle.join().unwrap(), vec![vec!["20".to_string()]],);
    }
    let after = server.stats_snapshot();
    assert!(after.db_read_lock_acquisitions >= before.db_read_lock_acquisitions + readers as u64);
    assert_eq!(
        after.db_write_lock_acquisitions,
        before.db_write_lock_acquisitions
    );
    assert!(after.peak_active_queries > before.peak_active_queries);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn concurrent_mixed_writes_are_bounded_and_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 128,
        // Every client here is on loopback, so they all share one source IP
        // and the per-IP cap (20 by default) is the one that bites first --
        // not max_connections.
        max_connections_per_ip: 128,
        max_active_queries: 128,
        max_queued_queries: 128,
        max_queued_writes: 128,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE write_mix (id TEXT PRIMARY KEY, bucket INT, note TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');
    send_query(
        &mut admin,
        "CREATE INDEX write_mix_bucket_idx ON write_mix (bucket);",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');

    let clients = 100;
    let barrier = Arc::new(Barrier::new(clients));
    let ready_clients = (0..clients)
        .map(|_| {
            let mut client = TcpStream::connect(address).unwrap();
            startup(&mut client);
            read_until_ready(&mut client);
            client
        })
        .collect::<Vec<_>>();
    let handles = ready_clients
        .into_iter()
        .enumerate()
        .map(|(id, mut client)| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                send_query(
                    &mut client,
                    &format!(
                        "INSERT INTO write_mix (id, bucket, note) VALUES ('p{id}', {}, 'new');",
                        id % 10
                    ),
                );
                assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
                if id % 2 == 0 {
                    send_query(
                        &mut client,
                        &format!("UPDATE write_mix SET note = 'updated' WHERE id = 'p{id}';"),
                    );
                    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
                }
                if id % 5 == 0 {
                    send_query(
                        &mut client,
                        &format!("DELETE FROM write_mix WHERE id = 'p{id}';"),
                    );
                    assert_eq!(read_query_rows_with_status(&mut client).1, b'I');
                }
                client.write_all(b"X\0\0\0\x04").unwrap();
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
    }

    send_query(&mut admin, "SELECT COUNT(*) FROM write_mix;");
    assert_eq!(read_query_rows(&mut admin), vec![vec!["80".to_string()]]);
    send_query(
        &mut admin,
        "SELECT COUNT(*) FROM write_mix WHERE bucket = 1;",
    );
    assert_eq!(read_query_rows(&mut admin), vec![vec!["10".to_string()]]);
    send_query(
        &mut admin,
        "SELECT COUNT(*) FROM write_mix WHERE note = 'updated';",
    );
    assert_eq!(read_query_rows(&mut admin), vec![vec!["40".to_string()]]);
    let stats = server.stats_snapshot();
    assert!(stats.writes_executed >= 122);
    assert!(stats.write_queue_depth_max >= 1);
    assert!(stats.write_execution_total_ns > 0);

    admin.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    // The server handle owns the database; a directory has one writer.
    drop(server);

    let reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(reopened.scan_collection("write_mix").unwrap().len(), 80);
}

#[test]
fn concurrent_cognee_provenance_upserts_are_atomic_and_persistent() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 16,
        max_active_queries: 16,
        max_queued_queries: 16,
        max_queued_writes: 16,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config.clone()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE graph_node (
            id TEXT PRIMARY KEY,
            source_ref_keys TEXT[] NOT NULL DEFAULT '{}'
        );
        INSERT INTO graph_node (id) VALUES ('shared-node');",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');

    let writers = 8;
    let barrier = Arc::new(Barrier::new(writers));
    let clients = (0..writers)
        .map(|_| {
            let mut client = TcpStream::connect(address).unwrap();
            startup(&mut client);
            read_until_ready(&mut client);
            client
        })
        .collect::<Vec<_>>();
    let handles = clients
        .into_iter()
        .enumerate()
        .map(|(writer, mut client)| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                let source = format!("source-{}", writer % 4);
                barrier.wait();
                send_query(
                    &mut client,
                    &format!(
                        "INSERT INTO graph_node (id, source_ref_keys)
                         VALUES ('shared-node', ARRAY['{source}']::text[])
                         ON CONFLICT (id) DO UPDATE SET
                           source_ref_keys = CASE
                             WHEN '{source}' = ANY(graph_node.source_ref_keys)
                               THEN graph_node.source_ref_keys
                             ELSE array_append(graph_node.source_ref_keys, '{source}')
                           END;"
                    ),
                );
                let status = read_query_rows_with_status(&mut client).1;
                client.write_all(b"X\0\0\0\x04").unwrap();
                status
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), b'I');
    }

    let assertion = "SELECT cardinality(source_ref_keys),
                source_ref_keys @> ARRAY['source-0', 'source-1', 'source-2', 'source-3']::text[]
         FROM graph_node WHERE id = 'shared-node';";
    send_query(&mut admin, assertion);
    assert_eq!(
        read_query_rows(&mut admin),
        vec![vec!["4".to_string(), "t".to_string()]]
    );
    admin.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    drop(server);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let reopened = PgWireServer::open(dir.path(), config).unwrap();
    let reopened_for_thread = reopened.clone();
    let reopened_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(reopened_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, assertion);
    assert_eq!(
        read_query_rows(&mut client),
        vec![vec!["4".to_string(), "t".to_string()]]
    );
    client.write_all(b"X\0\0\0\x04").unwrap();
    reopened.request_shutdown();
    reopened_thread.join().unwrap().unwrap();
}

#[test]
fn bounded_write_queue_rejects_overload() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 8,
        max_active_queries: 8,
        max_queued_writes: 1,
        write_timeout: Duration::from_secs(5),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE write_pressure (id TEXT PRIMARY KEY, value INT);",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');

    let server_for_hold = server.clone();
    let hold_handle = thread::spawn(move || {
        server_for_hold
            .hold_write_admission_for_test(Duration::from_millis(300))
            .unwrap();
    });

    let mut saw_queued_write = false;
    for _ in 0..200 {
        if server.stats_snapshot().write_queue_depth == 1 {
            saw_queued_write = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert!(saw_queued_write);

    send_query(
        &mut admin,
        "INSERT INTO write_pressure (id, value) VALUES ('rejected', 1);",
    );
    let error = read_error_response(&mut admin);
    assert!(error.contains("too many queued writes"));

    hold_handle.join().unwrap();
    let stats = server.stats_snapshot();
    assert_eq!(stats.max_queued_writes, 1);
    assert!(stats.write_rejected_count >= 1);
    assert_eq!(stats.write_queue_depth, 0);

    admin.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn read_only_query_during_uncommitted_write_sees_committed_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut writer = TcpStream::connect(address).unwrap();
    startup(&mut writer);
    read_until_ready(&mut writer);
    let mut reader = TcpStream::connect(address).unwrap();
    startup(&mut reader);
    read_until_ready(&mut reader);

    send_query(
        &mut writer,
        "CREATE TABLE snapshot_patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'I');
    send_query(
        &mut writer,
        "INSERT INTO snapshot_patients (id, name) VALUES ('p0', 'Committed');",
    );
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'I');

    send_query(&mut writer, "BEGIN;");
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'T');
    send_query(
        &mut writer,
        "INSERT INTO snapshot_patients (id, name) VALUES ('p1', 'Uncommitted');",
    );
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'T');

    send_query(&mut reader, "SELECT COUNT(*) FROM snapshot_patients;");
    assert_eq!(read_query_rows(&mut reader), vec![vec!["1".to_string()]]);

    send_query(&mut writer, "COMMIT;");
    assert_eq!(read_query_rows_with_status(&mut writer).1, b'I');
    send_query(&mut reader, "SELECT COUNT(*) FROM snapshot_patients;");
    assert_eq!(read_query_rows(&mut reader), vec![vec!["2".to_string()]]);

    writer.write_all(b"X\0\0\0\x04").unwrap();
    reader.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn concurrent_vector_search_reads_remain_correct() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 12,
        max_active_queries: 12,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut admin = TcpStream::connect(address).unwrap();
    startup(&mut admin);
    read_until_ready(&mut admin);
    send_query(
        &mut admin,
        "CREATE TABLE memories (id TEXT PRIMARY KEY, content TEXT, embedding VECTOR(3));",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');
    send_query(
        &mut admin,
        "INSERT INTO memories (id, content, embedding) VALUES ('m1', 'clinic', '[1,0,0]');",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');
    send_query(
        &mut admin,
        "INSERT INTO memories (id, content, embedding) VALUES ('m2', 'other', '[0,1,0]');",
    );
    assert_eq!(read_query_rows_with_status(&mut admin).1, b'I');

    let readers = 6;
    let barrier = Arc::new(Barrier::new(readers));
    let handles = (0..readers)
        .map(|_| {
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut client = TcpStream::connect(address).unwrap();
                startup(&mut client);
                read_until_ready(&mut client);
                barrier.wait();
                send_query(
                    &mut client,
                    "SELECT id FROM memories ORDER BY embedding <=> '[1,0,0]' LIMIT 1;",
                );
                let rows = read_query_rows(&mut client);
                client.write_all(b"X\0\0\0\x04").unwrap();
                rows
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert_eq!(handle.join().unwrap(), vec![vec!["m1".to_string()]],);
    }

    admin.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn max_connections_and_active_query_limits_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 3,
        max_active_queries: 1,
        max_queued_queries: 1,
        max_active_reads: 1,
        max_queued_reads: 1,
        query_timeout: Duration::from_secs(5),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut first = TcpStream::connect(address).unwrap();
    startup(&mut first);
    read_until_ready(&mut first);
    let mut second = TcpStream::connect(address).unwrap();
    startup(&mut second);
    read_until_ready(&mut second);
    let mut third = TcpStream::connect(address).unwrap();
    startup(&mut third);
    read_until_ready(&mut third);

    let mut rejected = TcpStream::connect(address).unwrap();
    let error = read_startup_error(&mut rejected);
    assert!(error.contains("FATAL"));
    assert!(error.contains("53300"));
    assert!(error.contains("too many connections"));
    assert_eq!(server.stats_snapshot().rejected_connections, 1);

    let left_handle = thread::spawn(move || {
        send_query(&mut first, "SELECT pg_sleep(0.3);");
        let rows = read_query_rows(&mut first);
        first.write_all(b"X\0\0\0\x04").unwrap();
        rows
    });

    let mut saw_active = false;
    for _ in 0..50 {
        if server.stats_snapshot().active_queries == 1 {
            saw_active = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(saw_active);
    let mut saw_active_query_snapshot = false;
    for _ in 0..50 {
        let active_queries = server
            .connection_snapshots()
            .into_iter()
            .filter_map(|connection| connection.active_query)
            .collect::<Vec<_>>();
        // Introspection redacts literals, so the snapshot shows the shape of
        // the query and not its parameters. Assert both halves: the statement
        // is visible, and the literal it carried is not.
        if active_queries
            .iter()
            .any(|query| query.contains("pg_sleep") && !query.contains("0.3"))
        {
            saw_active_query_snapshot = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(saw_active_query_snapshot);

    let queued_handle = thread::spawn(move || {
        send_query(&mut second, "SELECT pg_sleep(0.1);");
        let rows = read_query_rows(&mut second);
        second.write_all(b"X\0\0\0\x04").unwrap();
        rows
    });

    let mut saw_queued = false;
    for _ in 0..100 {
        if server.stats_snapshot().queued_reads == 1 {
            saw_queued = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(saw_queued);

    send_query(&mut third, "SELECT 1;");
    let error = read_error_response(&mut third);
    assert!(error.contains("too many queued"));
    assert!(error.contains("53300"));
    third.write_all(b"X\0\0\0\x04").unwrap();

    assert_eq!(left_handle.join().unwrap(), vec![vec![String::new()]]);
    assert_eq!(queued_handle.join().unwrap(), vec![vec![String::new()]]);
    let stats = server.stats_snapshot();
    assert_eq!(stats.max_connections, 3);
    assert_eq!(stats.max_active_queries, 1);
    assert_eq!(stats.peak_active_queries, 1);
    assert_eq!(stats.queued_reads, 0);
    assert_eq!(stats.queued_reads_max, 1);
    assert_eq!(stats.rejected_queries, 1);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn server_query_timeout_and_unsupported_sql_return_errors() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        query_timeout: Duration::from_millis(1),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT pg_sleep(0.05);");
    let error = read_error_response(&mut client);
    assert!(error.contains("canceling statement due to query timeout"));
    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    // Shadowing the binding does not drop the old server; the directory has
    // one writer, so release it before reopening.
    drop(server);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig::default();
    assert_eq!(config.query_timeout, Duration::ZERO);
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT pg_sleep(0.05);");
    assert_eq!(read_query_rows(&mut client), vec![vec![String::new()]]);
    send_query(&mut client, "SELECT * FROM missing_table;");
    let error = read_error_response(&mut client);
    assert!(error.contains("collection not found"));
    assert!(error.contains("C42P01"));
    send_query(
        &mut client,
        "WITH RECURSIVE nums(n) AS (SELECT 1) SELECT n FROM nums;",
    );
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    send_query(&mut client, "SELECT ROW_NUMBER() OVER (ORDER BY 1);");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn idle_timeout_closes_inactive_connections() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        idle_timeout: Duration::from_millis(50),
        shutdown_grace_period: Duration::from_millis(500),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    thread::sleep(Duration::from_millis(350));

    let mut byte = [0_u8; 1];
    assert_eq!(client.read(&mut byte).unwrap(), 0);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn error_response_fields_cover_common_sql_failures() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);

    send_query(&mut client, "SELECT FROM");
    let error = read_error_response(&mut client);
    assert!(error.contains("SERROR"));
    assert!(error.contains("VERROR"));
    assert!(error.contains("C42601"));

    send_query(
        &mut client,
        "CREATE TABLE error_fields (id TEXT PRIMARY KEY, age INT NOT NULL)",
    );
    read_query_rows(&mut client);

    send_query(&mut client, "SELECT * FROM missing_table;");
    let error = read_error_response(&mut client);
    assert!(error.contains("C42P01"));
    assert!(error.contains("tmissing_table"));

    send_query(&mut client, "SELECT missing_column FROM error_fields;");
    let error = read_error_response(&mut client);
    assert!(error.contains("C42703"));
    assert!(error.contains("terror_fields"));
    assert!(error.contains("cmissing_column"));

    send_query(
        &mut client,
        "INSERT INTO error_fields (id, age) VALUES ('p1', 'old');",
    );
    let error = read_error_response(&mut client);
    assert!(error.contains("C22P02"));
    assert!(error.contains("terror_fields"));
    assert!(error.contains("cage"));
    assert!(error.contains("dint"));

    send_query(
        &mut client,
        "INSERT INTO error_fields (id, age) VALUES ('p2', NULL);",
    );
    let error = read_error_response(&mut client);
    assert!(error.contains("C23502"));
    assert!(error.contains("terror_fields"));
    assert!(error.contains("cage"));
    assert!(error.contains("nerror_fields_age_not_null"));

    send_query(
        &mut client,
        "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM error_fields;",
    );
    assert!(read_query_rows(&mut client).is_empty());

    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn graceful_shutdown_flushes_writes() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(
        &mut client,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    read_query_rows(&mut client);
    send_query(
        &mut client,
        "INSERT INTO patients (id, name) VALUES ('p1', 'Ada');",
    );
    read_query_rows(&mut client);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    assert!(dir
        .path()
        .join(bicdb_pgwire::CLEAN_SHUTDOWN_MARKER)
        .exists());

    // The server handle owns the database; the directory has one writer, so it
    // must be released before this reopen.
    drop(server);
    let db = BicDb::open(dir.path()).unwrap();
    assert!(db.get("patients", "p1").unwrap().is_some());
}

#[test]
fn graceful_shutdown_drains_many_idle_and_active_clients() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 64,
        // All clients are on loopback and share a source IP, so the per-IP cap
        // is what refuses first at its default of 20.
        max_connections_per_ip: 64,
        max_active_queries: 4,
        max_active_reads: 4,
        shutdown_grace_period: Duration::from_secs(2),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut idle_clients = Vec::new();
    for _ in 0..20 {
        let mut client = TcpStream::connect(address).unwrap();
        startup(&mut client);
        read_until_ready(&mut client);
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        idle_clients.push(client);
    }

    // Hold a session lock so workers cannot finish before the observer gets
    // scheduled. A short pg_sleep made this a race on loaded CI runners.
    send_query(&mut idle_clients[0], "SELECT pg_advisory_lock(580014);");
    read_query_rows(&mut idle_clients[0]);
    let active_handles = (0..4)
        .map(|_| {
            thread::spawn(move || {
                let mut client = TcpStream::connect(address).unwrap();
                startup(&mut client);
                read_until_ready(&mut client);
                send_query(&mut client, "SELECT pg_advisory_lock(580014);");
                let rows = read_query_rows(&mut client);
                assert_eq!(rows, vec![vec![String::new()]]);
            })
        })
        .collect::<Vec<_>>();

    let mut saw_active = false;
    let active_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < active_deadline {
        if server.stats_snapshot().active_queries == 4 {
            saw_active = true;
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(saw_active);

    server.request_shutdown();
    // Closing the idle lock holder releases the workers only after shutdown
    // begins. Each worker's disconnect releases it for the next waiter.
    for client in &mut idle_clients {
        let _ = client.write_all(b"X\0\0\0\x04");
    }
    for handle in active_handles {
        handle.join().unwrap();
    }
    server_thread.join().unwrap().unwrap();
    for mut client in idle_clients {
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).unwrap(), 0);
    }
    let stats = server.stats_snapshot();
    assert!(stats.peak_active_connections >= 24);
    assert_eq!(stats.active_connections, 0);
}

#[test]
fn concurrent_clients_can_write_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut setup = TcpStream::connect(address).unwrap();
    startup(&mut setup);
    read_until_ready(&mut setup);
    send_query(
        &mut setup,
        "CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT);",
    );
    read_query_rows(&mut setup);
    setup.write_all(b"X\0\0\0\x04").unwrap();

    let handles = (0..4)
        .map(|idx| {
            thread::spawn(move || {
                let mut client = TcpStream::connect(address).unwrap();
                startup(&mut client);
                read_until_ready(&mut client);
                send_query(
                    &mut client,
                    &format!("INSERT INTO patients (id, name) VALUES ('p{idx}', 'name-{idx}');"),
                );
                read_query_rows(&mut client);
                send_query(&mut client, "SELECT COUNT(*) FROM patients;");
                let rows = read_query_rows(&mut client);
                assert!(!rows.is_empty());
                client.write_all(b"X\0\0\0\x04").unwrap();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    // The server handle owns the database; the directory has one writer, so it
    // must be released before this reopen.
    drop(server);
    let db = BicDb::open(dir.path()).unwrap();
    assert_eq!(db.scan_collection("patients").unwrap().len(), 4);
}

#[test]
fn password_auth_accepts_valid_user_and_rejects_bad_password() {
    let dir = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(dir.path(), "admin", "correct").unwrap();
    assert!(bicdb_pgwire::user_exists(dir.path(), "admin").unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        require_auth: true,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup_with_scram(&mut client, "admin", "correct");
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();

    let mut bad_client = TcpStream::connect(address).unwrap();
    send_startup(&mut bad_client, "admin");
    send_scram_exchange(&mut bad_client, "admin", "wrong", "n,,", false);
    let error = read_startup_error(&mut bad_client);
    assert!(error.contains("password authentication failed"));

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn scram_auth_accepts_valid_user() {
    let dir = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(dir.path(), "admin", "correct").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        require_auth: true,
        auth_method: AuthMethod::ScramSha256,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup_with_scram(&mut client, "admin", "correct");
    read_until_ready(&mut client);
    send_query(&mut client, "SELECT 1;");
    assert_eq!(read_query_rows(&mut client), vec![vec!["1".to_string()]]);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn scram_auth_rejects_bad_password_and_closes_connection() {
    let dir = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(dir.path(), "admin", "correct").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        require_auth: true,
        auth_method: AuthMethod::ScramSha256,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "admin");
    send_scram_exchange(&mut client, "admin", "wrong", "n,,", false);
    let error = read_startup_error(&mut client);
    assert!(error.contains("FATAL"));
    assert!(error.contains("28P01"));
    assert!(error.contains("password authentication failed"));
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn scram_auth_rejects_channel_binding_client_first() {
    let dir = tempfile::tempdir().unwrap();
    bicdb_pgwire::create_user(dir.path(), "admin", "correct").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        require_auth: true,
        auth_method: AuthMethod::ScramSha256,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "admin");
    let (code, payload) = read_auth_payload(&mut client);
    assert_eq!(code, 10);
    assert!(String::from_utf8_lossy(&payload[4..]).contains("SCRAM-SHA-256"));
    let client_first_bare = "n=admin,r=clientnonce";
    let client_first = format!("p=tls-server-end-point,,{client_first_bare}");
    send_sasl_initial_response(&mut client, "SCRAM-SHA-256", &client_first);
    let error = read_startup_error(&mut client);
    assert!(error.contains("FATAL"));
    assert!(error.contains("28P01"));
    assert!(error.contains("password authentication failed"));
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn tls_config_accepts_postgres_ssl_request() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, TEST_CERT).unwrap();
    std::fs::write(&key_path, TEST_KEY).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        tls_cert: Some(cert_path),
        tls_key: Some(key_path),
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_ssl_request(&mut client);
    let mut response = [0_u8; 1];
    client.read_exact(&mut response).unwrap();
    assert_eq!(response[0], b'S');

    server.request_shutdown();
    drop(client);
    server_thread.join().unwrap().unwrap();
}

#[test]
fn require_tls_rejects_plain_startup_and_closes_connection() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, TEST_CERT).unwrap();
    std::fs::write(&key_path, TEST_KEY).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        tls_cert: Some(cert_path),
        tls_key: Some(key_path),
        require_tls: true,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    send_startup(&mut client, "bicdb");
    let error = read_startup_error(&mut client);
    assert!(error.contains("FATAL"));
    assert!(error.contains("28000"));
    assert!(error.contains("TLS is required"));
    assert_connection_closed(&mut client);

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn require_tls_requires_server_certificate_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let config = PgWireConfig {
        require_tls: true,
        ..PgWireConfig::default()
    };
    let error = PgWireServer::open(dir.path(), config).unwrap_err();
    assert!(error
        .to_string()
        .contains("require_tls needs both tls_cert and tls_key"));
}

#[test]
fn client_certificate_configuration_is_explicitly_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let config = PgWireConfig {
        tls_client_ca: Some(dir.path().join("ca.pem")),
        ..PgWireConfig::default()
    };
    let error = PgWireServer::open(dir.path(), config).unwrap_err();
    assert!(error
        .to_string()
        .contains("client certificate authentication is not supported"));
}

fn startup(stream: &mut TcpStream) {
    send_startup(stream, "bicdb");
}

fn startup_with_password(stream: &mut TcpStream, user: &str, password: &str) {
    send_startup(stream, user);
    assert_eq!(read_auth_code(stream), 3);
    send_password(stream, password);
}

fn startup_with_scram(stream: &mut TcpStream, user: &str, password: &str) {
    send_startup(stream, user);
    send_scram_exchange(stream, user, password, "n,,", true);
}

fn send_scram_exchange(
    stream: &mut TcpStream,
    user: &str,
    password: &str,
    gs2_header: &str,
    expect_success: bool,
) {
    let (code, payload) = read_auth_payload(stream);
    assert_eq!(code, 10);
    assert!(String::from_utf8_lossy(&payload[4..]).contains("SCRAM-SHA-256"));

    let client_nonce = "clientnonce";
    let client_first_bare = format!("n={user},r={client_nonce}");
    let client_first = format!("{gs2_header}{client_first_bare}");
    send_sasl_initial_response(stream, "SCRAM-SHA-256", &client_first);

    let (code, payload) = read_auth_payload(stream);
    assert_eq!(code, 11);
    let server_first = String::from_utf8(payload[4..].to_vec()).unwrap();
    let nonce = scram_attr(&server_first, "r");
    let salt = BASE64.decode(scram_attr(&server_first, "s")).unwrap();
    let iterations = scram_attr(&server_first, "i").parse::<u32>().unwrap();
    assert!(nonce.starts_with(client_nonce));

    let client_final_without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{client_final_without_proof}");
    let mut salted_password = [0_u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted_password);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = Sha256::digest(&client_key);
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
    let proof = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(key, signature)| key ^ signature)
        .collect::<Vec<_>>();
    let client_final = format!("{client_final_without_proof},p={}", BASE64.encode(proof));
    send_message(stream, b'p', client_final.as_bytes());

    if expect_success {
        let (code, payload) = read_auth_payload(stream);
        assert_eq!(code, 12);
        assert!(String::from_utf8_lossy(&payload[4..]).starts_with("v="));
    }
}

fn send_sasl_initial_response(stream: &mut TcpStream, mechanism: &str, client_first: &str) {
    let mut initial = Vec::new();
    cstr(&mut initial, mechanism);
    put_i32(&mut initial, client_first.len() as i32);
    initial.extend_from_slice(client_first.as_bytes());
    send_message(stream, b'p', &initial);
}

fn send_startup(stream: &mut TcpStream, user: &str) {
    send_startup_version_for_user(stream, 196_608, user, true);
}

fn send_startup_for_database(stream: &mut TcpStream, user: &str, database: &str) {
    let mut payload = Vec::new();
    put_i32(&mut payload, 196_608);
    cstr(&mut payload, "user");
    cstr(&mut payload, user);
    cstr(&mut payload, "database");
    cstr(&mut payload, database);
    payload.push(0);
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn send_startup_version(stream: &mut TcpStream, version: i32, terminated: bool) {
    send_startup_version_for_user(stream, version, "bicdb", terminated);
}

fn send_startup_version_for_user(
    stream: &mut TcpStream,
    version: i32,
    user: &str,
    terminated: bool,
) {
    send_startup_version_with_options_for_user(stream, version, user, terminated, &[]);
}

fn send_startup_version_with_options(
    stream: &mut TcpStream,
    version: i32,
    terminated: bool,
    options: &[(&str, &str)],
) {
    send_startup_version_with_options_for_user(stream, version, "bicdb", terminated, options);
}

fn send_startup_version_with_options_for_user(
    stream: &mut TcpStream,
    version: i32,
    user: &str,
    terminated: bool,
    options: &[(&str, &str)],
) {
    let mut payload = Vec::new();
    put_i32(&mut payload, version);
    cstr(&mut payload, "user");
    cstr(&mut payload, user);
    cstr(&mut payload, "database");
    cstr(&mut payload, "bicdb");
    for (key, value) in options {
        cstr(&mut payload, key);
        cstr(&mut payload, value);
    }
    if terminated {
        payload.push(0);
    }

    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn read_negotiate_protocol_version(stream: &mut TcpStream) -> (i32, Vec<String>) {
    let (tag, payload) = read_message(stream);
    assert_eq!(tag, b'v');
    let protocol_version = i32::from_be_bytes(payload[0..4].try_into().unwrap());
    let option_count = i32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
    let mut idx = 8;
    let mut options = Vec::new();
    for _ in 0..option_count {
        let end = payload[idx..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| idx + offset)
            .unwrap();
        options.push(String::from_utf8(payload[idx..end].to_vec()).unwrap());
        idx = end + 1;
    }
    (protocol_version, options)
}

fn send_ssl_request(stream: &mut TcpStream) {
    stream.write_all(&8_i32.to_be_bytes()).unwrap();
    stream.write_all(&80_877_103_i32.to_be_bytes()).unwrap();
}

fn read_auth_code(stream: &mut TcpStream) -> i32 {
    read_auth_payload(stream).0
}

fn read_auth_payload(stream: &mut TcpStream) -> (i32, Vec<u8>) {
    let (tag, payload) = read_message(stream);
    assert_eq!(tag, b'R');
    (
        i32::from_be_bytes(payload[0..4].try_into().unwrap()),
        payload,
    )
}

fn send_password(stream: &mut TcpStream, password: &str) {
    let mut payload = password.as_bytes().to_vec();
    payload.push(0);
    send_message(stream, b'p', &payload);
}

fn send_query(stream: &mut TcpStream, query: &str) {
    let mut payload = query.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(b"Q").unwrap();
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
}

fn send_parse(stream: &mut TcpStream, name: &str, sql: &str, oids: &[i32]) {
    let mut payload = Vec::new();
    cstr(&mut payload, name);
    cstr(&mut payload, sql);
    put_i16(&mut payload, oids.len() as i16);
    for oid in oids {
        put_i32(&mut payload, *oid);
    }
    send_message(stream, b'P', &payload);
}

fn send_bind(stream: &mut TcpStream, portal: &str, statement: &str, params: &[&str]) {
    let mut payload = Vec::new();
    cstr(&mut payload, portal);
    cstr(&mut payload, statement);
    put_i16(&mut payload, 0);
    put_i16(&mut payload, params.len() as i16);
    for param in params {
        put_i32(&mut payload, param.len() as i32);
        payload.extend_from_slice(param.as_bytes());
    }
    put_i16(&mut payload, 0);
    send_message(stream, b'B', &payload);
}

fn send_bind_binary(
    stream: &mut TcpStream,
    portal: &str,
    statement: &str,
    params: &[(i16, Vec<u8>)],
    result_formats: &[i16],
) {
    let mut payload = Vec::new();
    cstr(&mut payload, portal);
    cstr(&mut payload, statement);
    put_i16(&mut payload, params.len() as i16);
    for (format, _) in params {
        put_i16(&mut payload, *format);
    }
    put_i16(&mut payload, params.len() as i16);
    for (_, param) in params {
        put_i32(&mut payload, param.len() as i32);
        payload.extend_from_slice(param);
    }
    put_i16(&mut payload, result_formats.len() as i16);
    for format in result_formats {
        put_i16(&mut payload, *format);
    }
    send_message(stream, b'B', &payload);
}

fn binary_array_payload(element_oid: i32, elements: &[Option<&Vec<u8>>]) -> Vec<u8> {
    binary_array_payload_with_dimensions(element_oid, &[(elements.len(), 1)], elements)
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn binary_array_payload_with_dimensions(
    element_oid: i32,
    dimensions: &[(usize, i32)],
    elements: &[Option<&Vec<u8>>],
) -> Vec<u8> {
    let mut payload = Vec::new();
    put_i32(&mut payload, dimensions.len() as i32);
    put_i32(
        &mut payload,
        i32::from(elements.iter().any(Option::is_none)),
    );
    put_i32(&mut payload, element_oid);
    for (length, lower_bound) in dimensions {
        put_i32(&mut payload, *length as i32);
        put_i32(&mut payload, *lower_bound);
    }
    for element in elements {
        match element {
            Some(element) => {
                put_i32(&mut payload, element.len() as i32);
                payload.extend_from_slice(element);
            }
            None => put_i32(&mut payload, -1),
        }
    }
    payload
}

fn structured_binary_codec_cases() -> Vec<(i32, Vec<u8>)> {
    let floats = |values: &[f64]| {
        values
            .iter()
            .flat_map(|value| value.to_be_bytes())
            .collect::<Vec<_>>()
    };
    let range = |bounds: &[Vec<u8>]| {
        let mut payload = vec![0x02];
        for bound in bounds {
            payload.extend_from_slice(&(bound.len() as i32).to_be_bytes());
            payload.extend_from_slice(bound);
        }
        payload
    };
    let multirange = |range: &[u8]| {
        [
            1_i32.to_be_bytes().as_slice(),
            (range.len() as i32).to_be_bytes().as_slice(),
            range,
        ]
        .concat()
    };

    let mut cidr = vec![2, 24, 1, 4];
    cidr.extend([192, 0, 2, 0]);
    let mut inet = vec![3, 64, 0, 16];
    inet.extend([0x20, 0x01, 0x0d, 0xb8]);
    inet.extend([0; 11]);
    inet.push(1);

    let mut path = vec![0];
    path.extend_from_slice(&2_i32.to_be_bytes());
    path.extend(floats(&[1.0, 2.0, 3.0, 4.0]));
    let mut polygon = 3_i32.to_be_bytes().to_vec();
    polygon.extend(floats(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));

    let int4range = range(&[1_i32.to_be_bytes().to_vec(), 3_i32.to_be_bytes().to_vec()]);
    let numrange = range(&[
        numeric_binary(1, 0, 0, 0, &[1]),
        numeric_binary(1, 0, 0, 0, &[3]),
    ]);
    let tsrange = range(&[0_i64.to_be_bytes().to_vec(), 42_i64.to_be_bytes().to_vec()]);
    let tstzrange = range(&[43_i64.to_be_bytes().to_vec(), 84_i64.to_be_bytes().to_vec()]);
    let daterange = range(&[0_i32.to_be_bytes().to_vec(), 1_i32.to_be_bytes().to_vec()]);
    let int8range = range(&[1_i64.to_be_bytes().to_vec(), 3_i64.to_be_bytes().to_vec()]);

    let snapshot = [
        2_i32.to_be_bytes().as_slice(),
        10_u64.to_be_bytes().as_slice(),
        20_u64.to_be_bytes().as_slice(),
        12_u64.to_be_bytes().as_slice(),
        15_u64.to_be_bytes().as_slice(),
    ]
    .concat();
    let int2_values = [
        Some(1_i16.to_be_bytes().to_vec()),
        Some(2_i16.to_be_bytes().to_vec()),
        Some((-3_i16).to_be_bytes().to_vec()),
    ];
    let int2vector = binary_array_payload_with_dimensions(
        21,
        &[(int2_values.len(), 0)],
        &int2_values.iter().map(Option::as_ref).collect::<Vec<_>>(),
    );
    let oid_values = [
        Some(1_u32.to_be_bytes().to_vec()),
        Some(2_u32.to_be_bytes().to_vec()),
        Some(u32::MAX.to_be_bytes().to_vec()),
    ];
    let oidvector = binary_array_payload_with_dimensions(
        26,
        &[(oid_values.len(), 0)],
        &oid_values.iter().map(Option::as_ref).collect::<Vec<_>>(),
    );

    let mut cases = vec![
        (22, int2vector),
        (
            27,
            [
                1_u32.to_be_bytes().as_slice(),
                2_u16.to_be_bytes().as_slice(),
            ]
            .concat(),
        ),
        (28, u32::MAX.to_be_bytes().to_vec()),
        (29, 42_u32.to_be_bytes().to_vec()),
        (30, oidvector),
        (1790, b"portal name".to_vec()),
        (650, cidr),
        (869, inet),
        (774, vec![8, 0, 43, 1, 2, 3, 4, 5]),
        (829, vec![8, 0, 43, 1, 2, 3]),
        (600, floats(&[1.0, 2.0])),
        (601, floats(&[1.0, 2.0, 3.0, 4.0])),
        (602, path),
        (603, floats(&[3.0, 4.0, 1.0, 2.0])),
        (604, polygon),
        (628, floats(&[1.0, 2.0, 3.0])),
        (718, floats(&[1.0, 2.0, 3.0])),
        (3220, 0x0000_0016_B374_D848_u64.to_be_bytes().to_vec()),
        (3904, int4range.clone()),
        (3906, numrange.clone()),
        (3908, tsrange.clone()),
        (3910, tstzrange.clone()),
        (3912, daterange.clone()),
        (3926, int8range.clone()),
        (4451, multirange(&int4range)),
        (4532, multirange(&numrange)),
        (4533, multirange(&tsrange)),
        (4534, multirange(&tstzrange)),
        (4535, multirange(&daterange)),
        (4536, multirange(&int8range)),
        (2970, snapshot.clone()),
        (5038, snapshot),
        (5069, u64::MAX.to_be_bytes().to_vec()),
    ];
    for oid in [
        24, 26, 2202, 2203, 2204, 2205, 2206, 3734, 3769, 4089, 4096, 4191,
    ] {
        cases.push((oid, 42_u32.to_be_bytes().to_vec()));
    }
    cases
}

fn numeric_binary(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
    assert_eq!(usize::from(ndigits), digits.len());
    let mut payload = Vec::with_capacity(8 + digits.len() * 2);
    payload.extend_from_slice(&ndigits.to_be_bytes());
    payload.extend_from_slice(&weight.to_be_bytes());
    payload.extend_from_slice(&sign.to_be_bytes());
    payload.extend_from_slice(&dscale.to_be_bytes());
    for digit in digits {
        payload.extend_from_slice(&digit.to_be_bytes());
    }
    payload
}

fn interval_binary(micros: i64, days: i32, months: i32) -> Vec<u8> {
    [
        micros.to_be_bytes().as_slice(),
        days.to_be_bytes().as_slice(),
        months.to_be_bytes().as_slice(),
    ]
    .concat()
}

fn timetz_binary(micros: i64, seconds_west_of_utc: i32) -> Vec<u8> {
    [
        micros.to_be_bytes().as_slice(),
        seconds_west_of_utc.to_be_bytes().as_slice(),
    ]
    .concat()
}

fn bit_string_binary(bits: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + bits.len().div_ceil(8));
    payload.extend_from_slice(&(bits.len() as i32).to_be_bytes());
    payload.resize(4 + bits.len().div_ceil(8), 0);
    for (index, bit) in bits.bytes().enumerate() {
        assert!(matches!(bit, b'0' | b'1'));
        if bit == b'1' {
            payload[4 + index / 8] |= 1 << (7 - index % 8);
        }
    }
    payload
}

fn vector_binary(values: &[f32]) -> Vec<u8> {
    let mut output = Vec::with_capacity(4 + values.len() * 4);
    output.extend_from_slice(&(values.len() as i16).to_be_bytes());
    output.extend_from_slice(&0_i16.to_be_bytes());
    for value in values {
        output.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    output
}

fn send_describe_portal(stream: &mut TcpStream, portal: &str) {
    let mut payload = Vec::new();
    payload.push(b'P');
    cstr(&mut payload, portal);
    send_message(stream, b'D', &payload);
}

fn send_describe_statement(stream: &mut TcpStream, statement: &str) {
    let mut payload = Vec::new();
    payload.push(b'S');
    cstr(&mut payload, statement);
    send_message(stream, b'D', &payload);
}

fn read_parameter_description(stream: &mut TcpStream) -> Vec<i32> {
    let (tag, payload) = read_message(stream);
    assert_eq!(tag, b't');
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    assert_eq!(payload.len(), 2 + count * 4);
    (0..count)
        .map(|idx| {
            let start = 2 + idx * 4;
            i32::from_be_bytes(payload[start..start + 4].try_into().unwrap())
        })
        .collect()
}

fn send_execute(stream: &mut TcpStream, portal: &str) {
    send_execute_max(stream, portal, 0);
}

fn send_execute_max(stream: &mut TcpStream, portal: &str, max_rows: i32) {
    let mut payload = Vec::new();
    cstr(&mut payload, portal);
    put_i32(&mut payload, max_rows);
    send_message(stream, b'E', &payload);
}

fn send_close(stream: &mut TcpStream, target: u8, name: &str) {
    let mut payload = Vec::new();
    payload.push(target);
    cstr(&mut payload, name);
    send_message(stream, b'C', &payload);
}

fn send_sync(stream: &mut TcpStream) {
    send_message(stream, b'S', &[]);
}

fn send_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    stream.write_all(&[tag]).unwrap();
    stream
        .write_all(&((payload.len() as i32) + 4).to_be_bytes())
        .unwrap();
    stream.write_all(payload).unwrap();
}

fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let (tag, _) = read_message(stream);
        if tag == b'Z' {
            break;
        }
    }
}

fn read_startup_parameters_until_ready(stream: &mut TcpStream) -> HashMap<String, String> {
    let mut parameters = HashMap::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'S' => {
                let mut fields = payload.split(|byte| *byte == 0);
                let name = fields.next().unwrap_or_default();
                let value = fields.next().unwrap_or_default();
                parameters.insert(
                    String::from_utf8(name.to_vec()).unwrap(),
                    String::from_utf8(value.to_vec()).unwrap(),
                );
            }
            b'Z' => return parameters,
            _ => {}
        }
    }
}

fn read_backend_key_until_ready(stream: &mut TcpStream) -> (i32, i32) {
    let mut backend_key = None;
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'K' => {
                backend_key = Some((
                    i32::from_be_bytes(payload[0..4].try_into().unwrap()),
                    i32::from_be_bytes(payload[4..8].try_into().unwrap()),
                ));
            }
            b'Z' => return backend_key.expect("startup did not return BackendKeyData"),
            _ => {}
        }
    }
}

fn read_query_rows(stream: &mut TcpStream) -> Vec<Vec<String>> {
    read_query_rows_with_status(stream).0
}

fn read_query_rows_and_oids(stream: &mut TcpStream) -> (Vec<Vec<String>>, Vec<i32>) {
    let mut rows = Vec::new();
    let mut oids = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'T' => oids = parse_row_description_oids(&payload),
            b'D' => rows.push(parse_data_row(&payload)),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => return (rows, oids),
            _ => {}
        }
    }
}

fn read_query_rows_with_status(stream: &mut TcpStream) -> (Vec<Vec<String>>, u8) {
    let mut rows = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'D' => rows.push(parse_data_row(&payload)),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => return (rows, payload[0]),
            _ => {}
        }
    }
}

fn read_execute_rows(stream: &mut TcpStream) -> (Vec<Vec<String>>, bool) {
    let mut rows = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'D' => rows.push(parse_data_row(&payload)),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b's' => return (rows, true),
            b'C' => return (rows, false),
            _ => {}
        }
    }
}

fn read_binary_query_result(stream: &mut TcpStream) -> (Vec<i16>, Vec<Vec<Vec<u8>>>, u8) {
    let (_, formats, rows, status) = read_binary_query_result_with_oids(stream);
    (formats, rows, status)
}

fn read_binary_query_result_with_oids(
    stream: &mut TcpStream,
) -> (Vec<i32>, Vec<i16>, Vec<Vec<Vec<u8>>>, u8) {
    let mut oids = Vec::new();
    let mut formats = Vec::new();
    let mut rows = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'T' => {
                oids = parse_row_description_oids(&payload);
                formats = parse_row_description_formats(&payload);
            }
            b'D' => rows.push(parse_binary_data_row(&payload)),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => return (oids, formats, rows, payload[0]),
            _ => {}
        }
    }
}

fn read_error_response(stream: &mut TcpStream) -> String {
    read_error_response_with_status(stream).0
}

fn read_error_response_with_status(stream: &mut TcpStream) -> (String, u8) {
    let mut message = String::new();
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'E' => {
                message = String::from_utf8_lossy(&payload).to_string();
            }
            b'Z' => return (message, payload[0]),
            _ => {}
        }
    }
}

fn read_copy_out(stream: &mut TcpStream) -> (Vec<String>, u8) {
    let mut rows = Vec::new();
    let mut saw_copy_out = false;
    loop {
        let (tag, payload) = read_message(stream);
        match tag {
            b'H' => saw_copy_out = true,
            b'd' => rows.push(String::from_utf8(payload).unwrap()),
            b'E' => panic!("server error: {}", String::from_utf8_lossy(&payload)),
            b'Z' => {
                assert!(saw_copy_out);
                return (rows, payload[0]);
            }
            _ => {}
        }
    }
}

fn read_tags_until_ready(stream: &mut TcpStream) -> (Vec<u8>, u8) {
    let mut tags = Vec::new();
    loop {
        let (tag, payload) = read_message(stream);
        if tag == b'Z' {
            return (tags, payload[0]);
        }
        tags.push(tag);
    }
}

fn read_startup_error(stream: &mut TcpStream) -> String {
    loop {
        let (tag, payload) = read_message(stream);
        if tag == b'E' {
            return String::from_utf8_lossy(&payload).to_string();
        }
    }
}

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).unwrap();
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).unwrap();
    let len = i32::from_be_bytes(len);
    let mut payload = vec![0_u8; (len - 4) as usize];
    stream.read_exact(&mut payload).unwrap();
    (tag[0], payload)
}

fn assert_connection_closed(stream: &mut TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut byte = [0_u8; 1];
    assert_eq!(stream.read(&mut byte).unwrap(), 0);
}

fn parse_data_row(payload: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    let mut idx = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
        idx += 4;
        if len < 0 {
            values.push(String::new());
            continue;
        }
        let end = idx + len as usize;
        values.push(String::from_utf8(payload[idx..end].to_vec()).unwrap());
        idx = end;
    }
    values
}

#[derive(Debug, PartialEq, Eq)]
struct RowDescriptionField {
    name: String,
    table_oid: i32,
    attribute_number: i16,
    type_oid: i32,
    type_size: i16,
    type_modifier: i32,
    format: i16,
}

impl RowDescriptionField {
    fn new(
        name: &str,
        table_oid: i32,
        attribute_number: i16,
        type_oid: i32,
        type_size: i16,
        type_modifier: i32,
        format: i16,
    ) -> Self {
        Self {
            name: name.to_string(),
            table_oid,
            attribute_number,
            type_oid,
            type_size,
            type_modifier,
            format,
        }
    }
}

fn parse_row_description_fields(payload: &[u8]) -> Vec<RowDescriptionField> {
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    let mut idx = 2;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name_end = payload[idx..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| idx + offset)
            .unwrap();
        let name = String::from_utf8(payload[idx..name_end].to_vec()).unwrap();
        idx = name_end + 1;
        let table_oid = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let attribute_number = i16::from_be_bytes(payload[idx..idx + 2].try_into().unwrap());
        idx += 2;
        let type_oid = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let type_size = i16::from_be_bytes(payload[idx..idx + 2].try_into().unwrap());
        idx += 2;
        let type_modifier = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let format = i16::from_be_bytes(payload[idx..idx + 2].try_into().unwrap());
        idx += 2;
        fields.push(RowDescriptionField {
            name,
            table_oid,
            attribute_number,
            type_oid,
            type_size,
            type_modifier,
            format,
        });
    }
    fields
}

fn parse_row_description_oids(payload: &[u8]) -> Vec<i32> {
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    let mut idx = 2;
    let mut oids = Vec::with_capacity(count);
    for _ in 0..count {
        while payload[idx] != 0 {
            idx += 1;
        }
        idx += 1;
        idx += 4;
        idx += 2;
        oids.push(i32::from_be_bytes(
            payload[idx..idx + 4].try_into().unwrap(),
        ));
        idx += 4;
        idx += 2;
        idx += 4;
        idx += 2;
    }
    oids
}

fn parse_row_description_formats(payload: &[u8]) -> Vec<i16> {
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    let mut idx = 2;
    let mut formats = Vec::with_capacity(count);
    for _ in 0..count {
        let name_end = payload[idx..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| idx + offset)
            .unwrap();
        idx = name_end + 1;
        idx += 4 + 2 + 4 + 2 + 4;
        formats.push(i16::from_be_bytes(
            payload[idx..idx + 2].try_into().unwrap(),
        ));
        idx += 2;
    }
    formats
}

fn parse_binary_data_row(payload: &[u8]) -> Vec<Vec<u8>> {
    let count = i16::from_be_bytes(payload[0..2].try_into().unwrap()) as usize;
    let mut idx = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = i32::from_be_bytes(payload[idx..idx + 4].try_into().unwrap());
        idx += 4;
        if len < 0 {
            values.push(Vec::new());
            continue;
        }
        let end = idx + len as usize;
        values.push(payload[idx..end].to_vec());
        idx = end;
    }
    values
}

fn cstr(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
}

fn put_i32(payload: &mut Vec<u8>, value: i32) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn put_i16(payload: &mut Vec<u8>, value: i16) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn scram_attr(message: &str, key: &str) -> String {
    message
        .split(',')
        .find_map(|part| part.strip_prefix(&format!("{key}=")))
        .unwrap()
        .to_string()
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

// ---------- Stream broker slice 8: LISTEN/NOTIFY push delivery ----------

fn read_notification(stream: &mut TcpStream) -> (String, String) {
    loop {
        let (tag, payload) = read_message(stream);
        if tag != b'A' {
            continue; // skip interleaved responses (CommandComplete etc.)
        }
        let channel_end = 4 + payload[4..].iter().position(|b| *b == 0).unwrap();
        let channel = String::from_utf8(payload[4..channel_end].to_vec()).unwrap();
        let payload_end = payload.len() - 1;
        let body = String::from_utf8(payload[channel_end + 1..payload_end].to_vec()).unwrap();
        return (channel, body);
    }
}

#[test]
fn listen_notify_delivers_across_connections() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut listener_conn = TcpStream::connect(address).unwrap();
    startup(&mut listener_conn);
    read_until_ready(&mut listener_conn);
    send_query(&mut listener_conn, "LISTEN app_events;");
    let (tags, _) = read_tags_until_ready(&mut listener_conn);
    assert!(tags.contains(&b'C'), "LISTEN acknowledged: {tags:?}");

    let mut notifier_conn = TcpStream::connect(address).unwrap();
    startup(&mut notifier_conn);
    read_until_ready(&mut notifier_conn);
    send_query(&mut notifier_conn, "NOTIFY app_events, 'hello';");
    read_tags_until_ready(&mut notifier_conn);

    // Push delivery: the idle listener receives the notification without
    // issuing another query (the connection loop flushes within ~250ms).
    listener_conn
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (channel, payload) = read_notification(&mut listener_conn);
    assert_eq!(channel, "app_events");
    assert_eq!(payload, "hello");

    listener_conn.write_all(b"X\0\0\0\x04").unwrap();
    notifier_conn.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn broker_publish_wakes_queue_listeners() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    // Worker connection listens on the queue's broker channel.
    let mut worker = TcpStream::connect(address).unwrap();
    startup(&mut worker);
    read_until_ready(&mut worker);
    send_query(&mut worker, "LISTEN \"bicdb_broker__commerce.orders\";");
    read_tags_until_ready(&mut worker);

    // Producer publishes through the broker SQL surface.
    let mut producer = TcpStream::connect(address).unwrap();
    startup(&mut producer);
    read_until_ready(&mut producer);
    send_query(
        &mut producer,
        "SELECT broker_publish('commerce.orders', '{\"order\": 1}', NULL);",
    );
    read_tags_until_ready(&mut producer);

    // The idle worker is woken with the queue and message id, then consumes.
    worker
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (channel, payload) = read_notification(&mut worker);
    assert_eq!(channel, "bicdb_broker__commerce.orders");
    let wakeup: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(wakeup["queue"], "commerce.orders");
    assert!(wakeup["message_id"].is_string());

    send_query(
        &mut worker,
        "SELECT broker_consume('commerce.orders', 'workers', 'w1', 10, 30000);",
    );
    let rows = read_query_rows(&mut worker);
    assert_eq!(rows.len(), 1);
    let batch: serde_json::Value = serde_json::from_str(&rows[0][0]).unwrap();
    assert_eq!(batch.as_array().unwrap().len(), 1);

    // UNLISTEN stops further wakeups.
    send_query(&mut worker, "UNLISTEN *;");
    read_tags_until_ready(&mut worker);
    send_query(
        &mut producer,
        "SELECT broker_publish('commerce.orders', '{\"order\": 2}', NULL);",
    );
    read_tags_until_ready(&mut producer);
    worker
        .set_read_timeout(Some(Duration::from_millis(800)))
        .unwrap();
    let mut byte = [0_u8; 1];
    match worker.read(&mut byte) {
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::TimedOut => {}
        other => panic!("expected quiet socket after UNLISTEN, got {other:?}"),
    }

    worker.write_all(b"X\0\0\0\x04").unwrap();
    producer.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

// ---------- Per-database scoping of session-scoped namespaces ----------

/// PostgreSQL scopes LISTEN/NOTIFY and advisory locks to a DATABASE. BicDB
/// serves many databases from one listener, and both namespaces are plain maps
/// keyed only by channel name / lock key — the isolation comes entirely from
/// each database owning its own `PgWireServer`, not from anything in the keys.
///
/// That is a real invariant resting on an implementation detail: hoisting
/// either map to the cluster to "share" it would silently merge every tenant
/// database's channels and locks, leaking payloads across databases and
/// letting one database block another's advisory-lock coordination. This test
/// pins the boundary so that refactor fails loudly.
#[test]
fn listen_notify_and_advisory_locks_are_scoped_per_database() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cluster = PgWireCluster::open(dir.path(), "bicdb", PgWireConfig::default()).unwrap();
    let server = cluster.default_server().unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut root = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut root, "bicdb", "bicdb");
    read_until_ready(&mut root);
    send_query(&mut root, "CREATE DATABASE tenant_b;");
    read_tags_until_ready(&mut root);

    let mut a = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut a, "bicdb", "bicdb");
    read_until_ready(&mut a);
    send_query(&mut a, "LISTEN private_channel;");
    read_tags_until_ready(&mut a);

    // NOTIFY from a different database on the same channel name.
    let mut b = TcpStream::connect(address).unwrap();
    send_startup_for_database(&mut b, "bicdb", "tenant_b");
    read_until_ready(&mut b);
    send_query(&mut b, "NOTIFY private_channel, 'CROSS-DB-LEAK';");
    read_tags_until_ready(&mut b);

    // The listener must stay quiet. Its own database can still reach it, which
    // proves the silence is scoping and not a broken listener.
    a.set_read_timeout(Some(Duration::from_millis(1_500)))
        .unwrap();
    let mut buffer = [0u8; 1];
    assert!(
        a.peek(&mut buffer).is_err(),
        "a NOTIFY from another database must not reach this listener"
    );
    a.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    send_query(&mut root, "NOTIFY private_channel, 'same-db';");
    read_tags_until_ready(&mut root);
    let (channel, payload) = read_notification(&mut a);
    assert_eq!(channel, "private_channel");
    assert_eq!(payload, "same-db", "same-database delivery must still work");

    // Advisory locks: the same key held in another database must not block.
    send_query(&mut b, "SELECT pg_advisory_lock(4242);");
    read_tags_until_ready(&mut b);
    send_query(&mut a, "SELECT pg_try_advisory_lock(4242);");
    let mut granted = None;
    loop {
        let (tag, payload) = read_message(&mut a);
        if tag == b'D' {
            granted = Some(payload.last().copied() == Some(b't'));
        }
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(
        granted,
        Some(true),
        "an advisory lock held in another database must not block this one"
    );

    a.write_all(b"X\0\0\0\x04").unwrap();
    b.write_all(b"X\0\0\0\x04").unwrap();
    root.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

/// A clean-shutdown marker claims the database is closed. It must not be
/// written while background workers still hold it.
///
/// The flush loop slept its whole five-second interval before re-checking the
/// shutdown flag, so `request_shutdown()` returned, the marker was written,
/// and the database stayed open and writable for seconds afterwards. With one
/// writer per directory enforced, that also made an immediate reopen fail.
#[test]
fn a_clean_shutdown_marker_means_the_database_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    send_query(&mut client, "CREATE TABLE t (id TEXT PRIMARY KEY);");
    read_query_rows(&mut client);
    client.write_all(b"X\0\0\0\x04").unwrap();

    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
    assert!(dir
        .path()
        .join(bicdb_pgwire::CLEAN_SHUTDOWN_MARKER)
        .exists());
    drop(server);

    // No sleep, no retry: if the marker is honest the directory is free now.
    BicDb::open(dir.path())
        .expect("the clean-shutdown marker was written while the database was still held");
}

/// The two connection caps are raised with different knobs, so a refusal must
/// name the one that actually applied.
///
/// The per-IP cap is checked first, but the operator hint always said
/// `--max-connections`. On a server whose clients share one host — every
/// loopback deployment, every app behind a single NAT — that sends an operator
/// to raise a setting that cannot help, while the server as a whole sits idle.
#[test]
fn a_refused_connection_names_the_limit_that_refused_it() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = PgWireConfig {
        max_connections: 64,
        max_connections_per_ip: 2,
        max_pending_accepts: 1,
        ..PgWireConfig::default()
    };
    let server = PgWireServer::open(dir.path(), config).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));

    // Fill this source IP's allowance while the server is nowhere near full.
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut client = TcpStream::connect(address).unwrap();
        startup(&mut client);
        read_until_ready(&mut client);
        held.push(client);
    }
    assert!(server.stats_snapshot().active_connections < 64);

    // Occupy the one asynchronous drain permit with a peer which never sends
    // startup bytes. The following refusal must take the inline-backpressure
    // path and still deliver the complete PostgreSQL error before closing.
    let mut silent_refused = TcpStream::connect(address).unwrap();
    let error = read_startup_error(&mut silent_refused);
    assert!(error.contains("53300"), "{error}");
    let mut backpressured_refused = TcpStream::connect(address).unwrap();
    send_startup(&mut backpressured_refused, "bicdb");
    let error = read_startup_error(&mut backpressured_refused);
    assert!(error.contains("53300"), "{error}");
    drop(silent_refused);

    // Exercise the close path repeatedly: closing a TCP socket with unread
    // startup bytes can make Linux emit RST and discard the queued FATAL frame.
    // Every refusal must remain a PostgreSQL-shaped error, never a reset race.
    for _ in 0..16 {
        let mut refused = TcpStream::connect(address).unwrap();
        send_startup(&mut refused, "bicdb");
        let error = read_startup_error(&mut refused);
        assert!(error.contains("53300"), "{error}");
    }

    for mut client in held {
        client.write_all(b"X\0\0\0\x04").unwrap();
    }
    server.request_shutdown();
    server_thread.join().unwrap().unwrap();
}

#[tokio::test]
async fn tokio_postgres_row_locks_survive_protocol_messages_and_skip_before_limit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    let server_for_thread = server.clone();
    let server_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(server_for_thread, listener));
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (mut first, first_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let first_task = tokio::spawn(async move {
        let _ = first_connection.await;
    });
    let (second, second_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let second_task = tokio::spawn(async move {
        let _ = second_connection.await;
    });

    first.batch_execute("CREATE TABLE tasks (id int PRIMARY KEY, rank int); INSERT INTO tasks VALUES (1,1),(2,2),(3,3);").await.unwrap();
    first.batch_execute("BEGIN").await.unwrap();
    first
        .query("SELECT id FROM tasks WHERE id=1 FOR UPDATE", &[])
        .await
        .unwrap();
    // Preparing a locking statement must not acquire locks or raise NOWAIT.
    let immediate = second
        .prepare("SELECT id FROM tasks WHERE id=1 FOR UPDATE NOWAIT")
        .await
        .unwrap();
    second.batch_execute("BEGIN").await.unwrap();
    let error = second.query(&immediate, &[]).await.unwrap_err();
    assert_eq!(
        error.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE)
    );
    second.batch_execute("ROLLBACK").await.unwrap();
    second.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        second
            .query_one(
                "SELECT id FROM tasks ORDER BY rank LIMIT 1 FOR UPDATE SKIP LOCKED",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        2
    );
    second.batch_execute("ROLLBACK").await.unwrap();
    first.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        second
            .query_one(
                "SELECT id FROM tasks ORDER BY rank LIMIT 1 FOR UPDATE SKIP LOCKED",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    // The preceding autocommit locking SELECT releases its lock.
    assert_eq!(
        first
            .query_one("SELECT * FROM tasks WHERE id=1 FOR UPDATE NOWAIT", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(first);
    drop(second);
    server.request_shutdown();
    first_task.await.unwrap();
    second_task.await.unwrap();
    server_thread.join().unwrap().unwrap();
}

#[test]
fn copy_trigger_rejection_rolls_back_all_replayed_batches() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            security_context: Some(SecurityContext::new("user-a", "tenant-a")),
            query_timeout: Duration::from_secs(300),
            ..PgWireConfig::default()
        },
    )
    .unwrap();
    let thread_server = server.clone();
    let serving =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(thread_server, listener));
    let mut client = TcpStream::connect(address).unwrap();
    startup(&mut client);
    read_until_ready(&mut client);
    for sql in [
        "CREATE TABLE imported_rows (id INT PRIMARY KEY)",
        "CREATE FUNCTION enforce_operation_context() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = 10001 THEN RAISE EXCEPTION 'rejected final row' USING ERRCODE = '42501'; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER import_guard AFTER INSERT ON imported_rows FOR EACH ROW EXECUTE FUNCTION enforce_operation_context()",
    ] {
        send_query(&mut client, sql);
        read_query_rows(&mut client);
    }
    send_query(&mut client, "COPY imported_rows FROM STDIN CSV");
    assert_eq!(read_message(&mut client).0, b'G');
    let input: String = (1..=10001).map(|id| format!("{id}\n")).collect();
    send_message(&mut client, b'd', input.as_bytes());
    send_message(&mut client, b'c', &[]);
    let (error, status) = read_error_response_with_status(&mut client);
    assert!(error.contains("rejected final row"), "{error}");
    assert_eq!(status, b'I');
    send_query(&mut client, "SELECT count(*) FROM imported_rows");
    assert_eq!(read_query_rows(&mut client), vec![vec!["0".to_string()]]);
    send_query(&mut client, "INSERT INTO imported_rows VALUES (1)");
    read_query_rows(&mut client);
    client.write_all(b"X\0\0\0\x04").unwrap();
    server.request_shutdown();
    serving.join().unwrap().unwrap();
}
