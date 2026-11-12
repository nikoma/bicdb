//! Catalog ACLs must describe and render their actual PostgreSQL array type.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use futures_util::FutureExt;
use parking_lot::RwLock;
use std::{net::TcpListener, panic::AssertUnwindSafe, sync::Arc, thread, time::Duration};
use tokio_postgres::{NoTls, SimpleQueryMessage};

fn rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<Option<String>>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_catalog_acl_describe_and_text_output_preserve_oid_and_permissions() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for profile in ["postgres", "oracle-preview-v1"] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open_with_config(
                root.path(),
                DbConfig::default()
                    .with_fsync(true)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap();
            bicdb_sql::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = PgWireServer::open_with_shared(
                root.path(),
                PgWireConfig {
                    sql_profile: profile.into(),
                    fsync: true,
                    automatic_paged_checkpoint: false,
                    ..Default::default()
                },
                Arc::new(RwLock::new(db)),
            )
            .unwrap();
            let serving = Arc::clone(&server);
            let service =
                thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user("bicdb")
                .dbname("bicdb")
                .options(&format!("-c bicdb.sql_profile={profile}"))
                .connect_timeout(Duration::from_secs(10))
                .connect(NoTls)
                .await
                .unwrap();
            let connection = tokio::spawn(connection);
            let outcome = AssertUnwindSafe(async {
                tokio::time::timeout(Duration::from_secs(20), async {
                    client.simple_query("CREATE ROLE acl_reader; CREATE TABLE acl_items(id INT PRIMARY KEY,relacl TEXT); GRANT SELECT ON acl_items TO acl_reader").await.unwrap();
                    // Table grants and column grants are distinct. This engine
                    // currently refuses column GRANT; retain that honest limit
                    // while describing the null attacl using its actual type.
                    let column_grant = client.simple_query("GRANT SELECT(id) ON acl_items TO acl_reader").await.unwrap_err();
                    assert_eq!(column_grant.as_db_error().unwrap().code().code(), "0A000");
                    for (query, has_explicit_acl) in [
                        ("SELECT relacl FROM pg_catalog.pg_class WHERE relname IN ('acl_items','ACL_ITEMS')", true),
                        ("SELECT attacl FROM pg_catalog.pg_attribute WHERE attname IN ('id','ID') AND attrelid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname IN ('acl_items','ACL_ITEMS'))", false),
                    ] {
                        let prepared = client.prepare(query).await.unwrap();
                        assert_eq!(prepared.columns()[0].type_().oid(), 1034, "{mode} {profile}: {query}");
                        let values = rows(client.simple_query(query).await.unwrap());
                        assert_eq!(values.len(), 1, "{mode} {profile}: {values:?}");
                        if has_explicit_acl {
                            let acl = values[0][0].as_deref().unwrap();
                            assert!(acl.starts_with('{') && acl.ends_with('}'), "{acl}");
                            assert!(acl.to_ascii_lowercase().contains("acl_reader=r/"), "{acl}");
                        } else {
                            assert_eq!(values[0][0], None);
                        }
                    }
                    // Null and empty catalogs still need the declared array OID.
                    for (catalog, column) in [
                        ("pg_class", "relacl"), ("pg_attribute", "attacl"),
                        ("pg_type", "typacl"), ("pg_namespace", "nspacl"),
                        ("pg_proc", "proacl"), ("pg_database", "datacl"),
                        ("pg_tablespace", "spcacl"), ("pg_language", "lanacl"),
                        ("pg_foreign_data_wrapper", "fdwacl"), ("pg_foreign_server", "srvacl"),
                        ("pg_default_acl", "defaclacl"), ("pg_largeobject_metadata", "lomacl"),
                    ] {
                        let query = format!("SELECT {column} AS permission_array FROM pg_catalog.{catalog} WHERE 1=0");
                        let prepared = client.prepare(&query).await.unwrap();
                        assert_eq!(prepared.columns()[0].type_().oid(), 1034, "{mode} {profile}: {query}");
                    }
                    let user_column = client.prepare("SELECT relacl FROM acl_items WHERE 1=0").await.unwrap();
                    assert_eq!(user_column.columns()[0].type_().oid(), 25);
                    let unavailable_codecs = rows(client.simple_query("SELECT typreceive,typsend FROM pg_catalog.pg_type WHERE oid=1033").await.unwrap());
                    assert_eq!(unavailable_codecs, vec![vec![Some("-".into()), Some("-".into())]]);
                    let metadata = rows(client.simple_query("SELECT typname,typlen,typalign,typreceive::oid,typsend::oid,typarray FROM pg_catalog.pg_type WHERE oid=1033").await.unwrap());
                    assert_eq!(metadata, vec![vec![Some("aclitem".into()),Some("16".into()),Some("d".into()),Some("0".into()),Some("0".into()),Some("1034".into())]]);
                    let error = client.query("SELECT relacl FROM pg_catalog.pg_class WHERE relname IN ('acl_items','ACL_ITEMS')", &[]).await.unwrap_err();
                    assert_eq!(error.as_db_error().unwrap().code().code(), "42883");
                    assert_eq!(rows(client.simple_query("SELECT 1").await.unwrap()), vec![vec![Some("1".into())]]);
                }).await.expect("catalog metadata query exceeded its focused test bound");
            }).catch_unwind().await;
            drop(client);
            let _ = connection.await;
            server.request_shutdown();
            service.join().unwrap().unwrap();
            if let Err(panic) = outcome {
                std::panic::resume_unwind(panic);
            }
        }
    }
}
