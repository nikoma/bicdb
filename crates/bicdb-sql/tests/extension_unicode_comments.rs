use bicdb_sql::{parse_extension_catalog_ddl, ExtensionCatalogDdl};

#[test]
fn extension_probe_preserves_unicode_sql_comments() {
    for sql in [
        "-- ── section heading\nSELECT 1;",
        "-- 🧘 section heading\nSELECT 1;",
        "/* 中文 section heading */ SELECT 1;",
        "SELECT 'é';",
    ] {
        assert!(parse_extension_catalog_ddl(sql).unwrap().is_none(), "{sql}");
    }
}

#[test]
fn extension_probe_keeps_keyword_and_postgres_fallback_behavior() {
    assert_eq!(
        parse_extension_catalog_ddl("DROP EXTENSION IF EXISTS sample CASCADE").unwrap(),
        Some(ExtensionCatalogDdl::DropExtension {
            name: "sample".to_string(),
            if_exists: true,
            cascade: true,
        })
    );
    assert!(
        parse_extension_catalog_ddl("CREATE EXTENSION IF NOT EXISTS vector")
            .unwrap()
            .is_none()
    );
    assert!(parse_extension_catalog_ddl("ALTERATION EXTENSION sample")
        .unwrap()
        .is_none());
}
