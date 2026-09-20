use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn xml_casts_validate_content_and_preserve_source_text() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT '<a>one</a><b attr="x">two</b>'::xml,
                      xml '<?xml version="1.0" encoding="LATIN1"?><a>é</a>',
                      xml '<?xml version="1.1" encoding="UTF-8"?><a/>',
                      xml_is_well_formed('<a/>'),
                      xml_is_well_formed_document('<a/><b/>'),
                      xml_is_well_formed_content('<a/><b/>')"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String(r#"<a>one</a><b attr="x">two</b>"#.into()),
            SqlValue::String("<a>é</a>".into()),
            SqlValue::String("<?xml version=\"1.1\"?><a/>".into()),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT '<a><b></a>'::xml")
            .unwrap_err()
            .sqlstate(),
        "2200N"
    );
    for query in ["SELECT 1::xml", "SELECT xml '<a/>'::integer"] {
        assert_eq!(session.execute(query).unwrap_err().sqlstate(), "42846");
    }
    assert_eq!(
        session
            .execute("SELECT xml '<a/>' = xml '<a/>'")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
}

#[test]
fn xpath_and_xml_conventional_functions_match_postgres_shapes() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                xpath('/root/a/text()', '<root><a>one</a><a>two</a></root>'::xml),
                xpath_exists('/root/a[@id="x"]', '<root><a id="x"/></root>'::xml),
                xpath('/r:root/r:item',
                    xml '<r:root xmlns:r="urn:test"><r:item id="1">A</r:item></r:root>',
                    ARRAY[ARRAY['r', 'urn:test']]),
                xmlconcat('<a/>'::xml, NULL, '<b/>'::xml),
                xmlcomment('reviewed')"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Json(json!(["one", "two"])),
            SqlValue::Bool(true),
            SqlValue::Json(json!([r#"<r:item xmlns:r="urn:test" id="1">A</r:item>"#])),
            SqlValue::String("<a/><b/>".into()),
            SqlValue::String("<!--reviewed-->".into()),
        ]]
    );
    assert_eq!(result.column_types[0], Some("xml[]".into()));
    assert_eq!(result.column_types[1], Some("bool".into()));
    assert_eq!(result.column_types[2], Some("xml[]".into()));
    assert_eq!(result.column_types[3], Some("xml".into()));
}

#[test]
fn xml_dtd_entities_are_validated_without_external_io() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                xml '<!DOCTYPE foo [<!ENTITY x "bar">]><foo>&x;</foo>',
                xpath('string(/foo)', xml '<!DOCTYPE foo [<!ENTITY x "bar">]><foo>&x;</foo>'),
                xml '<!DOCTYPE foo SYSTEM "file:///etc/passwd"><foo>&external;</foo>'"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String(r#"<!DOCTYPE foo [<!ENTITY x "bar">]><foo>&x;</foo>"#.into()),
            SqlValue::Json(json!(["bar"])),
            SqlValue::String(
                r#"<!DOCTYPE foo SYSTEM "file:///etc/passwd"><foo>&external;</foo>"#.into()
            ),
        ]]
    );

    assert_eq!(
        session
            .execute("SELECT xml '<foo>&undeclared;</foo>'")
            .unwrap_err()
            .sqlstate(),
        "2200N"
    );
}

#[test]
fn sql_xml_parse_serialize_and_exists_syntax_matches_postgres() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                xmlparse(document '<root><a/></root>'),
                xmlparse(content '<a/><b/>'),
                xmlserialize(document xml '<a/>' AS text),
                xmlexists('/root/a' PASSING BY VALUE xml '<root><a/></root>')"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("<root><a/></root>".into()),
            SqlValue::String("<a/><b/>".into()),
            SqlValue::String("<a/>".into()),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn sql_xml_constructors_escape_values_and_omit_nulls() {
    let (_dir, mut db) = session_db();
    let result = SqlSession::new(&mut db)
        .execute(
            r#"SELECT
                xmlelement(name patient,
                    xmlattributes(42 AS id, NULL AS omitted),
                    xmlforest('Ada & Bob' AS name, NULL AS missing)),
                xmlelement(name typed_content, '<a/>'::text, xml '<b/>'),
                xmlpi(name php, 'echo 1;'),
                xmlroot(xml '<a/>', version '1.1', standalone yes),
                XmLrOoT(xml '<a/>', VeRsIoN no value, StAnDaLoNe no value)"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String(r#"<patient id="42"><name>Ada &amp; Bob</name></patient>"#.into()),
            SqlValue::String("<typed_content>&lt;a/&gt;<b/></typed_content>".into()),
            SqlValue::String("<?php echo 1;?>".into()),
            SqlValue::String("<?xml version=\"1.1\" standalone=\"yes\"?><a/>".into()),
            SqlValue::String("<a/>".into()),
        ]]
    );
}

#[test]
fn xmltable_projects_typed_columns_defaults_and_ordinality() {
    let (_dir, mut db) = session_db();
    let result = SqlSession::new(&mut db)
        .execute(
            r#"SELECT id, label, ordinality
               FROM XMLTABLE('/rows/row'
                 PASSING xml '<rows><row id="1"><label>A</label></row><row id="2"/></rows>'
                 COLUMNS
                   id int PATH '@id',
                   label text PATH 'label' DEFAULT 'missing',
                   ordinality FOR ORDINALITY)
               ORDER BY id"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::String("A".into()),
                SqlValue::Int(1)
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::String("missing".into()),
                SqlValue::Int(2)
            ],
        ]
    );
}

#[test]
fn xmltable_applies_declared_namespaces() {
    let (_dir, mut db) = session_db();
    let result = SqlSession::new(&mut db)
        .execute(
            r#"SELECT id, name
               FROM XMLTABLE(
                 XMLNAMESPACES('urn:test' AS r),
                 '/r:rows/r:row'
                 PASSING xml '<r:rows xmlns:r="urn:test"><r:row id="7"><r:name>A</r:name></r:row></r:rows>'
                 COLUMNS id int PATH '@id', name text PATH 'r:name')"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(7), SqlValue::String("A".into())]]
    );
}

#[test]
fn xmlagg_orders_filters_omits_nulls_and_reports_xml_type() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE xml_fragments (id int PRIMARY KEY, payload xml)")
        .unwrap();
    session
        .execute("INSERT INTO xml_fragments VALUES (2, xml '<b/>'), (1, xml '<a/>'), (3, NULL)")
        .unwrap();
    let result = session
        .execute(
            "SELECT xmlagg(payload ORDER BY id), xmlagg(payload) FILTER (WHERE id <> 2) FROM xml_fragments",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("<a/><b/>".into()),
            SqlValue::String("<a/>".into()),
        ]]
    );
    assert_eq!(
        result.column_types,
        vec![Some("xml".into()), Some("xml".into())]
    );
    assert_eq!(
        session
            .execute("SELECT xmlagg(DISTINCT payload) FROM xml_fragments")
            .unwrap_err()
            .sqlstate(),
        "42883"
    );
}

#[test]
fn xml_storage_arrays_catalog_and_restart_are_stable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE xml_store (id int PRIMARY KEY, payload xml, fragments xml[])")
            .unwrap();
        session
            .execute(
                r#"INSERT INTO xml_store VALUES (
                     1,
                     xml '<?xml version="1.1" encoding="UTF-8"?><root><value>saved</value></root>',
                     ARRAY[xml '<a/>', NULL, xml '<b/>'])"#,
            )
            .unwrap();
    }

    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute("SELECT payload, fragments FROM xml_store WHERE id = 1")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("<?xml version=\"1.1\"?><root><value>saved</value></root>".into()),
            SqlValue::Json(json!(["<a/>", null, "<b/>"])),
        ]]
    );
    assert_eq!(
        session
            .execute("UPDATE xml_store SET payload = '<root>'::xml WHERE id = 1")
            .unwrap_err()
            .sqlstate(),
        "2200N"
    );
    assert_eq!(
        session
            .execute("SELECT payload FROM xml_store WHERE id = 1")
            .unwrap()
            .rows[0][0],
        SqlValue::String("<?xml version=\"1.1\"?><root><value>saved</value></root>".into())
    );

    let catalog = session
        .execute("SELECT oid, typarray FROM pg_type WHERE typname = 'xml'")
        .unwrap();
    assert_eq!(
        catalog.rows,
        vec![vec![SqlValue::Int(142), SqlValue::Int(143)]]
    );
}

#[test]
fn xml_rejects_postgres_undefined_comparison_and_ordering_operations() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE xml_ops (id int PRIMARY KEY, payload xml)")
        .unwrap();
    session
        .execute("INSERT INTO xml_ops VALUES (1, xml '<a/>'), (2, xml '<b/>')")
        .unwrap();

    for (query, sqlstate) in [
        ("SELECT * FROM xml_ops WHERE payload = xml '<a/>'", "42883"),
        ("SELECT payload FROM xml_ops ORDER BY payload", "42883"),
        ("SELECT DISTINCT payload FROM xml_ops", "42883"),
        (
            "SELECT payload, count(*) FROM xml_ops GROUP BY payload",
            "42883",
        ),
        ("CREATE INDEX xml_ops_idx ON xml_ops(payload)", "42704"),
        ("ALTER TABLE xml_ops ADD UNIQUE(payload)", "42704"),
    ] {
        let error = session.execute(query).unwrap_err();
        assert_eq!(
            error.sqlstate(),
            sqlstate,
            "unexpected SQLSTATE for: {query}: {error}"
        );
    }
}
