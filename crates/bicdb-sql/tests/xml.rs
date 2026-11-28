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
            SqlValue::json(json!(["one", "two"])),
            SqlValue::Bool(true),
            SqlValue::json(json!([r#"<r:item xmlns:r="urn:test" id="1">A</r:item>"#])),
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
            SqlValue::json(json!(["bar"])),
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
            SqlValue::json(json!(["<a/>", null, "<b/>"])),
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

#[test]
fn postgres_xmlbinary_controls_bytea_in_elements_attributes_and_forests() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let literal = "SELECT xmlelement(name item, decode('abff', 'hex')),
                          xmlforest(decode('abff', 'hex') AS bytes),
                          xmlelement(name item, xmlattributes(decode('abff', 'hex') AS bytes))";
    let expected = |encoded: &str| {
        [
            format!("<item>{encoded}</item>"),
            format!("<bytes>{encoded}</bytes>"),
            format!("<item bytes=\"{encoded}\"/>"),
        ]
        .into_iter()
        .map(SqlValue::String)
        .collect::<Vec<_>>()
    };
    assert_eq!(sql.execute(literal).unwrap().rows, [expected("q/8=")]);
    sql.execute(
        "CREATE TABLE xml_binary_input (payload bytea);
                 INSERT INTO xml_binary_input VALUES (decode('abff', 'hex'));
                 BEGIN;
                 SET LOCAL xmlbinary TO hex",
    )
    .unwrap();
    let statement = "SELECT xmlelement(name item, payload),
                            xmlforest(payload AS bytes),
                            xmlelement(name item, xmlattributes(payload AS bytes))
                     FROM xml_binary_input";
    assert_eq!(sql.execute(statement).unwrap().rows, [expected("ABFF")]);
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(sql.execute(statement).unwrap().rows, [expected("q/8=")]);
    assert_eq!(
        sql.execute("SET xmlbinary TO unsupported")
            .unwrap_err()
            .sqlstate(),
        "22023"
    );
}
#[test]
fn xml_fragment_concatenation_preserves_a_single_compatible_declaration() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute(r#"SELECT
        xmlconcat(NULL,NULL) IS NULL,
        xmlconcat('<a/>', '<?xml version="1.0" standalone="no"?><b/>'),
        xmlconcat('<?xml version="1.1"?><a/>', NULL, '<?xml version="1.1" standalone="no"?><b/>'),
        xmlconcat('<?xml version="1.0" standalone="yes"?><a/>', '<?xml version="1.1" standalone="yes"?><b/>'),
        '<?xml-stylesheet type="text/xsl" href="style.xsl"?><a/>'::xml,
        xml_is_well_formed('<?xml version="1.0" standalone="y"?><a/>')"#).unwrap();
    assert_eq!(
        result.rows,
        [[
            SqlValue::Bool(true),
            SqlValue::String("<a/><b/>".into()),
            SqlValue::String("<?xml version=\"1.1\"?><a/><b/>".into()),
            SqlValue::String("<?xml version=\"1.0\" standalone=\"yes\"?><a/><b/>".into()),
            SqlValue::String("<?xml-stylesheet type=\"text/xsl\" href=\"style.xsl\"?><a/>".into()),
            SqlValue::Bool(false),
        ]]
    );
    let aggregate = session
        .execute(
            r#"SELECT xmlagg(fragment ORDER BY id) FROM (VALUES
        (2, '<?xml version="1.1" standalone="no"?><b/>'::xml),
        (1, '<?xml version="1.1"?><a/>'::xml)
    ) q(id,fragment)"#,
        )
        .unwrap();
    assert_eq!(
        aggregate.rows,
        [[SqlValue::String("<?xml version=\"1.1\"?><a/><b/>".into())]]
    );
    for (sql, state, message) in [
        (
            "SELECT xmlconcat(1,2)",
            "42804",
            "argument of XMLCONCAT must be type xml, not type integer",
        ),
        (
            "SELECT xmlconcat('x'::text)",
            "42804",
            "argument of XMLCONCAT must be type xml, not type text",
        ),
        (
            "SELECT xmlconcat('ok','<broken')",
            "2200N",
            "invalid XML content",
        ),
        ("SELECT xmlcomment('bad-')", "2200S", "invalid XML comment"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), state, "{sql}: {error:?}");
        assert!(error.to_string().contains(message), "{sql}: {error:?}");
    }
}
#[test]
fn xmltable_relation_aliases_rename_output_columns_without_changing_their_paths() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    for lateral in ["", "LATERAL "] {
        let result = session
            .execute(&format!(
                r#"SELECT f.* FROM {lateral}XMLTABLE('/r/a' PASSING '<r><a><n>7</n></a><a><n>8</n></a></r>'
                   COLUMNS n int, rowno FOR ORDINALITY) AS f("Value")"#
            ))
            .unwrap();
        assert_eq!(result.columns, ["Value", "rowno"]);
        assert_eq!(
            result.rows,
            [
                [SqlValue::Int(7), SqlValue::Int(1)],
                [SqlValue::Int(8), SqlValue::Int(2)]
            ]
        );
        let result = session
            .execute(&format!(
                "SELECT d.id, f.val FROM (VALUES (1, xml '<a>7</a>'), (2, xml '<a>8</a>')) d(id,doc),
                 {lateral}XMLTABLE('/a' PASSING d.doc COLUMNS n int PATH '.') f(val) ORDER BY d.id"
            ))
            .unwrap();
        assert_eq!(
            result.rows,
            [
                [SqlValue::Int(1), SqlValue::Int(7)],
                [SqlValue::Int(2), SqlValue::Int(8)]
            ]
        );
    }
    let error = session
        .execute("SELECT * FROM XMLTABLE('/a' PASSING '<a>7</a>' COLUMNS n int PATH '.') f(a,b)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42P10");
}

#[test]
fn xmltable_uses_the_selected_node_as_xpath_context() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"
        SELECT q.* FROM XMLTABLE(
            '/doc/item' PASSING '<doc x="ok"><item id="one"><b>txt &amp; x</b><b>c</b></item></doc>'
            COLUMNS ord FOR ORDINALITY, "Id" text PATH '@id',
                whole text PATH '.', parent text PATH '../@x',
                n int PATH 'count(b)', joined text PATH 'concat(@id,"-",../@x)',
                bs xml PATH 'b', truth integer PATH 'true()',
                missing text PATH 'absent' DEFAULT 'fallback'
        ) AS q
    "#,
        )
        .unwrap();
    assert_eq!(
        result.columns,
        ["ord", "Id", "whole", "parent", "n", "joined", "bs", "truth", "missing"]
    );
    assert_eq!(
        result.rows,
        [[
            SqlValue::Int(1),
            SqlValue::String("one".into()),
            SqlValue::String("txt & xc".into()),
            SqlValue::String("ok".into()),
            SqlValue::Int(2),
            SqlValue::String("one-ok".into()),
            SqlValue::String("<b>txt &amp; x</b><b>c</b>".into()),
            SqlValue::Int(1),
            SqlValue::String("fallback".into()),
        ]]
    );
    assert!(session.execute(
        "SELECT * FROM XMLTABLE('count(/doc/item)' PASSING '<doc><item/></doc>' COLUMNS v text PATH '.')"
    ).unwrap().rows.is_empty());
    assert_eq!(
        session.execute("SELECT xmltable.* FROM XMLTABLE('/doc/item' PASSING '<doc><item/></doc>' COLUMNS v text DEFAULT 'fallback' PATH 'missing')").unwrap().rows,
        [[SqlValue::String("fallback".into())]]
    );
    for (sql, state, message) in [
        ("SELECT * FROM XMLTABLE('/doc/item' PASSING '<doc><item><b>x</b><b>y</b></item></doc>' COLUMNS many text PATH 'b')", "21000", "more than one value returned by column XPath expression"),
        ("SELECT * FROM XMLTABLE('/doc/item' PASSING '<doc><item/></doc>' COLUMNS \"UPPER\" text PATH '@absent' NOT NULL)", "22004", "null is not allowed in column \"UPPER\""),
        ("SELECT * FROM XMLTABLE('/doc/item' PASSING '<doc/>' COLUMNS v text PATH NULL)", "22004", "column filter expression must not be null"),
        ("SELECT * FROM XMLTABLE(NULL PASSING '<doc/>' COLUMNS v text)", "22004", "row filter expression must not be null"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), state, "{sql}: {error:?}");
        assert!(error.to_string().contains(message), "{sql}: {error:?}");
    }
}
#[test]
fn xmlroot_applies_version_and_standalone_modes_to_the_declaration() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute(
        r#"SELECT xmlroot(xml '<x/>', VERSION '2.0'),
                   xmlroot(xml '<x/>', VERSION 123),
                   xmlroot('<?xml version="1.2" standalone="yes"?> <x/>', VERSION NO VALUE),
                   xmlroot('<?xml version="1.2" standalone="yes"?> <x/>', VERSION NO VALUE, STANDALONE NO VALUE),
                   xmlroot(xml '  <x/>  ', VERSION '1.1'),
                   xmlroot(xmlroot(xml '<x/>', VERSION '1.0'), VERSION '1.1', STANDALONE NO),
                   xmlroot(NULL, VERSION '1.1')"#,
    ).unwrap();
    let expected = [
        r#"<?xml version="2.0"?><x/>"#,
        r#"<?xml version="123"?><x/>"#,
        r#"<?xml version="1.0" standalone="yes"?> <x/>"#,
        " <x/>",
        r#"<?xml version="1.1"?>  <x/>  "#,
        r#"<?xml version="1.1" standalone="no"?><x/>"#,
    ];
    assert_eq!(
        result.rows[0][..6],
        expected.map(|text| SqlValue::String(text.into()))
    );
    assert_eq!(result.rows[0][6], SqlValue::Null);
    assert_eq!(
        session
            .execute("SELECT xmlroot('<x/>'::text, VERSION '1.1')")
            .unwrap_err()
            .sqlstate(),
        "42804"
    );
}

#[test]
fn xml_document_predicates_preserve_postfix_precedence_and_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT xml '<a/>' IS DOCUMENT,
                    xml '<a/><b/>' IS NOT DOCUMENT,
                    'text' IS DOCUMENT, NULL IS DOCUMENT,
                    NOT xml '<a/>' IS DOCUMENT,
                    xml '<a/>' IS DOCUMENT IS NOT NULL,
                    CASE WHEN false THEN xml '<a/>' ELSE xml 'text' END IS NOT DOCUMENT",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Null,
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(result.columns[0], "?column?");
    assert_eq!(result.column_types[0].as_deref(), Some("bool"));
    for (sql, state) in [
        ("SELECT ('x'::text) IS NOT DOCUMENT", "42804"),
        ("SELECT 'plain <' IS DOCUMENT", "2200N"),
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), state, "{sql}");
    }
    session
        .execute(
            "CREATE TABLE checked_xml(document xml CHECK (document IS DOCUMENT));
         INSERT INTO checked_xml VALUES ('<a/>')",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO checked_xml VALUES ('plain')")
            .unwrap_err()
            .sqlstate(),
        "23514"
    );
}

#[test]
fn postgres_server_encoding_is_visible_and_read_only() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session.execute("SHOW server_encoding").unwrap().rows,
        vec![vec![SqlValue::String("UTF8".into())]]
    );
    assert_eq!(
        session
            .execute("SELECT current_setting('server_encoding')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("UTF8".into())]]
    );
    for sql in [
        "SET server_encoding = 'LATIN1'",
        "SELECT set_config('server_encoding','UTF8',false)",
        "RESET server_encoding",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "55P02",
            "{sql}"
        );
    }
}

#[test]
fn xpath_existence_uses_result_cardinality_and_sql_xml_passing_modes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            "SELECT xpath_exists('false()', xml '<x/>'),
                    xpath_exists('count(/missing)', xml '<x/>'),
                    xpath_exists('/missing', xml '<x/>'),
                    xmlexists('/x' PASSING BY REF xmlparse(DOCUMENT '<x/>') BY VALUE),
                    xmlexists('/missing' PASSING xml '<x/>' BY REF)",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
    assert_eq!(
        session
            .execute("SELECT n FROM XMLTABLE('/x' PASSING BY REF xml '<x/>' BY REF COLUMNS n FOR ORDINALITY)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute("SELECT xpath_exists('', xml '<x/>')")
            .unwrap_err()
            .sqlstate(),
        "10608"
    );
}

#[test]
fn postgres_xmltext_and_xml_processing_instructions_preserve_text() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT xmltext(NULL), xmltext(''), xmltext(E' '' " & < >\r\n'),
                       pg_catalog.xmltext('a  '::char(4)), xmlpi(NAME ident, NULL),
                       xmlpi(NAME IdEnt), xmlpi(NAME "IdEnt", ''),
                       xmlpi(NAME ident, '  words   ')"#,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Null,
            SqlValue::String("".into()),
            SqlValue::String(" ' &quot; &amp; &lt; &gt;&#13;\n".into()),
            SqlValue::String("a".into()),
            SqlValue::Null,
            SqlValue::String("<?ident?>".into()),
            SqlValue::String("<?IdEnt ?>".into()),
            SqlValue::String("<?ident words   ?>".into()),
        ]]
    );
    assert_eq!(result.column_types[2].as_deref(), Some("xml"));
    for (sql, state) in [
        ("SELECT xmltext(1)", "42883"),
        ("SELECT xmlpi(NAME Xml)", "2200T"),
        ("SELECT xmlpi(NAME ident, 'ab?>')", "2200T"),
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), state, "{sql}");
    }
}

#[test]
fn xmlparse_and_xmlserialize_preserve_document_and_content_modes() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let value = session
        .execute(
            r#"SELECT
        xmlparse(DOCUMENT '<?xml version="1.0" encoding="LATIN1"?><a>å</a>'),
        xmlparse(CONTENT ' a ' STRIP WHITESPACE),
        xmlserialize(DOCUMENT xml '<a><b/><c>text</c></a>' AS text INDENT),
        xmlserialize(CONTENT xml '<a/><b/>' AS text INDENT),
        xmlserialize(CONTENT xml '<a>at<b/>b</a>' AS text INDENT),
        xmlserialize(CONTENT xml '<a><b/></a>' AS text NO INDENT)"#,
        )
        .unwrap();
    assert_eq!(
        value.rows,
        [[
            SqlValue::String("<a>å</a>".into()),
            SqlValue::String(" a ".into()),
            SqlValue::String("<a>\n  <b/>\n  <c>text</c>\n</a>".into()),
            SqlValue::String("<a/>\n<b/>".into()),
            SqlValue::String("<a>at<b/>b</a>".into()),
            SqlValue::String("<a><b/></a>".into()),
        ]]
    );
    for (sql, state, message) in [
        (
            "SELECT xmlparse(DOCUMENT '<a/><b/>')",
            "2200M",
            "invalid XML document",
        ),
        (
            "SELECT xmlserialize(DOCUMENT xml '<a/><b/>' AS text)",
            "2200L",
            "not an XML document",
        ),
        (
            "SELECT xmlserialize(CONTENT '<a/>'::text AS text)",
            "42804",
            "argument of XMLSERIALIZE must be type xml",
        ),
        (
            "SELECT xmlserialize(CONTENT xml '<a/>' AS integer)",
            "42846",
            "cannot cast XMLSERIALIZE result to integer",
        ),
        (
            "SELECT xmlserialize(CONTENT xml '<long/>' AS varchar(4))",
            "22001",
            "value too long for type character varying(4)",
        ),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), state, "{sql}: {error:?}");
        assert!(error.to_string().contains(message), "{sql}: {error:?}");
    }
}

#[test]
fn sql_xml_maps_arrays_and_temporal_values_to_xml_text() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                xmlelement(NAME demo, ARRAY[[1,NULL],[2,3]]),
                xmlforest(ARRAY['x<','y>'] AS vals),
                xmlelement(NAME t, xmlattributes(ARRAY['<&'] AS a)),
                xmlelement(NAME demo, ARRAY[xml '<a/>', xml 'some text']),
                xmlelement(NAME demo, true, false, timestamp '2009-04-09 00:24:37.1234'),
                xmlelement(NAME demo, '[0:2]={1,NULL,3}'::int[])"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        [
            "<demo><element>1</element><element>2</element><element>3</element></demo>",
            "<vals><element>x&lt;</element><element>y&gt;</element></vals>",
            r#"<t a="&lt;element&gt;&amp;lt;&amp;amp;&lt;/element&gt;"/>"#,
            "<demo><element><a/></element><element>some text</element></demo>",
            "<demo>truefalse2009-04-09T00:24:37.1234</demo>",
            "<demo><element>1</element><element>3</element></demo>",
        ]
    );
    session.execute("SET TIME ZONE 'America/New_York'").unwrap();
    assert_eq!(
        session
            .execute("SELECT xmlelement(NAME demo, timestamptz '2009-04-09 00:24:37Z')")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "<demo>2009-04-08T20:24:37-04:00</demo>"
    );
    for sql in [
        "SELECT xmlelement(NAME x, date 'infinity')",
        "SELECT xmlelement(NAME x, timestamp '-infinity')",
        "SELECT xmlelement(NAME x, timestamptz 'infinity')",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "22008");
    }
}

#[test]
fn xmlserialize_indents_mixed_content_and_keeps_the_document_prolog() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT
                xmlserialize(CONTENT 'text<foo>73</foo>next<bar><val x="y">42</val></bar>' AS text INDENT),
                xmlserialize(DOCUMENT '<?xml version="1.0"?><foo><bar/></foo>' AS text INDENT),
                xmlserialize(DOCUMENT '<?pi hi?><!DOCTYPE a><a/>' AS text INDENT),
                xmlserialize(CONTENT '<?pi hi?><!DOCTYPE a><a/>' AS text INDENT),
                xmlserialize(CONTENT ' a &#65; &amp; ' AS text INDENT),
                xmlserialize(CONTENT '<!--<!DOCTYPE sample>--><a/><b/>' AS text INDENT)"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0]
            .iter()
            .map(SqlValue::to_cell)
            .collect::<Vec<_>>(),
        [
            "text\n<foo>73</foo>next\n<bar>\n  <val x=\"y\">42</val>\n</bar>",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<foo>\n  <bar/>\n</foo>",
            "<?pi hi?>\n<!DOCTYPE a>\n<a/>",
            "<?pi hi?>\n<!DOCTYPE a>\n<a/>\n",
            " a A &amp; ",
            "<!--<!DOCTYPE sample>-->\n<a/>\n<b/>",
        ]
    );
    let error = session
        .execute("SELECT xml '<!DOCTYPE a><a/><b/>'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "2200N");
}

#[test]
fn sql_xml_names_escape_characters_and_duplicate_attributes_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let mut db = bicdb_core::BicDb::open(root.path()).unwrap();
    let mut sql = bicdb_sql::SqlSession::new(&mut db);
    let result = sql
        .execute(
            r#"SELECT xmlpi(NAME "123"), xmlpi(NAME ":::_xml_abc135.%-&_"),
              xmlelement(NAME "a b", 'text'),
              xmlelement(NAME "_x0031_", xmlattributes(1 AS "any value")),
              xmlforest(1 AS "123", 'value' AS "café", 2 AS "é", 3 AS "𐐀")"#,
        )
        .unwrap();
    let cells: Vec<_> = result.rows[0].iter().map(|value| value.to_cell()).collect();
    assert_eq!(
        cells,
        [
            "<?_x0031_23?>",
            "<?_x003A_::_x005F_xml_abc135._x0025_-_x0026__?>",
            "<a_x0020_b>text</a_x0020_b>",
            r#"<_x005F_x0031_ any_x0020_value="1"/>"#,
            "<_x0031_23>1</_x0031_23><café>value</café><é>2</é><_x10400_>3</_x10400_>",
        ]
    );
    let error = sql
        .execute("SELECT xmlelement(NAME e, XMLATTRIBUTES(NULL AS a, 'value' AS a))")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42601");
}

#[test]
fn xmloption_controls_input_without_reinterpreting_existing_xml_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let initial = sql
        .execute("SELECT current_setting('xmloption'), xml_is_well_formed('text')")
        .unwrap();
    assert_eq!(
        initial.rows,
        vec![vec![
            SqlValue::String("content".into()),
            SqlValue::Bool(true)
        ]]
    );
    sql.execute(
        "CREATE TABLE xmloption_values (id int PRIMARY KEY, body xml);
         INSERT INTO xmloption_values VALUES (1, XMLPARSE(CONTENT '<a/><b/>'));
         BEGIN;
         SET LOCAL XML /* language syntax */ OPTION DOCUMENT;",
    )
    .unwrap();
    let result = sql
        .execute("SELECT body::xml::text, xml_is_well_formed(body::text), pg_input_is_valid(body::text, 'xml') FROM xmloption_values")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("<a/><b/>".into()),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
        ]]
    );
    let result = sql
        .execute("SELECT XMLPARSE(CONTENT 'text<a/>')::xml::text, xml_is_well_formed_content('text<a/>'), xml_is_well_formed_document('<!-- no element -->')")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("text<a/>".into()),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
        ]]
    );
    sql.execute("COMMIT").unwrap();
    for input in [
        "SELECT 'text'::xml",
        "SELECT XML 'text'",
        "SELECT xml_in('text')",
    ] {
        sql.execute("SET XMLOPTION TO DOCUMENT").unwrap();
        let error = sql.execute(input).unwrap_err();
        assert_eq!(error.sqlstate(), "2200M", "{input}");
    }
    let normalized = sql
        .execute("SELECT set_config('xmloption', 'CoNTent', false)")
        .unwrap();
    assert_eq!(
        normalized.rows,
        vec![vec![SqlValue::String("content".into())]]
    );
    assert_eq!(
        sql.execute("SELECT 'text'::xml::text").unwrap().rows,
        vec![vec![SqlValue::String("text".into())]]
    );
}

#[test]
fn xml_document_mode_applies_to_assignment_and_copy_input() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE xml_write_input (id int PRIMARY KEY, body xml);
         SET xmloption TO DOCUMENT;
         INSERT INTO xml_write_input VALUES (1, XMLPARSE(CONTENT 'keep this fragment'));",
    )
    .unwrap();

    for (statement, state) in [
        (
            "INSERT INTO xml_write_input VALUES (2, 'bad fragment')",
            "2200M",
        ),
        (
            "INSERT INTO xml_write_input SELECT 2, 'bad fragment'",
            "2200M",
        ),
        (
            "INSERT INTO xml_write_input SELECT 2, 'typed text'::text WHERE false",
            "42804",
        ),
        (
            "UPDATE xml_write_input SET body = 'bad fragment' WHERE id = 1",
            "2200M",
        ),
    ] {
        assert_eq!(
            sql.execute(statement).unwrap_err().sqlstate(),
            state,
            "{statement}"
        );
    }
    let error = sql
        .copy_insert_rows_reporting_at_lines(
            "xml_write_input",
            &[],
            vec![vec![Some("2".into()), Some("<a/><b/>".into())]],
            &[1],
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "2200M");
    assert!(error.fields().iter().any(|field| {
        matches!(field, bicdb_sql::SqlErrorField::Context(context)
            if context == "COPY xml_write_input, line 1, column body: \"<a/><b/>\"")
    }));
    let inserted = sql
        .copy_insert_rows(
            "xml_write_input",
            &[],
            vec![vec![Some("2".into()), Some("<root/>".into())]],
        )
        .unwrap();
    assert_eq!(inserted, 1);
    assert_eq!(
        sql.execute("SELECT body::text FROM xml_write_input ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("keep this fragment".into())],
            vec![SqlValue::String("<root/>".into())]
        ]
    );
}

#[test]
fn xml_mapping_serializes_query_rows_and_does_not_execute_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = bicdb_core::BicDb::open(root.path()).unwrap();
    let mut session = bicdb_sql::SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE \"t-xml\" (id int, \"a:name\" text, markup xml);
             INSERT INTO \"t-xml\" VALUES (1, NULL, '<p/>')",
        )
        .unwrap();
    let wrapped = session
        .execute("SELECT table_to_xml('\"t-xml\"', true, false, 'urn:test')")
        .unwrap();
    assert_eq!(
        wrapped.rows[0][0].to_cell(),
        concat!(
            "<t-xml xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:test\">\n\n",
            "<row>\n  <id>1</id>\n  <a_x003A_name xsi:nil=\"true\"/>\n  <markup><p/></markup>\n",
            "</row>\n\n</t-xml>\n"
        ),
    );
    let forest = session
        .execute(
            "SELECT query_to_xml(
                'SELECT 1; SELECT ARRAY[1,NULL,3] AS arr, true AS ok',
                false, true, ''
             )",
        )
        .unwrap();
    assert_eq!(
        forest.rows[0][0].to_cell(),
        concat!(
            "<row xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n",
            "  <arr><element>1</element><element>3</element></arr>\n",
            "  <ok>true</ok>\n</row>\n\n"
        ),
    );
    let error = session
        .execute(
            "SELECT query_to_xml(
                'INSERT INTO \"t-xml\" (id) VALUES (2) RETURNING id', false, false, ''
             )",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    let error = session
        .execute(
            "SELECT query_to_xml('WITH w AS (
                INSERT INTO \"t-xml\" (id) VALUES (3) RETURNING id
             ) SELECT id FROM w', false, false, '')",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert!(error.to_string().starts_with("SELECT is not allowed"));
    let result = session.execute("SELECT count(*) FROM \"t-xml\"").unwrap();
    assert_eq!(result.rows[0][0].to_cell(), "1");
}

#[test]
fn xml_constructors_infer_names_from_column_references() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session
        .execute(
            r#"SELECT xmlelement(NAME BIG,
                  xmlattributes(x.VAL),
                  xmlforest(x.VAL, ("VaL"), amount+1 AS "Ca'Se"))
               FROM (VALUES (1,2,3)) AS x(val,"VaL",amount)"#,
        )
        .unwrap();
    assert_eq!(
        result.rows[0][0].to_cell(),
        r#"<big val="1"><val>1</val><VaL>2</VaL><Ca_x0027_Se>4</Ca_x0027_Se></big>"#
    );
    for (sql, kind) in [
        ("SELECT xmlforest(1) WHERE false", "element"),
        ("SELECT xmlelement(NAME X, xmlattributes(1))", "attribute"),
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42601");
        assert_eq!(
            error.to_string(),
            format!("unnamed XML {kind} value must be a column reference")
        );
    }
}

#[test]
fn schema_xml_maps_readable_relations_in_name_order() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"CREATE SCHEMA "xml Set";
               CREATE TABLE "xml Set".z (a int);
               INSERT INTO "xml Set".z VALUES (NULL);
               CREATE TABLE "xml Set".a (b text);
               INSERT INTO "xml Set".a VALUES ('&');
               CREATE VIEW "xml Set".m AS SELECT a FROM "xml Set".z;
               CREATE SEQUENCE "xml Set".s"#,
        )
        .unwrap();
    let result = session
        .execute(r#"SELECT schema_to_xml('xml Set', true, false, 'urn:x')"#)
        .unwrap();
    assert_eq!(
        result.rows[0][0].to_cell(),
        concat!(
            "<_x0078_ml_x0020_Set xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"urn:x\">\n\n",
            "<a>\n\n<row>\n  <b>&amp;</b>\n</row>\n\n</a>\n\n",
            "<m>\n\n<row>\n  <a xsi:nil=\"true\"/>\n</row>\n\n</m>\n\n",
            "<z>\n\n<row>\n  <a xsi:nil=\"true\"/>\n</row>\n\n</z>\n\n",
            "</_x0078_ml_x0020_Set>\n",
        )
    );
    assert_eq!(
        session
            .execute("SELECT schema_to_xml('xml_schema_absent', true, false, '')")
            .unwrap_err()
            .sqlstate(),
        "3F000"
    );
}
