//! XML Schema annotations for PostgreSQL SQL/XML query results.
use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::map_sql_identifier_to_xml_name as xml_name;

pub(crate) fn query_schema(
    columns: &[(String, Option<String>)],
    database: &str,
    relation: Option<(&str, &str)>,
    namespace: &str,
    nulls: bool,
    forest: bool,
    binary_hex: bool,
) -> String {
    let mut output =
        String::from("<xsd:schema\n    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"");
    if !namespace.is_empty() {
        write!(
            output,
            "\n    targetNamespace=\"{namespace}\"\n    elementFormDefault=\"qualified\""
        )
        .unwrap();
    }
    output.push_str(">\n\n");

    let mut definitions = BTreeSet::new();
    let mut types = Vec::with_capacity(columns.len());
    for (_, pg_type) in columns {
        let pg_type = pg_type.as_deref().unwrap_or("text");
        let (name, definition) = type_definition(pg_type, database, binary_hex);
        if definitions.insert(name.clone()) {
            output.push_str(&definition);
        }
        types.push(name);
    }

    let component = |name: &str| xml_name(name, true).replace('.', "_x002E_");
    let suffix = relation.map_or_else(String::new, |(schema, relation)| {
        format!(
            ".{}.{}.{}",
            component(database),
            component(schema),
            component(relation),
        )
    });
    let row_type = format!("RowType{suffix}");
    write!(
        output,
        "<xsd:complexType name=\"{row_type}\">\n  <xsd:sequence>\n"
    )
    .unwrap();
    let optional = if nulls {
        "nillable=\"true\""
    } else {
        "minOccurs=\"0\""
    };
    for ((column, _), kind) in columns.iter().zip(types) {
        let column = xml_name(column, true);
        writeln!(
            output,
            "    <xsd:element name=\"{column}\" type=\"{kind}\" {optional}></xsd:element>"
        )
        .unwrap();
    }
    output.push_str("  </xsd:sequence>\n</xsd:complexType>\n\n");

    let element = relation.map_or_else(
        || {
            if forest {
                "row".to_string()
            } else {
                "table".to_string()
            }
        },
        |(_, name)| xml_name(name, true),
    );
    let element_type = if forest {
        row_type
    } else {
        let table_type = format!("TableType{suffix}");
        write!(
            output,
            concat!(
                "<xsd:complexType name=\"{0}\">\n",
                "  <xsd:sequence>\n",
                "    <xsd:element name=\"row\" type=\"{1}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n",
                "  </xsd:sequence>\n",
                "</xsd:complexType>\n\n",
            ),
            table_type, row_type,
        ).unwrap();
        table_type
    };
    write!(
        output,
        "<xsd:element name=\"{element}\" type=\"{element_type}\"/>\n\n"
    )
    .unwrap();
    output.push_str("</xsd:schema>");
    output
}

fn type_definition(pg_type: &str, database: &str, binary_hex: bool) -> (String, String) {
    if pg_type == "xml" {
        return (
            "XML".to_string(),
            concat!(
                "<xsd:complexType mixed=\"true\">\n",
                "  <xsd:sequence>\n",
                "    <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n",
                "  </xsd:sequence>\n",
                "</xsd:complexType>\n\n"
            )
            .to_string(),
        );
    }
    let bounded = |base: &str, min: i64, max: i64| {
        format!(
            "  <xsd:restriction base=\"xsd:{base}\">\n    <xsd:maxInclusive value=\"{max}\"/>\n    <xsd:minInclusive value=\"{min}\"/>\n  </xsd:restriction>\n"
        )
    };
    let scalar =
        |base: &str| format!("  <xsd:restriction base=\"xsd:{base}\"></xsd:restriction>\n");
    let restrict =
        |base: &str| format!("  <xsd:restriction base=\"xsd:{base}\">\n  </xsd:restriction>\n");
    let pattern = |base: &str, value: &str| {
        format!(
            "  <xsd:restriction base=\"xsd:{base}\">\n    <xsd:pattern value=\"{value}\"/>\n  </xsd:restriction>\n"
        )
    };
    const DATE: &str = r"\p{Nd}{4}-\p{Nd}{2}-\p{Nd}{2}";
    const TIME: &str = r"\p{Nd}{2}:\p{Nd}{2}:\p{Nd}{2}(.\p{Nd}+)?";
    const ZONE: &str = r"(\+|-)\p{Nd}{2}:\p{Nd}{2}";

    let (name, body) = match pg_type {
        "int2" | "smallint" => (
            "SMALLINT".into(),
            bounded("short", i64::from(i16::MIN), i64::from(i16::MAX)),
        ),
        "int4" | "integer" => (
            "INTEGER".into(),
            bounded("int", i64::from(i32::MIN), i64::from(i32::MAX)),
        ),
        "int8" | "bigint" => ("BIGINT".into(), bounded("long", i64::MIN, i64::MAX)),
        "numeric" => ("NUMERIC".into(), String::new()),
        "float4" | "real" => ("REAL".into(), scalar("float")),
        "float8" | "double precision" => ("DOUBLE".into(), scalar("double")),
        "varchar" => ("VARCHAR".into(), restrict("string")),
        "bpchar" => ("CHAR".into(), restrict("string")),
        "bool" | "boolean" => ("BOOLEAN".into(), scalar("boolean")),
        "date" => ("DATE".into(), pattern("date", DATE)),
        "time" => ("TIME".into(), pattern("time", TIME)),
        "timetz" => ("TIME_WTZ".into(), pattern("time", &format!("{TIME}{ZONE}"))),
        "timestamp" => (
            "TIMESTAMP".into(),
            pattern("dateTime", &format!("{DATE}T{TIME}")),
        ),
        "timestamptz" => (
            "TIMESTAMP_WTZ".into(),
            pattern("dateTime", &format!("{DATE}T{TIME}{ZONE}")),
        ),
        pg_type => {
            let body = match pg_type {
                "text" => restrict("string"),
                "bytea" => restrict(if binary_hex {
                    "hexBinary"
                } else {
                    "base64Binary"
                }),
                _ => String::new(),
            };
            (catalog_type_name(pg_type, database), body)
        }
    };
    let definition = format!("<xsd:simpleType name=\"{name}\">\n{body}</xsd:simpleType>\n\n");
    (name, definition)
}

fn catalog_type_name(pg_type: &str, database: &str) -> String {
    let element_type = pg_type.trim_end_matches("[]");
    let parts = crate::sql_identifier_parts(element_type).unwrap_or_default();
    let (schema, name) = match parts.as_slice() {
        [schema, name] => (schema.as_str(), name.as_str()),
        [name] => ("pg_catalog", name.as_str()),
        _ => ("pg_catalog", element_type),
    };
    let array_prefix = if pg_type == element_type { "" } else { "_" };
    format!(
        "UDT.{}.{}.{}",
        xml_name(database, true),
        xml_name(schema, true),
        xml_name(&format!("{array_prefix}{name}"), true)
    )
}
