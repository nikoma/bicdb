use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use sxd_document::{dom, parser};
use sxd_xpath::{nodeset::Node, Context, Factory, Value as XPathValue, XPath};

use crate::{Result, SqlError, SqlValue};

fn invalid_xml(detail: impl std::fmt::Display) -> SqlError {
    SqlError::invalid_xml_content(format!("invalid XML content: {detail}"))
}

fn xml_text(value: &SqlValue) -> String {
    value.to_cell()
}

fn xml_declaration(value: &str) -> Option<(&str, &str)> {
    let value = value.trim_start_matches('\u{feff}');
    let after_prefix = value.strip_prefix("<?xml")?;
    let starts_with_whitespace = |character| matches!(character, ' ' | '\t' | '\r' | '\n');
    if !after_prefix.starts_with(starts_with_whitespace) {
        return None;
    }
    let end = value.find("?>")? + 2;
    Some(value.split_at(end))
}

fn strip_declaration(value: &str) -> &str {
    xml_declaration(value).map_or(value, |(_, body)| body)
}

fn wrapped_content(value: &str) -> String {
    format!(
        "<bicdb_xml_content>{}</bicdb_xml_content>",
        strip_declaration(value)
    )
}

const MAX_XML_ENTITY_DEPTH: usize = 32;
const MAX_XML_ENTITY_EXPANSION: usize = 1_048_576;

struct XmlDoctype {
    start: usize,
    end: usize,
    entities: BTreeMap<String, Option<String>>,
    external_subset: bool,
}

fn xml_doctype(value: &str) -> Result<Option<XmlDoctype>> {
    // A DTD can appear only in the prolog, outside comments and processing
    // instructions. Identical characters inside XML content are ordinary text.
    let mut prolog = value.trim_start_matches('\u{feff}');
    let start = loop {
        prolog = prolog.trim_start_matches([' ', '\t', '\r', '\n']);
        if prolog.starts_with("<!DOCTYPE") {
            break value.len() - prolog.len();
        }
        let (skip, terminator) = if prolog.starts_with("<!--") {
            (4, "-->")
        } else if prolog.starts_with("<?") {
            (2, "?>")
        } else {
            return Ok(None);
        };
        let Some(end) = prolog[skip..].find(terminator) else {
            return Ok(None);
        };
        prolog = &prolog[skip + end + terminator.len()..];
    };
    let bytes = value.as_bytes();
    let mut quote = None;
    let mut subset_depth = 0_u32;
    let mut end = None;
    for (offset, byte) in bytes[start + 9..].iter().copied().enumerate() {
        let index = start + 9 + offset;
        if let Some(active) = quote {
            if byte == active {
                quote = None;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'[' => subset_depth += 1,
            b']' if subset_depth > 0 => subset_depth -= 1,
            b'>' if subset_depth == 0 => {
                end = Some(index + 1);
                break;
            }
            _ => {}
        }
    }
    let end = end.ok_or_else(|| invalid_xml("unterminated document type declaration"))?;
    let declaration = &value[start..end];
    let subset_start = declaration.find('[');
    let subset_end = declaration.rfind(']');
    if subset_start.is_some() != subset_end.is_some() {
        return Err(invalid_xml("malformed document type declaration"));
    }
    let header_end = subset_start.unwrap_or(declaration.len());
    let header = &declaration[..header_end];
    let external_subset = header.contains(" SYSTEM ") || header.contains(" PUBLIC ");
    let entities = match (subset_start, subset_end) {
        (Some(start), Some(end)) if start < end => {
            parse_xml_entity_declarations(&declaration[start + 1..end])?
        }
        _ => BTreeMap::new(),
    };
    Ok(Some(XmlDoctype {
        start,
        end,
        entities,
        external_subset,
    }))
}

fn parse_xml_entity_declarations(subset: &str) -> Result<BTreeMap<String, Option<String>>> {
    let mut entities = BTreeMap::new();
    let mut remaining = subset;
    while let Some(start) = remaining.find("<!ENTITY") {
        remaining = &remaining[start + 8..];
        let bytes = remaining.as_bytes();
        let mut quote = None;
        let mut declaration_end = None;
        for (index, byte) in bytes.iter().copied().enumerate() {
            if let Some(active) = quote {
                if byte == active {
                    quote = None;
                }
            } else {
                match byte {
                    b'\'' | b'"' => quote = Some(byte),
                    b'>' => {
                        declaration_end = Some(index);
                        break;
                    }
                    _ => {}
                }
            }
        }
        let declaration_end =
            declaration_end.ok_or_else(|| invalid_xml("unterminated entity declaration"))?;
        let declaration = remaining[..declaration_end].trim();
        remaining = &remaining[declaration_end + 1..];
        if declaration.starts_with('%') {
            continue;
        }
        let name_end = declaration
            .find(char::is_whitespace)
            .ok_or_else(|| invalid_xml("malformed entity declaration"))?;
        let name = xml_name(&declaration[..name_end])?.to_string();
        let definition = declaration[name_end..].trim_start();
        let replacement = match definition.as_bytes().first().copied() {
            Some(quote @ (b'\'' | b'"')) => {
                let tail = &definition[1..];
                let end = tail
                    .find(quote as char)
                    .ok_or_else(|| invalid_xml("unterminated entity value"))?;
                if !tail[end + 1..].trim().is_empty() {
                    return Err(invalid_xml("malformed entity declaration"));
                }
                Some(tail[..end].to_string())
            }
            _ if definition.starts_with("SYSTEM ") || definition.starts_with("PUBLIC ") => None,
            _ => return Err(invalid_xml("malformed entity declaration")),
        };
        entities.insert(name, replacement);
    }
    Ok(entities)
}

fn expand_xml_entities(
    value: &str,
    entities: &BTreeMap<String, Option<String>>,
    external_subset: bool,
    depth: usize,
    budget: &mut usize,
) -> Result<String> {
    if depth > MAX_XML_ENTITY_DEPTH {
        return Err(invalid_xml("entity expansion depth limit exceeded"));
    }
    let mut output = String::with_capacity(value.len());
    let mut cursor = 0;
    while let Some(relative) = value[cursor..].find('&') {
        let start = cursor + relative;
        output.push_str(&value[cursor..start]);
        let Some(relative_end) = value[start..].find(';') else {
            output.push_str(&value[start..]);
            cursor = value.len();
            break;
        };
        let end = start + relative_end;
        let name = &value[start + 1..end];
        if name.starts_with('#') || matches!(name, "amp" | "lt" | "gt" | "apos" | "quot") {
            output.push_str(&value[start..=end]);
        } else if let Some(replacement) = entities.get(name) {
            if let Some(replacement) = replacement {
                let expanded =
                    expand_xml_entities(replacement, entities, external_subset, depth + 1, budget)?;
                *budget = budget
                    .checked_sub(expanded.len())
                    .ok_or_else(|| invalid_xml("entity expansion size limit exceeded"))?;
                output.push_str(&expanded);
            }
        } else if external_subset {
            // PostgreSQL/libxml does not fetch external subsets for the xml type.
        } else {
            output.push_str(&value[start..=end]);
        }
        cursor = end + 1;
    }
    output.push_str(&value[cursor..]);
    Ok(output)
}

fn xml_for_parser(value: &str) -> Result<String> {
    let doctype = xml_doctype(value)?;
    xml_for_parser_with_doctype(value, doctype.as_ref())
}

fn xml_for_parser_with_doctype(value: &str, doctype: Option<&XmlDoctype>) -> Result<String> {
    let Some(doctype) = doctype else {
        return Ok(value.to_string());
    };
    let mut budget = MAX_XML_ENTITY_EXPANSION;
    let body = expand_xml_entities(
        &value[doctype.end..],
        &doctype.entities,
        doctype.external_subset,
        0,
        &mut budget,
    )?;
    Ok(format!("{}{}", &value[..doctype.start], body))
}

pub(crate) fn validate_xml(value: &str, document: bool) -> Result<()> {
    if let Some((declaration, _)) = xml_declaration(value) {
        // Content parsing can wrap multiple root elements and strip the
        // outer declaration. Analyze the declaration before that fallback.
        parser::parse(&format!("{declaration}<bicdb/>"))
            .map_err(|_| invalid_xml("invalid XML declaration"))?;
    }
    // An explicit DTD describes an XML document even in CONTENT mode.
    let doctype = xml_doctype(value)?;
    let document = document || doctype.is_some();
    let prepared = xml_for_parser_with_doctype(value, doctype.as_ref())?;
    let parsed = if document {
        parser::parse(&prepared)
    } else {
        parser::parse(&prepared).or_else(|_| parser::parse(&wrapped_content(&prepared)))
    };
    let parsed = parsed.map_err(invalid_xml)?;
    if document
        && !parsed
            .as_document()
            .root()
            .children()
            .iter()
            .any(|child| matches!(child, dom::ChildOfRoot::Element(_)))
    {
        return Err(invalid_xml("document has no root element"));
    }
    Ok(())
}

fn xml_declaration_attribute(declaration: &str, name: &str) -> Option<String> {
    let bytes = declaration.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        let start = index;
        while bytes
            .get(index)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            index += 1;
        }
        let attribute = &declaration[start..index];
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index) != Some(&b'=') {
            index = index.saturating_add(1);
            continue;
        }
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        let quote = *bytes.get(index)?;
        if !matches!(quote, b'\'' | b'"') {
            return None;
        }
        index += 1;
        let value_start = index;
        while bytes.get(index).is_some_and(|byte| *byte != quote) {
            index += 1;
        }
        let value = declaration.get(value_start..index)?;
        index += 1;
        if attribute.eq_ignore_ascii_case(name) {
            return Some(value.to_string());
        }
    }
    None
}

pub(crate) fn normalize_xml(value: &str) -> Result<String> {
    validate_xml(value, false)?;
    Ok(normalize_validated_xml(value))
}

// SQL input follows the setting or the authored XMLPARSE option. Values
// already stored with type XML use normalize_xml when read under any setting.
fn parse_xml_value(value: &str, document: bool) -> Result<String> {
    validate_xml(value, document).map_err(|error| {
        if document {
            SqlError::data_exception_public("2200M", "invalid XML document", None)
        } else {
            error
        }
    })?;
    Ok(normalize_validated_xml(value))
}

pub(crate) fn xml_input_value(value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    parse_xml_value(&value.to_cell(), xml_input_is_document()).map(SqlValue::String)
}

/// Validate a scalar XML value using the settings at PostgreSQL Bind time.
pub fn decode_postgres_xml_parameter(bytes: &[u8], binary: bool, document: bool) -> Result<String> {
    let mut encoding = "UTF8";
    if binary {
        // A binary XML declaration determines the encoding. Its attributes
        // are ASCII, so they can be read before decoding the rest of the value.
        let header = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
        if header.starts_with(b"<?xml") {
            let declared = header
                .windows(2)
                .position(|pair| pair == b"?>")
                .and_then(|end| std::str::from_utf8(&header[..end + 2]).ok())
                .and_then(xml_declaration)
                .and_then(|(declaration, _)| xml_declaration_attribute(declaration, "encoding"));
            if let Some(declared) = declared {
                encoding = crate::normalized_encoding_name(&SqlValue::String(declared.clone()))
                    .map_err(|_| {
                        SqlError::invalid_parameter_value(format!(
                            "invalid encoding name \"{declared}\""
                        ))
                    })?;
            }
        }
    }
    let value = crate::decode_server_text(bytes, encoding)?;
    parse_xml_value(&value, document)
}

pub(crate) fn xml_assignment_value(
    column: &crate::ColumnSchema,
    source: Option<&str>,
    value: SqlValue,
) -> Result<SqlValue> {
    match source {
        Some("xml") => Ok(value),
        None | Some("unknown") => xml_input_value(value),
        Some(source) => Err(SqlError::coded(
            "42804",
            format!(
                "column \"{}\" is of type xml but expression is of type {}",
                column.name,
                crate::common_type_display(source)
            ),
            None,
            Some("You will need to rewrite or cast the expression.".to_owned()),
        )),
    }
}

fn normalize_validated_xml(value: &str) -> String {
    let value = value.trim_start_matches('\u{feff}');
    let Some((declaration, body)) = xml_declaration(value) else {
        return value.to_string();
    };
    let declaration = &declaration[5..declaration.len() - 2];
    let version =
        xml_declaration_attribute(declaration, "version").unwrap_or_else(|| "1.0".to_string());
    let standalone = xml_declaration_attribute(declaration, "standalone");
    if version == "1.0" && standalone.is_none() {
        return body.to_string();
    }
    let standalone = standalone
        .map(|standalone| format!(" standalone=\"{standalone}\""))
        .unwrap_or_default();
    format!("<?xml version=\"{version}\"{standalone}?>{body}")
}

fn escape_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_attribute(value: &str) -> String {
    escape_text(value).replace('"', "&quot;")
}

fn xml_name(value: &str) -> Result<&str> {
    let mut chars = value.chars();
    let valid_start = chars
        .next()
        .is_some_and(|ch| ch == '_' || ch == ':' || ch.is_alphabetic());
    if !valid_start
        || !chars.all(|ch| ch == '_' || ch == ':' || ch == '-' || ch == '.' || ch.is_alphanumeric())
    {
        return Err(SqlError::invalid_parameter_value(format!(
            "invalid XML name: {value}"
        )));
    }
    Ok(value)
}

fn map_sql_identifier_to_xml_name(identifier: &str, fully_escaped: bool) -> String {
    use std::fmt::Write;

    let reserved_prefix = fully_escaped
        && identifier
            .get(..3)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("xml"));
    let mut name = String::with_capacity(identifier.len());
    let mut characters = identifier.chars().enumerate().peekable();
    while let Some((index, character)) = characters.next() {
        let letter = u32::from(character) <= 0xffff && character.is_alphabetic();
        let valid = match index {
            0 => letter || character == '_',
            _ => {
                letter
                    || character.is_ascii_digit()
                    || matches!(character, '.' | '-' | '_' | '\u{00b7}')
                    || (!fully_escaped && character == ':')
                    || unicode_normalization::char::is_combining_mark(character)
            }
        };
        let starts_escape =
            character == '_' && characters.peek().is_some_and(|(_, next)| *next == 'x');
        if valid && !starts_escape && !(index == 0 && reserved_prefix) {
            name.push(character);
        } else {
            write!(name, "_x{:04X}_", u32::from(character))
                .expect("writing to a String cannot fail");
        }
    }
    name
}

/// The constructor rewrite uses a NULL label for an unnamed non-column
/// expression. Reject it during parsing, including queries over empty inputs.
pub(crate) fn validate_xml_constructor_names(
    statements: &[sqlparser::ast::Statement],
) -> Result<()> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value, ValueWithSpan,
    };
    use std::ops::ControlFlow;

    let invalid_name = |expression: &Expr| {
        let Expr::Function(function) = expression else {
            return ControlFlow::Continue(());
        };
        let kind = match function.name.to_string().as_str() {
            "bicdb_xmlattributes" => "attribute",
            "bicdb_xmlforest" => "element",
            _ => return ControlFlow::Continue(()),
        };
        let FunctionArguments::List(arguments) = &function.args else {
            return ControlFlow::Continue(());
        };
        if arguments.args.iter().step_by(2).any(|argument| {
            matches!(
                argument,
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(ValueWithSpan {
                    value: Value::Null,
                    ..
                })))
            )
        }) {
            ControlFlow::Break(kind)
        } else {
            ControlFlow::Continue(())
        }
    };
    for statement in statements {
        if let ControlFlow::Break(kind) = sqlparser::ast::visit_expressions(statement, invalid_name)
        {
            return Err(SqlError::data_exception_public(
                "42601",
                format!("unnamed XML {kind} value must be a column reference"),
                None,
            ));
        }
    }
    Ok(())
}

const XML_ATTRIBUTES_MARKER: &str = "\0bicdb:xmlattributes:";

thread_local! {
    static XML_BINARY_HEX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static XML_INPUT_DOCUMENT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn xml_input_is_document() -> bool {
    XML_INPUT_DOCUMENT.with(std::cell::Cell::get)
}

pub(crate) struct XmlSettingsScope {
    binary_hex: bool,
    input_document: bool,
}

impl XmlSettingsScope {
    pub(crate) fn enter(gucs: &std::collections::HashMap<String, String>) -> Self {
        let hex = gucs.get("xmlbinary").is_some_and(|mode| mode == "hex");
        let document = gucs.get("xmloption").is_some_and(|mode| mode == "document");
        Self {
            binary_hex: XML_BINARY_HEX.with(|setting| setting.replace(hex)),
            input_document: XML_INPUT_DOCUMENT.with(|setting| setting.replace(document)),
        }
    }
}

impl Drop for XmlSettingsScope {
    fn drop(&mut self) {
        XML_BINARY_HEX.with(|setting| setting.set(self.binary_hex));
        XML_INPUT_DOCUMENT.with(|setting| setting.set(self.input_document));
    }
}

fn xml_scalar_text(value: &SqlValue, pg_type: Option<&str>) -> Result<String> {
    if let Some(element_type) = pg_type.and_then(|kind| kind.strip_suffix("[]")) {
        let decoded;
        let value = if let (SqlValue::String(_), Some(pg_type)) = (value, pg_type) {
            decoded = crate::cast_value_to_pg_type(value.clone(), pg_type)?;
            &decoded
        } else {
            value
        };
        let Some((array, _)) = crate::array_json_parts(value, "SQL/XML")? else {
            return Ok(String::new());
        };
        let mut elements = Vec::new();
        crate::flatten_array_json(array, &mut elements);
        let mut output = String::new();
        for element in elements.into_iter().filter(|element| !element.is_null()) {
            let element = crate::json_to_sql_value(element);
            output.push_str("<element>");
            output.push_str(&xml_content(&element, Some(element_type))?);
            output.push_str("</element>");
        }
        return Ok(output);
    }
    match pg_type {
        Some("bool") => {
            let value = crate::sql_value_bool(value)
                .ok_or_else(|| SqlError::InvalidSql("expected a boolean XML value".into()))?;
            return Ok(value.to_string());
        }
        Some("date") => {
            let value = crate::PgDate::from_postgres_text(&xml_text(value))
                .map_err(|_| xml_datetime_out_of_range("date", false))?;
            if value.epoch_days().is_none() {
                return Err(xml_datetime_out_of_range("date", true));
            }
            return Ok(value.to_iso_text());
        }
        Some("timestamp" | "timestamptz") => {
            return xml_timestamp(value, pg_type == Some("timestamptz"))
        }
        Some("bytea") => {}
        _ => return Ok(xml_text(value)),
    }
    use base64::Engine;
    let bytes = crate::bytea_argument(value)?.unwrap_or_default();
    Ok(if XML_BINARY_HEX.with(std::cell::Cell::get) {
        hex::encode_upper(bytes)
    } else {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    })
}

fn xml_datetime_out_of_range(kind: &str, infinite: bool) -> SqlError {
    SqlError::coded(
        "22008",
        format!("{kind} out of range"),
        infinite.then(|| format!("XML does not support infinite {kind} values.")),
        None,
    )
}

fn xml_timestamp(value: &SqlValue, with_timezone: bool) -> Result<String> {
    let timestamp = crate::PgTimestamp::from_postgres_text(&xml_text(value), with_timezone)
        .map_err(|_| xml_datetime_out_of_range("timestamp", false))?;
    if timestamp.finite_micros().is_none() {
        return Err(xml_datetime_out_of_range("timestamp", true));
    }
    if !with_timezone {
        return Ok(timestamp.to_iso_text(false).replacen(' ', "T", 1));
    }
    let mut text = crate::render_timestamptz(timestamp).replacen(' ', "T", 1);
    let offset_end = text.strip_suffix(" BC").unwrap_or(&text).len();
    if text[..offset_end]
        .rfind(['+', '-'])
        .is_some_and(|offset| offset_end - offset == 3)
    {
        text.insert_str(offset_end, ":00");
    }
    Ok(text)
}

fn xml_content(value: &SqlValue, pg_type: Option<&str>) -> Result<String> {
    let text = xml_scalar_text(value, pg_type)?;
    Ok(
        if pg_type == Some("xml") || crate::is_array_pg_type(pg_type) {
            text
        } else {
            escape_text(&text).replace('\r', "&#x0d;")
        },
    )
}

/// Nesting cap for XML documents and XPath expressions.
///
/// Both are attacker-supplied and both drive recursion — the document
/// through every recursive tree walk (serialization above all), the
/// expression through the XPath library's own recursive-descent parser,
/// which offers no depth knob of its own. Bounding the TEXT before either
/// reaches a recursive consumer is the host-defined budget the rules in
/// `bicdb_core::parse_budget` require, applied where the third-party parser
/// cannot be changed.
const MAX_XML_NESTING_DEPTH: usize = 100;

/// Refuse text whose bracket/paren nesting exceeds the budget.
///
/// Deliberately structural rather than semantic: it counts the delimiters
/// that make a recursive parser descend, ignores anything inside quotes, and
/// never tries to be a parser itself.
fn check_nesting_budget(
    text: &str,
    open: &[char],
    close: &[char],
    label: &'static str,
) -> Result<()> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for character in text.chars() {
        match quote {
            Some(active) => {
                if character == active {
                    quote = None;
                }
            }
            None => {
                if character == '\'' || character == '"' {
                    quote = Some(character);
                } else if open.contains(&character) {
                    depth += 1;
                    if depth > MAX_XML_NESTING_DEPTH {
                        return Err(SqlError::invalid_parameter_value(format!(
                            "{label} is nested deeper than {MAX_XML_NESTING_DEPTH} levels"
                        )));
                    }
                } else if close.contains(&character) {
                    depth = depth.saturating_sub(1);
                }
            }
        }
    }
    Ok(())
}

fn parse_xpath_document(xml: &str) -> Result<sxd_document::Package> {
    // `<` counts element and declaration starts alike; a document that
    // trips this bound is far past anything a recursive walk of the parsed
    // tree could survive.
    check_nesting_budget_for_document(xml)?;
    parser::parse(&xml_for_parser(xml)?).map_err(invalid_xml)
}

/// Element-nesting budget for a document: count `<tag>` opens against
/// `</tag>` closes, ignoring self-closing tags, comments and declarations.
fn check_nesting_budget_for_document(xml: &str) -> Result<()> {
    let bytes = xml.as_bytes();
    let mut depth = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'<' {
            index += 1;
            continue;
        }
        let Some(end) = xml[index..].find('>').map(|offset| index + offset) else {
            break;
        };
        let tag = &xml[index + 1..end];
        let is_close = tag.starts_with('/');
        let is_meta = tag.starts_with('!') || tag.starts_with('?');
        let is_self_closing = tag.ends_with('/');
        if !is_meta {
            if is_close {
                depth = depth.saturating_sub(1);
            } else if !is_self_closing {
                depth += 1;
                if depth > MAX_XML_NESTING_DEPTH {
                    return Err(SqlError::invalid_parameter_value(format!(
                        "XML document nests elements deeper than {MAX_XML_NESTING_DEPTH} levels"
                    )));
                }
            }
        }
        index = end + 1;
    }
    Ok(())
}

fn qualified_name(local_name: &str, prefix: Option<&str>) -> String {
    match prefix.filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => format!("{prefix}:{local_name}"),
        None => local_name.to_string(),
    }
}

fn serialize_element(
    element: dom::Element<'_>,
    inherited_namespaces: &BTreeMap<String, String>,
    output: &mut String,
    indent: Option<usize>,
) {
    let name = element.name();
    let local_name = name.local_part_clone();
    let element_name = qualified_name(&local_name, element.preferred_prefix().as_deref());
    let namespaces = element
        .namespaces_in_scope()
        .into_iter()
        .filter(|namespace| namespace.prefix() != "xml")
        .map(|namespace| (namespace.prefix().to_string(), namespace.uri().to_string()))
        .collect::<BTreeMap<_, _>>();
    output.push('<');
    output.push_str(&element_name);
    for (prefix, uri) in &namespaces {
        if inherited_namespaces.get(prefix) == Some(uri) {
            continue;
        }
        if prefix.is_empty() {
            output.push_str(" xmlns=\"");
        } else {
            output.push_str(" xmlns:");
            output.push_str(prefix);
            output.push_str("=\"");
        }
        output.push_str(&escape_attribute(uri));
        output.push('"');
    }
    for attribute in element.attributes() {
        let attribute_name = attribute.name().local_part_clone();
        output.push(' ');
        output.push_str(&qualified_name(
            &attribute_name,
            attribute.preferred_prefix().as_deref(),
        ));
        output.push_str("=\"");
        output.push_str(&escape_attribute(&attribute.value()));
        output.push('"');
    }
    let children = element.children();
    if children.is_empty() {
        output.push_str("/>");
        return;
    }
    output.push('>');
    let indent = indent.filter(|_| {
        element.attribute_value(("http://www.w3.org/XML/1998/namespace", "space")).as_deref() != Some("preserve")
            && children.iter().any(|child| !matches!(child, dom::ChildOfElement::Text(_)))
            && children.iter().all(|child| {
                !matches!(child, dom::ChildOfElement::Text(text) if !text.text().trim().is_empty())
            })
    });
    for child in children {
        if let Some(level) = indent {
            if matches!(child, dom::ChildOfElement::Text(_)) {
                continue;
            }
            write_xml_indent(output, level + 1);
        }
        serialize_child(child, &namespaces, output, indent.map(|level| level + 1));
    }
    if let Some(level) = indent {
        write_xml_indent(output, level);
    }
    output.push_str("</");
    output.push_str(&element_name);
    output.push('>');
}

fn write_xml_indent(output: &mut String, level: usize) {
    output.push('\n');
    for _ in 0..level {
        output.push_str("  ");
    }
}

fn serialize_child(
    child: dom::ChildOfElement<'_>,
    namespaces: &BTreeMap<String, String>,
    output: &mut String,
    indent: Option<usize>,
) {
    match child {
        dom::ChildOfElement::Element(child) => serialize_element(child, namespaces, output, indent),
        dom::ChildOfElement::Text(text) => output.push_str(&escape_text(&text.text())),
        dom::ChildOfElement::Comment(comment) => {
            output.push_str("<!--");
            output.push_str(&comment.text());
            output.push_str("-->");
        }
        dom::ChildOfElement::ProcessingInstruction(pi) => {
            output.push_str("<?");
            output.push_str(&pi.target());
            if let Some(value) = pi.value() {
                output.push(' ');
                output.push_str(&value);
            }
            output.push_str("?>");
        }
    }
}

fn wrapped_xml_children(package: &sxd_document::Package) -> Vec<dom::ChildOfElement<'_>> {
    package
        .as_document()
        .root()
        .children()
        .into_iter()
        .find_map(dom::ChildOfRoot::element)
        .expect("wrapped XML has one enclosing element")
        .children()
}

fn indent_xml_content(value: &str, document: bool) -> Result<String> {
    check_nesting_budget_for_document(value)?;
    let doctype = xml_doctype(value)?;
    let prepared = xml_for_parser_with_doctype(value, doctype.as_ref())?;
    let package = parser::parse(&wrapped_content(&prepared)).map_err(invalid_xml)?;
    let children = wrapped_xml_children(&package);
    let mixed = children.iter().any(
        |child| matches!(child, dom::ChildOfElement::Text(text) if !text.text().trim().is_empty()),
    );
    let doctype_after = if let Some(doctype) = &doctype {
        let prefix =
            parser::parse(&wrapped_content(&value[..doctype.start])).map_err(invalid_xml)?;
        Some(
            wrapped_xml_children(&prefix)
                .iter()
                .filter(|child| !matches!(child, dom::ChildOfElement::Text(_)))
                .count(),
        )
    } else {
        None
    };

    let mut output = String::new();
    if let Some((declaration, _)) = xml_declaration(value).filter(|_| document) {
        let version =
            xml_declaration_attribute(declaration, "version").unwrap_or_else(|| "1.0".into());
        output = format!("<?xml version=\"{version}\" encoding=\"UTF-8\"");
        if let Some(standalone) = xml_declaration_attribute(declaration, "standalone") {
            output.push_str(&format!(" standalone=\"{standalone}\""));
        }
        output.push_str("?>");
    }
    let namespaces = BTreeMap::new();
    let last = children.len().saturating_sub(1);
    let mut markup = 0;
    for (index, child) in children.into_iter().enumerate() {
        if matches!(child, dom::ChildOfElement::Text(_)) {
            if mixed || (index == last && !document && doctype.is_none()) {
                serialize_child(child, &namespaces, &mut output, None);
            }
            continue;
        }
        if let Some(doctype) = &doctype {
            if doctype_after == Some(markup) {
                if !output.is_empty() {
                    output.push('\n');
                }
                output.push_str(&value[doctype.start..doctype.end]);
            }
        }
        if !output.is_empty() {
            output.push('\n');
        }
        serialize_child(child, &namespaces, &mut output, Some(0));
        markup += 1;
    }
    if !document && doctype.is_some() {
        output.push('\n');
    }
    Ok(output)
}

fn xml_serialize(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<SqlValue> {
    let input_type = arg_types
        .and_then(|types| types.first())
        .and_then(Option::as_deref);
    if let Some(input_type) = input_type.filter(|kind| !matches!(*kind, "xml" | "unknown")) {
        return Err(SqlError::data_exception_public(
            "42804",
            format!(
                "argument of XMLSERIALIZE must be type xml, not type {}",
                crate::common_type_display(input_type)
            ),
            None,
        ));
    }
    let target = args.get(2).map(xml_text);
    let target_data_type = target.as_deref().and_then(|declaration| {
        sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
            .try_with_sql(declaration)
            .ok()
            .and_then(|mut parser| parser.parse_data_type().ok())
    });
    let target_type = target_data_type
        .as_ref()
        .and_then(|data_type| crate::pg_type_from_data_type(data_type).ok());
    if let Some((kind, _)) = target_type.as_ref() {
        if crate::pg_type_oid_by_name(kind).is_some()
            && !matches!(kind.as_str(), "text" | "varchar" | "bpchar" | "name")
        {
            let target = target.as_deref().unwrap_or(kind);
            return Err(SqlError::cannot_coerce(format!(
                "cannot cast XMLSERIALIZE result to {}",
                if kind == "char" {
                    target.to_owned()
                } else {
                    crate::common_type_display(kind)
                }
            )));
        }
    }
    let character_target = match (target_type, target_data_type) {
        (Some((kind, _)), Some(data_type)) if matches!(kind.as_str(), "varchar" | "bpchar") => {
            match crate::pg_type_modifier_from_data_type(&data_type)? {
                Some(crate::PgTypeModifier::Character { length }) => Some((kind, length as usize)),
                _ => None,
            }
        }
        _ => None,
    };
    let Some(value) = args
        .first()
        .filter(|value| !matches!(value, SqlValue::Null))
    else {
        return Ok(SqlValue::Null);
    };
    let value = xml_text(value);
    let document = name == "bicdb_xmlserialize_document";
    if document && validate_xml(&value, true).is_err() {
        return Err(SqlError::data_exception_public(
            "2200L",
            "not an XML document",
            None,
        ));
    }
    if !document {
        validate_xml(&value, false)?;
    }
    let value = if matches!(args.get(1), Some(SqlValue::Bool(true))) {
        indent_xml_content(&value, document)?
    } else {
        normalize_validated_xml(&value)
    };
    if let Some((kind, length)) = character_target {
        if value.chars().skip(length).any(|character| character != ' ') {
            return Err(SqlError::data_exception_public(
                "22001",
                format!(
                    "value too long for type {}({length})",
                    crate::common_type_display(&kind)
                ),
                None,
            ));
        }
    }
    Ok(SqlValue::String(value))
}

fn xpath_node_xml(node: Node<'_>) -> String {
    match node {
        Node::Element(element) => {
            let mut output = String::new();
            serialize_element(element, &BTreeMap::new(), &mut output, None);
            output
        }
        Node::Attribute(attribute) => escape_text(&attribute.value()),
        Node::Text(text) => escape_text(&text.text()),
        Node::Comment(comment) => format!("<!--{}-->", comment.text()),
        Node::ProcessingInstruction(pi) => match pi.value() {
            Some(value) => format!("<?{} {}?>", pi.target(), value),
            None => format!("<?{}?>", pi.target()),
        },
        Node::Namespace(namespace) => escape_text(namespace.uri()),
        Node::Root(root) => root
            .children()
            .into_iter()
            .filter_map(dom::ChildOfRoot::element)
            .map(|element| {
                let mut output = String::new();
                serialize_element(element, &BTreeMap::new(), &mut output, None);
                output
            })
            .collect(),
    }
}

fn xpath_results(
    xml: &str,
    expression: &str,
    namespaces: Option<&JsonValue>,
) -> Result<Vec<String>> {
    let (package, xpath) = parse_xpath_input(xml, expression)?;
    let context = xpath_context(namespaces);
    let value = evaluate_xpath(&xpath, &context, package.as_document().root().into())?;
    Ok(match value {
        XPathValue::Nodeset(nodes) => nodes
            .document_order()
            .into_iter()
            .map(xpath_node_xml)
            .collect(),
        other => vec![other.string()],
    })
}

fn parse_xpath_input(xml: &str, expression: &str) -> Result<(sxd_document::Package, XPath)> {
    // PostgreSQL checks an empty expression first. Other syntax errors
    // follow XML document validation.
    require_xpath_expression(expression, "empty XPath expression")?;
    let package = parse_xpath_document(xml)?;
    Ok((package, compile_xpath(expression)?))
}

fn require_xpath_expression(expression: &str, message: &'static str) -> Result<()> {
    if expression.is_empty() {
        return Err(SqlError::data_exception_public("10608", message, None));
    }
    Ok(())
}

fn compile_xpath(expression: &str) -> Result<XPath> {
    check_nesting_budget(expression, &['(', '['], &[')', ']'], "XPath expression")?;
    Factory::new().build(expression).map_err(|_| {
        SqlError::coded(
            "10608",
            "invalid XPath expression",
            Some("Invalid expression".to_string()),
            None,
        )
    })
}

fn xpath_context<'d>(namespaces: Option<&JsonValue>) -> Context<'d> {
    let mut context = Context::new();
    if let Some(JsonValue::Array(entries)) = namespaces {
        for entry in entries {
            if let Some(pair) = entry.as_array() {
                if let [JsonValue::String(prefix), JsonValue::String(uri)] = pair.as_slice() {
                    context.set_namespace(prefix, uri);
                }
            }
        }
    }
    context
}

fn evaluate_xpath<'d>(
    xpath: &XPath,
    context: &Context<'d>,
    node: Node<'d>,
) -> Result<XPathValue<'d>> {
    xpath.evaluate(context, node).map_err(|error| {
        SqlError::invalid_parameter_value(format!("XPath evaluation failed: {error}"))
    })
}

struct XmlTableColumn<'a> {
    name: &'a str,
    xpath: Option<XPath>,
    xml: bool,
    numeric: bool,
    required: bool,
    default: JsonValue,
}

fn xml_table_columns<'a>(
    specs: &'a [JsonValue],
    args: &[SqlValue],
) -> Result<Vec<XmlTableColumn<'a>>> {
    let mut columns = Vec::with_capacity(specs.len());
    let mut seen_ordinality = false;
    for spec in specs {
        let Some(name) = spec.get("name").and_then(JsonValue::as_str) else {
            continue;
        };
        let flag = |key| spec.get(key).and_then(JsonValue::as_bool).unwrap_or(false);
        let argument = |key| {
            spec.get(key)
                .and_then(JsonValue::as_u64)
                .and_then(|index| args.get(index as usize))
        };
        let xpath = if flag("ordinality") {
            if std::mem::replace(&mut seen_ordinality, true) {
                return Err(SqlError::data_exception_public(
                    "42601",
                    "only one FOR ORDINALITY column is allowed",
                    None,
                ));
            }
            None
        } else {
            let path = match argument("path_arg") {
                Some(SqlValue::Null) => {
                    return Err(SqlError::with_detail(
                        SqlError::data_exception_public(
                            "22004",
                            "column filter expression must not be null",
                            None,
                        ),
                        format!("Filter for column \"{name}\" is null."),
                    ));
                }
                Some(path) => path.to_cell(),
                None => name.to_owned(),
            };
            require_xpath_expression(&path, "column path filter must not be empty string")?;
            Some(compile_xpath(&path)?)
        };
        columns.push(XmlTableColumn {
            name,
            xpath,
            xml: flag("xml"),
            numeric: flag("numeric"),
            required: flag("required"),
            default: argument("default_arg").map_or(JsonValue::Null, sql_value_json),
        });
    }
    Ok(columns)
}

fn xml_table_column_value<'d>(
    column: &XmlTableColumn<'_>,
    context: &Context<'d>,
    row: Node<'d>,
    ordinal: usize,
) -> Result<JsonValue> {
    let Some(xpath) = &column.xpath else {
        return Ok(JsonValue::Number(ordinal.into()));
    };
    let value = match evaluate_xpath(xpath, context, row)? {
        XPathValue::Nodeset(nodes) if nodes.size() == 0 => column.default.clone(),
        XPathValue::Nodeset(nodes) if column.xml => JsonValue::String(
            nodes
                .document_order()
                .into_iter()
                .map(xpath_node_xml)
                .collect(),
        ),
        XPathValue::Nodeset(nodes) => {
            if nodes.size() > 1 {
                return Err(SqlError::data_exception_public(
                    "21000",
                    "more than one value returned by column XPath expression",
                    None,
                ));
            }
            JsonValue::String(nodes.document_order()[0].string_value())
        }
        XPathValue::Boolean(value) if column.numeric => JsonValue::Number(u64::from(value).into()),
        value if column.xml => JsonValue::String(escape_text(&value.string())),
        value => JsonValue::String(value.string()),
    };
    if column.required && value.is_null() {
        return Err(SqlError::data_exception_public(
            "22004",
            format!("null is not allowed in column \"{}\"", column.name),
            None,
        ));
    }
    Ok(value)
}

fn xml_table_rows(args: &[SqlValue]) -> Result<SqlValue> {
    if args.len() < 4 || matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if matches!(args[1], SqlValue::Null) {
        return Err(SqlError::data_exception_public(
            "22004",
            "row filter expression must not be null",
            None,
        ));
    }
    let document = xml_text(&args[0]);
    let row_path = xml_text(&args[1]);
    require_xpath_expression(&row_path, "row path filter must not be empty string")?;
    let spec_json = match &args[2] {
        SqlValue::Json(value) => value.as_ref().clone(),
        value => serde_json::from_str::<JsonValue>(&value.to_cell())?,
    };
    let specs = spec_json.as_array().map_or(&[][..], Vec::as_slice);
    let namespaces = match &args[3] {
        SqlValue::Json(value) => Some(value.as_ref()),
        _ => None,
    };
    if namespaces
        .and_then(JsonValue::as_array)
        .is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.get(0).and_then(JsonValue::as_str) == Some(""))
        })
    {
        return Err(SqlError::data_exception_public(
            "0A000",
            "DEFAULT namespace is not supported",
            None,
        ));
    }
    let columns = xml_table_columns(specs, args)?;
    let package = parse_xpath_document(&document)?;
    let context = xpath_context(namespaces);
    let xpath = compile_xpath(&row_path)?;
    let XPathValue::Nodeset(rows) =
        evaluate_xpath(&xpath, &context, package.as_document().root().into())?
    else {
        return Ok(SqlValue::Json(JsonValue::Array(Vec::new()).into()));
    };
    let mut output = Vec::with_capacity(rows.size());
    for (index, row) in rows.document_order().into_iter().enumerate() {
        let mut values = serde_json::Map::new();
        for column in &columns {
            values.insert(
                column.name.to_owned(),
                xml_table_column_value(column, &context, row.clone(), index + 1)?,
            );
        }
        output.push(JsonValue::Object(values));
    }
    Ok(SqlValue::Json(JsonValue::Array(output).into()))
}

fn sql_value_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(value) => JsonValue::Bool(*value),
        SqlValue::Int(value) => JsonValue::Number((*value).into()),
        SqlValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        SqlValue::Json(value) => (**value).clone(),
        _ => JsonValue::String(value.to_cell()),
    }
}

pub(crate) fn finish_xml_agg(
    mut entries: Vec<(SqlValue, Vec<SqlValue>)>,
    order_by: &[sqlparser::ast::OrderByExpr],
) -> Result<SqlValue> {
    if !order_by.is_empty() {
        entries.sort_by(|left, right| crate::json_agg_entry_ordering(left, right, order_by));
    }
    Ok(concat_xml_fragments(entries.into_iter().filter_map(
        |(value, _)| (!matches!(value, SqlValue::Null)).then(|| xml_text(&value)),
    )))
}

fn concat_xml_fragments(values: impl IntoIterator<Item = String>) -> SqlValue {
    let mut version = None;
    let mut standalone = None;
    let mut content = String::new();
    let mut present = false;
    for value in values {
        let (declaration, body) = xml_declaration(&value)
            .map_or(("", value.as_str()), |(declaration, body)| {
                (&declaration[5..declaration.len() - 2], body)
            });
        let next_version = xml_declaration_attribute(declaration, "version");
        let next_standalone = xml_declaration_attribute(declaration, "standalone");
        if present {
            if next_version != version {
                version = None;
            }
            standalone = standalone.zip(next_standalone).map(|(left, right)| {
                if left == "no" || right == "no" {
                    "no".to_string()
                } else {
                    left
                }
            });
        } else {
            version = next_version;
            standalone = next_standalone;
            present = true;
        }
        content.push_str(body);
    }
    if !present {
        return SqlValue::Null;
    }
    let version = version.as_deref().unwrap_or("1.0");
    if version == "1.0" && standalone.is_none() {
        return SqlValue::String(content);
    }
    let standalone = standalone
        .map(|attribute| format!(" standalone=\"{attribute}\""))
        .unwrap_or_default();
    SqlValue::String(format!(
        "<?xml version=\"{version}\"{standalone}?>{content}"
    ))
}

pub(crate) fn eval_xml_function_value(
    name: &str,
    args: &[SqlValue],
    arg_types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = name
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase();
    let strict_null = args.iter().any(|value| matches!(value, SqlValue::Null));
    let value = match name.as_str() {
        "bicdb_xml_is_document" | "bicdb_xml_is_not_document" => {
            let [value] = args else {
                return Err(SqlError::invalid_parameter_value(
                    "IS DOCUMENT requires one argument",
                ));
            };
            let kind = arg_types
                .and_then(|kinds| kinds.first())
                .and_then(Option::as_deref);
            if let Some(kind) = kind.filter(|kind| !matches!(*kind, "unknown" | "xml")) {
                return Err(SqlError::data_exception_public(
                    "42804",
                    format!(
                        "argument of IS DOCUMENT must be type xml, not type {}",
                        crate::common_type_display(kind)
                    ),
                    None,
                ));
            }
            if strict_null {
                SqlValue::Null
            } else {
                let text = xml_text(value);
                if kind != Some("xml") {
                    validate_xml(&text, false)?;
                }
                let document = validate_xml(&text, true).is_ok();
                SqlValue::Bool(document == (name == "bicdb_xml_is_document"))
            }
        }
        "xml_is_well_formed" | "xml_is_well_formed_content" | "xml_is_well_formed_document" => {
            if strict_null {
                SqlValue::Null
            } else {
                let document = name == "xml_is_well_formed_document"
                    || (name == "xml_is_well_formed" && xml_input_is_document());
                SqlValue::Bool(validate_xml(&xml_text(&args[0]), document).is_ok())
            }
        }
        "xmlconcat" => {
            let mut values = Vec::with_capacity(args.len());
            for (index, value) in args.iter().enumerate() {
                let pg_type = arg_types
                    .and_then(|types| types.get(index))
                    .and_then(Option::as_deref);
                if let Some(pg_type) = pg_type.filter(|kind| !matches!(*kind, "xml" | "unknown")) {
                    return Err(SqlError::data_exception_public(
                        "42804",
                        format!(
                            "argument of XMLCONCAT must be type xml, not type {}",
                            crate::common_type_display(pg_type)
                        ),
                        None,
                    ));
                }
                if !matches!(value, SqlValue::Null) {
                    values.push(normalize_xml(&xml_text(value))?);
                }
            }
            concat_xml_fragments(values)
        }
        "bicdb_xmlattributes" => {
            let mut attributes = Vec::new();
            let mut names = std::collections::BTreeSet::new();
            for (pair_index, pair) in args.chunks(2).enumerate() {
                if let [name, value] = pair {
                    let name = xml_text(name);
                    if !names.insert(name.clone()) {
                        return Err(SqlError::data_exception_public(
                            "42601",
                            format!("XML attribute name \"{name}\" appears more than once"),
                            None,
                        ));
                    }
                    if matches!(value, SqlValue::Null) {
                        continue;
                    }
                    let value_type = arg_types
                        .and_then(|types| types.get(pair_index * 2 + 1))
                        .and_then(Option::as_deref);
                    attributes.push(JsonValue::Array(vec![
                        JsonValue::String(map_sql_identifier_to_xml_name(&name, false)),
                        JsonValue::String(xml_scalar_text(value, value_type)?),
                    ]));
                }
            }
            SqlValue::String(format!(
                "{XML_ATTRIBUTES_MARKER}{}",
                JsonValue::Array(attributes)
            ))
        }
        "bicdb_xmlforest" => {
            let mut output = String::new();
            for (pair_index, pair) in args.chunks(2).enumerate() {
                if let [name, value] = pair {
                    if matches!(value, SqlValue::Null) {
                        continue;
                    }
                    let name = map_sql_identifier_to_xml_name(&xml_text(name), false);
                    let value_type = arg_types
                        .and_then(|types| types.get(pair_index * 2 + 1))
                        .and_then(Option::as_deref);
                    output.push_str(&format!(
                        "<{name}>{}</{name}>",
                        xml_content(value, value_type)?
                    ));
                }
            }
            SqlValue::String(output)
        }
        "bicdb_xmlelement" => {
            let Some(first) = args.first() else {
                return Err(SqlError::invalid_parameter_value(
                    "XMLELEMENT requires a name",
                ));
            };
            let name = map_sql_identifier_to_xml_name(&xml_text(first), false);
            let mut attributes = String::new();
            let mut content = String::new();
            for (index, value) in args[1..].iter().enumerate() {
                if matches!(value, SqlValue::Null) {
                    continue;
                }
                let text = xml_text(value);
                if let Some(encoded) = text.strip_prefix(XML_ATTRIBUTES_MARKER) {
                    if let JsonValue::Array(pairs) = serde_json::from_str::<JsonValue>(encoded)? {
                        for pair in pairs {
                            if let JsonValue::Array(pair) = pair {
                                if let [JsonValue::String(key), JsonValue::String(value)] =
                                    pair.as_slice()
                                {
                                    attributes.push_str(&format!(
                                        " {key}=\"{}\"",
                                        escape_attribute(value)
                                    ));
                                }
                            }
                        }
                    }
                } else {
                    let value_type = arg_types
                        .and_then(|types| types.get(index + 1))
                        .and_then(Option::as_deref);
                    content.push_str(&xml_content(value, value_type)?);
                }
            }
            SqlValue::String(if content.is_empty() {
                format!("<{name}{attributes}/>")
            } else {
                format!("<{name}{attributes}>{content}</{name}>")
            })
        }
        "bicdb_xmlpi" => {
            let Some(first) = args.first() else {
                return Err(SqlError::invalid_parameter_value("XMLPI requires a name"));
            };
            let name = map_sql_identifier_to_xml_name(&xml_text(first), false);
            if name.eq_ignore_ascii_case("xml") {
                return Err(SqlError::coded(
                    "2200T",
                    "invalid XML processing instruction",
                    Some("XML processing instruction target name cannot be \"xml\".".into()),
                    None,
                ));
            }
            match args.get(1) {
                Some(SqlValue::Null) => SqlValue::Null,
                Some(value) => {
                    let content = xml_text(value);
                    if content.contains("?>") {
                        return Err(SqlError::coded(
                            "2200T",
                            "invalid XML processing instruction",
                            Some("XML processing instruction cannot contain \"?>\".".into()),
                            None,
                        ));
                    }
                    SqlValue::String(format!("<?{name} {}?>", content.trim_start_matches(' ')))
                }
                None => SqlValue::String(format!("<?{name}?>")),
            }
        }
        "bicdb_xmlroot" => {
            let kind = arg_types
                .and_then(|types| types.first())
                .and_then(Option::as_deref);
            if let Some(kind) = kind.filter(|kind| !matches!(*kind, "unknown" | "xml")) {
                return Err(SqlError::data_exception_public(
                    "42804",
                    format!(
                        "argument of XMLROOT must be type xml, not type {}",
                        crate::common_type_display(kind)
                    ),
                    None,
                ));
            }
            if args
                .first()
                .is_none_or(|value| matches!(value, SqlValue::Null))
            {
                SqlValue::Null
            } else {
                let text = xml_text(&args[0]);
                if kind != Some("xml") {
                    validate_xml(&text, false)?;
                }
                let declaration = xml_declaration(&text);
                let body = declaration.map_or(text.as_str(), |(_, body)| body);
                let version = args
                    .get(1)
                    .filter(|value| !matches!(value, SqlValue::Null))
                    .map(xml_text);
                let standalone = match args.get(2) {
                    Some(SqlValue::Null) => None,
                    Some(value) => Some(xml_text(value)),
                    None => declaration
                        .and_then(|(decl, _)| xml_declaration_attribute(decl, "standalone")),
                };
                if version.as_deref().is_none_or(|version| version == "1.0") && standalone.is_none()
                {
                    SqlValue::String(body.to_owned())
                } else {
                    let version = version.unwrap_or_else(|| "1.0".to_string());
                    let standalone = standalone
                        .map(|value| format!(" standalone=\"{}\"", value.to_ascii_lowercase()))
                        .unwrap_or_default();
                    SqlValue::String(format!("<?xml version=\"{version}\"{standalone}?>{body}"))
                }
            }
        }
        "xmlcomment" => {
            if strict_null {
                SqlValue::Null
            } else {
                let text = xml_text(&args[0]);
                if text.contains("--") || text.ends_with('-') {
                    return Err(SqlError::data_exception_public(
                        "2200S",
                        "invalid XML comment",
                        None,
                    ));
                }
                SqlValue::String(format!("<!--{text}-->"))
            }
        }
        "xmltext" => {
            let [value] = args else {
                return Err(SqlError::undefined_function(
                    "function xmltext requires one argument",
                ));
            };
            let kind = arg_types
                .and_then(|kinds| kinds.first())
                .and_then(Option::as_deref);
            if let Some(kind) = kind.filter(|kind| {
                !matches!(
                    *kind,
                    "text" | "varchar" | "bpchar" | "char" | "name" | "unknown"
                )
            }) {
                return Err(SqlError::coded(
                    "42883",
                    format!("function xmltext({}) does not exist", crate::common_type_display(kind)),
                    None,
                    Some("No function matches the given name and argument types. You might need to add explicit type casts.".into()),
                ));
            }
            if strict_null {
                SqlValue::Null
            } else {
                let text = xml_text(value);
                let text = if kind == Some("bpchar") {
                    text.trim_end_matches(' ')
                } else {
                    &text
                };
                SqlValue::String(escape_attribute(text).replace('\r', "&#13;"))
            }
        }
        "xpath" => {
            if strict_null {
                SqlValue::Null
            } else {
                let namespaces = args.get(2).and_then(|value| match value {
                    SqlValue::Json(value) => Some(&**value),
                    _ => None,
                });
                SqlValue::Json(
                    JsonValue::Array(
                        xpath_results(&xml_text(&args[1]), &xml_text(&args[0]), namespaces)
                            .map_err(|error| {
                                error.with_context_line(
                                    "SQL function \"xpath\" statement 1".to_string(),
                                )
                            })?
                            .into_iter()
                            .map(JsonValue::String)
                            .collect(),
                    )
                    .into(),
                )
            }
        }
        "xpath_exists" | "bicdb_xmlexists" => {
            if strict_null {
                SqlValue::Null
            } else {
                let namespaces = args.get(2).and_then(|value| match value {
                    SqlValue::Json(value) => Some(&**value),
                    _ => None,
                });
                let result = (|| {
                    let xml = xml_text(&args[1]);
                    let expression = xml_text(&args[0]);
                    let (package, xpath) = parse_xpath_input(&xml, &expression)?;
                    let context = xpath_context(namespaces);
                    let result =
                        evaluate_xpath(&xpath, &context, package.as_document().root().into())?;
                    Ok(!matches!(result, XPathValue::Nodeset(nodes) if nodes.size() == 0))
                })()
                .map_err(|error: SqlError| {
                    if name == "xpath_exists" {
                        error.with_context_line(
                            "SQL function \"xpath_exists\" statement 1".to_string(),
                        )
                    } else {
                        error
                    }
                })?;
                SqlValue::Bool(result)
            }
        }
        "bicdb_xmltable_json" => return xml_table_rows(args).map(Some),
        "xml_in" | "bicdb_xmlparse_content" | "bicdb_xmlparse_document" => {
            if strict_null {
                SqlValue::Null
            } else {
                let document = name == "bicdb_xmlparse_document"
                    || (name == "xml_in" && xml_input_is_document());
                SqlValue::String(parse_xml_value(&xml_text(&args[0]), document)?)
            }
        }
        "xml_out" => args.first().cloned().unwrap_or(SqlValue::Null),
        "bicdb_xmlserialize_content" | "bicdb_xmlserialize_document" => {
            return xml_serialize(&name, args, arg_types).map(Some);
        }
        "xml_send" => {
            if strict_null {
                SqlValue::Null
            } else {
                SqlValue::String(crate::format_bytea_hex(xml_text(&args[0]).as_bytes()))
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn map_query_rows(
    result: &crate::SqlResult,
    document_name: &str,
    row_name: &str,
    nulls: bool,
    forest: bool,
    namespace: Option<&str>,
    check_cancellation: impl Fn() -> Result<()>,
) -> Result<String> {
    use std::fmt::Write as _;

    let row_name = map_sql_identifier_to_xml_name(row_name, true);
    let columns = result
        .columns
        .iter()
        .map(|label| map_sql_identifier_to_xml_name(label, true))
        .collect::<Vec<_>>();
    let attributes = xml_mapping_attributes(namespace);
    let mut output = String::new();
    for (index, row) in result.rows.iter().enumerate() {
        if index % 1024 == 0 {
            check_cancellation()?;
        }
        let row_attributes = if forest { attributes.as_str() } else { "" };
        writeln!(output, "<{row_name}{row_attributes}>").unwrap();
        for (index, (value, column)) in row.iter().zip(&columns).enumerate() {
            if matches!(value, SqlValue::Null) {
                if nulls {
                    writeln!(output, "  <{column} xsi:nil=\"true\"/>").unwrap();
                }
            } else {
                let pg_type = result.column_types.get(index).and_then(Option::as_deref);
                let content = xml_content(value, pg_type)?;
                writeln!(output, "  <{column}>{content}</{column}>").unwrap();
            }
        }
        write!(output, "</{row_name}>\n\n").unwrap();
    }
    if forest {
        Ok(output)
    } else {
        Ok(wrap_xml_mapping(&output, document_name, namespace))
    }
}

fn xml_mapping_attributes(namespace: Option<&str>) -> String {
    let Some(namespace) = namespace else {
        return String::new();
    };
    let mut attributes = " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"".to_string();
    if !namespace.is_empty() {
        attributes.push_str(&format!(" xmlns=\"{namespace}\""));
    }
    attributes
}

pub(crate) fn wrap_xml_mapping(
    contents: &str,
    document_name: &str,
    namespace: Option<&str>,
) -> String {
    let document = map_sql_identifier_to_xml_name(document_name, true);
    let attributes = xml_mapping_attributes(namespace);
    format!("<{document}{attributes}>\n\n{contents}</{document}>\n")
}

pub(crate) fn xml_function_pg_type(name: &str) -> Option<&'static str> {
    let name = name
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase();
    match name.as_str() {
        "xml_is_well_formed"
        | "xml_is_well_formed_content"
        | "xml_is_well_formed_document"
        | "xpath_exists"
        | "bicdb_xml_is_document"
        | "bicdb_xml_is_not_document"
        | "bicdb_xmlexists" => Some("bool"),
        "xpath" => Some("xml[]"),
        "bicdb_xmltable_json" => Some("jsonb"),
        "xml_send" => Some("bytea"),
        "xml_out" => Some("cstring"),
        "xmlconcat"
        | "table_to_xml"
        | "query_to_xml"
        | "schema_to_xml"
        | "cursor_to_xml"
        | "database_to_xml"
        | "xmlcomment"
        | "xmltext"
        | "xml_in"
        | "bicdb_xmlattributes"
        | "bicdb_xmlforest"
        | "bicdb_xmlelement"
        | "bicdb_xmlpi"
        | "bicdb_xmlroot"
        | "bicdb_xmlparse_content"
        | "bicdb_xmlparse_document"
        | "bicdb_xmlserialize_content"
        | "bicdb_xmlserialize_document"
        | "xmlagg" => Some("xml"),
        _ => None,
    }
}
