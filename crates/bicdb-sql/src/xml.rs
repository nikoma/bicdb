use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use sxd_document::{dom, parser};
use sxd_xpath::{nodeset::Node, Context, Factory, Value as XPathValue};

use crate::{Result, SqlError, SqlValue};

fn invalid_xml(detail: impl std::fmt::Display) -> SqlError {
    SqlError::invalid_xml_content(format!("invalid XML content: {detail}"))
}

fn xml_text(value: &SqlValue) -> String {
    value.to_cell()
}

fn strip_declaration(value: &str) -> &str {
    let trimmed = value.trim_start_matches('\u{feff}').trim_start();
    if trimmed.starts_with("<?xml") {
        trimmed
            .find("?>")
            .map(|end| &trimmed[end + 2..])
            .unwrap_or(trimmed)
    } else {
        value
    }
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
    let Some(start) = value.find("<!DOCTYPE") else {
        return Ok(None);
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
    let Some(doctype) = xml_doctype(value)? else {
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
    let prepared = xml_for_parser(value)?;
    let parsed = if document {
        parser::parse(&prepared)
    } else {
        parser::parse(&prepared).or_else(|_| parser::parse(&wrapped_content(&prepared)))
    };
    parsed.map(|_| ()).map_err(invalid_xml)
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
    let value = value.trim_start_matches('\u{feff}');
    let Some(declaration_end) = value
        .starts_with("<?xml")
        .then(|| value.find("?>"))
        .flatten()
    else {
        return Ok(value.to_string());
    };
    let declaration = &value[5..declaration_end];
    let version =
        xml_declaration_attribute(declaration, "version").unwrap_or_else(|| "1.0".to_string());
    let standalone = xml_declaration_attribute(declaration, "standalone");
    let body = &value[declaration_end + 2..];
    if version == "1.0" && standalone.is_none() {
        return Ok(body.to_string());
    }
    let standalone = standalone
        .map(|standalone| format!(" standalone=\"{standalone}\""))
        .unwrap_or_default();
    Ok(format!("<?xml version=\"{version}\"{standalone}?>{body}"))
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

const XML_ATTRIBUTES_MARKER: &str = "\0bicdb:xmlattributes:";

fn xml_content(value: &SqlValue, pg_type: Option<&str>) -> String {
    let text = xml_text(value);
    if pg_type.is_some_and(|pg_type| pg_type == "xml") {
        text
    } else {
        escape_text(&text)
    }
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
    for child in children {
        match child {
            dom::ChildOfElement::Element(child) => serialize_element(child, &namespaces, output),
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
    output.push_str("</");
    output.push_str(&element_name);
    output.push('>');
}

fn xpath_node_xml(node: Node<'_>) -> String {
    match node {
        Node::Element(element) => {
            let mut output = String::new();
            serialize_element(element, &BTreeMap::new(), &mut output);
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
                serialize_element(element, &BTreeMap::new(), &mut output);
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
    let package = parse_xpath_document(xml)?;
    let document = package.as_document();
    check_nesting_budget(expression, &['(', '['], &[')', ']'], "XPath expression")?;
    let xpath = Factory::new().build(expression).map_err(|error| {
        SqlError::invalid_parameter_value(format!("invalid XPath expression: {error}"))
    })?;
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
    let value = xpath.evaluate(&context, document.root()).map_err(|error| {
        SqlError::invalid_parameter_value(format!("XPath evaluation failed: {error}"))
    })?;
    Ok(match value {
        XPathValue::Nodeset(nodes) => nodes
            .document_order()
            .into_iter()
            .map(xpath_node_xml)
            .collect(),
        other => vec![other.string()],
    })
}

fn sql_value_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(value) => JsonValue::Bool(*value),
        SqlValue::Int(value) => JsonValue::Number((*value).into()),
        SqlValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        SqlValue::Json(value) => value.clone(),
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
    let values = entries
        .into_iter()
        .filter_map(|(value, _)| (!matches!(value, SqlValue::Null)).then(|| xml_text(&value)))
        .collect::<Vec<_>>();
    if values.is_empty() {
        Ok(SqlValue::Null)
    } else {
        Ok(SqlValue::String(values.concat()))
    }
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
        "xml_is_well_formed" | "xml_is_well_formed_content" => {
            if strict_null {
                SqlValue::Null
            } else {
                SqlValue::Bool(validate_xml(&xml_text(&args[0]), false).is_ok())
            }
        }
        "xml_is_well_formed_document" => {
            if strict_null {
                SqlValue::Null
            } else {
                SqlValue::Bool(validate_xml(&xml_text(&args[0]), true).is_ok())
            }
        }
        "xmlconcat" => SqlValue::String(
            args.iter()
                .filter(|value| !matches!(value, SqlValue::Null))
                .map(xml_text)
                .collect(),
        ),
        "bicdb_xmlattributes" => {
            let mut attributes = Vec::new();
            for pair in args.chunks(2) {
                if let [name, value] = pair {
                    if matches!(value, SqlValue::Null) {
                        continue;
                    }
                    attributes.push(JsonValue::Array(vec![
                        JsonValue::String(xml_name(&xml_text(name))?.to_string()),
                        JsonValue::String(xml_text(value)),
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
                    let name_text = xml_text(name);
                    let name = xml_name(&name_text)?;
                    let value_type = arg_types
                        .and_then(|types| types.get(pair_index * 2 + 1))
                        .and_then(Option::as_deref);
                    output.push_str(&format!(
                        "<{name}>{}</{name}>",
                        xml_content(value, value_type)
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
            let name_text = xml_text(first);
            let name = xml_name(&name_text)?;
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
                    content.push_str(&xml_content(value, value_type));
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
            let name_text = xml_text(first);
            let name = xml_name(&name_text)?;
            if name.eq_ignore_ascii_case("xml") {
                return Err(invalid_xml(
                    "XML processing instruction target name cannot be xml",
                ));
            }
            let content = args
                .get(1)
                .filter(|value| !matches!(value, SqlValue::Null))
                .map(xml_text);
            if content.as_deref().is_some_and(|value| value.contains("?>")) {
                return Err(invalid_xml("invalid XML processing instruction"));
            }
            SqlValue::String(match content {
                Some(content) => format!("<?{name} {}?>", content.trim()),
                None => format!("<?{name}?>"),
            })
        }
        "bicdb_xmlroot" => {
            if args
                .first()
                .is_none_or(|value| matches!(value, SqlValue::Null))
            {
                SqlValue::Null
            } else {
                let body = strip_declaration(&xml_text(&args[0]))
                    .trim_start()
                    .to_string();
                let version = args
                    .get(1)
                    .filter(|value| !matches!(value, SqlValue::Null))
                    .map(xml_text);
                if version
                    .as_deref()
                    .is_some_and(|version| !matches!(version, "1.0" | "1.1"))
                {
                    return Err(invalid_xml("invalid XML version"));
                }
                let standalone = args
                    .get(2)
                    .filter(|value| !matches!(value, SqlValue::Null))
                    .map(xml_text);
                if version.as_deref().is_none_or(|version| version == "1.0") && standalone.is_none()
                {
                    SqlValue::String(body)
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
                    return Err(invalid_xml("invalid XML comment"));
                }
                SqlValue::String(format!("<!--{text}-->"))
            }
        }
        "xpath" => {
            if strict_null {
                SqlValue::Null
            } else {
                let namespaces = args.get(2).and_then(|value| match value {
                    SqlValue::Json(value) => Some(value),
                    _ => None,
                });
                SqlValue::Json(JsonValue::Array(
                    xpath_results(&xml_text(&args[1]), &xml_text(&args[0]), namespaces)?
                        .into_iter()
                        .map(JsonValue::String)
                        .collect(),
                ))
            }
        }
        "xpath_exists" | "bicdb_xmlexists" => {
            if strict_null {
                SqlValue::Null
            } else {
                let namespaces = args.get(2).and_then(|value| match value {
                    SqlValue::Json(value) => Some(value),
                    _ => None,
                });
                let package = parse_xpath_document(&xml_text(&args[1]))?;
                let document = package.as_document();
                let expression_text = xml_text(&args[0]);
                check_nesting_budget(
                    &expression_text,
                    &['(', '['],
                    &[')', ']'],
                    "XPath expression",
                )?;
                let xpath = Factory::new().build(&expression_text).map_err(|error| {
                    SqlError::invalid_parameter_value(format!("invalid XPath expression: {error}"))
                })?;
                let mut context = Context::new();
                if let Some(JsonValue::Array(entries)) = namespaces {
                    for entry in entries {
                        if let Some(pair) = entry.as_array() {
                            if let [JsonValue::String(prefix), JsonValue::String(uri)] =
                                pair.as_slice()
                            {
                                context.set_namespace(prefix, uri);
                            }
                        }
                    }
                }
                SqlValue::Bool(
                    xpath
                        .evaluate(&context, document.root())
                        .map_err(|error| {
                            SqlError::invalid_parameter_value(format!(
                                "XPath evaluation failed: {error}"
                            ))
                        })?
                        .boolean(),
                )
            }
        }
        "bicdb_xmltable_json" => {
            let required_null = args.get(..4).is_none_or(|required| {
                required.iter().any(|value| matches!(value, SqlValue::Null))
            });
            if required_null {
                SqlValue::Null
            } else {
                let document = xml_text(&args[0]);
                let row_path = xml_text(&args[1]);
                let specs = match &args[2] {
                    SqlValue::Json(value) => value.as_array().cloned().unwrap_or_default(),
                    value => serde_json::from_str::<JsonValue>(&value.to_cell())?
                        .as_array()
                        .cloned()
                        .unwrap_or_default(),
                };
                let namespaces = args.get(3).and_then(|value| match value {
                    SqlValue::Json(value) => Some(value),
                    _ => None,
                });
                let rows = xpath_results(&document, &row_path, namespaces)?;
                let mut output = Vec::with_capacity(rows.len());
                for (ordinality, row_xml) in rows.into_iter().enumerate() {
                    let mut object = serde_json::Map::new();
                    for spec in &specs {
                        let Some(name) = spec.get("name").and_then(JsonValue::as_str) else {
                            continue;
                        };
                        if spec.get("ordinality").and_then(JsonValue::as_bool) == Some(true) {
                            object.insert(
                                name.to_string(),
                                JsonValue::Number((ordinality + 1).into()),
                            );
                            continue;
                        }
                        let path_arg = spec
                            .get("path_arg")
                            .and_then(JsonValue::as_u64)
                            .unwrap_or(0) as usize;
                        let path = args
                            .get(path_arg)
                            .map(xml_text)
                            .unwrap_or_else(|| name.to_string());
                        let relative_path = if path.starts_with('/') {
                            path
                        } else {
                            format!("/*/{path}")
                        };
                        let raw_values = xpath_results(&row_xml, &relative_path, namespaces)?;
                        let values = if raw_values.is_empty()
                            || spec.get("xml").and_then(JsonValue::as_bool) == Some(true)
                        {
                            raw_values
                        } else {
                            xpath_results(
                                &row_xml,
                                &format!("string({relative_path})"),
                                namespaces,
                            )?
                        };
                        if values.len() > 1 {
                            return Err(SqlError::data_exception_public(
                                "21000",
                                "more than one value returned by XMLTABLE column expression",
                                None,
                            ));
                        }
                        let value = values
                            .first()
                            .map(|value| JsonValue::String(value.clone()))
                            .or_else(|| {
                                spec.get("default_arg")
                                    .and_then(JsonValue::as_u64)
                                    .and_then(|index| args.get(index as usize))
                                    .map(sql_value_json)
                            })
                            .unwrap_or(JsonValue::Null);
                        object.insert(name.to_string(), value);
                    }
                    output.push(JsonValue::Object(object));
                }
                SqlValue::Json(JsonValue::Array(output))
            }
        }
        "xml_in" | "bicdb_xmlparse_content" | "bicdb_xmlparse_document" => {
            if strict_null {
                SqlValue::Null
            } else {
                let document = name == "bicdb_xmlparse_document";
                let text = xml_text(&args[0]);
                validate_xml(&text, document)?;
                SqlValue::String(text)
            }
        }
        "xml_out" | "bicdb_xmlserialize_content" | "bicdb_xmlserialize_document" => {
            args.first().cloned().unwrap_or(SqlValue::Null)
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
        | "bicdb_xmlexists" => Some("bool"),
        "xpath" => Some("xml[]"),
        "bicdb_xmltable_json" => Some("jsonb"),
        "xml_send" => Some("bytea"),
        "xml_out" => Some("cstring"),
        "xmlconcat"
        | "xmlcomment"
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
