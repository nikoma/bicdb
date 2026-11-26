//! PostgreSQL's EXPLAIN option list and output formats (TEXT, XML, JSON,
//! YAML): `ParseExplainOptionList` and `explain_format.c`. The plan itself
//! is built by `crate::engine::explain_pg` from what BicDB executes.

use sqlparser::ast::{Expr, UtilityOption, Value};

use crate::{Result, SqlError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Format {
    Text,
    Xml,
    Json,
    Yaml,
}

#[derive(Clone, Debug)]
pub(crate) struct ExplainOptions {
    pub(crate) analyze: bool,
    pub(crate) verbose: bool,
    pub(crate) costs: bool,
    pub(crate) buffers: bool,
    pub(crate) wal: bool,
    pub(crate) settings: bool,
    pub(crate) generic_plan: bool,
    pub(crate) timing: bool,
    pub(crate) summary: bool,
    pub(crate) memory: bool,
    pub(crate) serialize: bool,
    pub(crate) format: Format,
}

fn option_error(sqlstate: &'static str, message: String) -> SqlError {
    SqlError::data_exception_public(sqlstate, message, None)
}

/// The text of an option argument: a keyword, a string or a number.
fn argument_text(argument: &Expr) -> String {
    match argument {
        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => text.clone(),
            Value::Number(number, _) => number.clone(),
            Value::Boolean(flag) => flag.to_string(),
            other => other.to_string(),
        },
        other => other.to_string().to_ascii_lowercase(),
    }
}

/// `defGetBoolean`.
fn boolean_option(option: &UtilityOption) -> Result<bool> {
    let Some(argument) = &option.arg else {
        return Ok(true);
    };
    let text = argument_text(argument);
    match text.to_ascii_lowercase().as_str() {
        "true" | "on" | "1" => Ok(true),
        "false" | "off" | "0" => Ok(false),
        _ => Err(option_error(
            "42601",
            format!(
                "{} requires a Boolean value",
                option.name.value.to_ascii_lowercase()
            ),
        )),
    }
}

impl ExplainOptions {
    /// The options of `EXPLAIN [ANALYZE] [VERBOSE]` without a list.
    pub(crate) fn legacy(analyze: bool, verbose: bool) -> Self {
        Self {
            analyze,
            verbose,
            costs: true,
            buffers: analyze,
            wal: false,
            settings: false,
            generic_plan: false,
            timing: analyze,
            summary: analyze,
            memory: false,
            serialize: false,
            format: Format::Text,
        }
    }

    /// `ParseExplainOptionList`.
    pub(crate) fn parse(options: &[UtilityOption]) -> Result<Self> {
        let mut parsed = Self::legacy(false, false);
        let (mut timing, mut buffers, mut summary) = (None, None, None);
        for option in options {
            let name = option.name.value.to_ascii_lowercase();
            match name.as_str() {
                "analyze" => parsed.analyze = boolean_option(option)?,
                "verbose" => parsed.verbose = boolean_option(option)?,
                "costs" => parsed.costs = boolean_option(option)?,
                "buffers" => buffers = Some(boolean_option(option)?),
                "wal" => parsed.wal = boolean_option(option)?,
                "settings" => parsed.settings = boolean_option(option)?,
                "generic_plan" => parsed.generic_plan = boolean_option(option)?,
                "timing" => timing = Some(boolean_option(option)?),
                "summary" => summary = Some(boolean_option(option)?),
                "memory" => parsed.memory = boolean_option(option)?,
                "serialize" => {
                    parsed.serialize = match option.arg.as_ref().map(argument_text) {
                        None => true,
                        Some(value) => match value.as_str() {
                            "off" | "none" => false,
                            "text" | "binary" => true,
                            _ => {
                                return Err(option_error(
                                    "22023",
                                    format!(
                                "unrecognized value for EXPLAIN option \"serialize\": \"{value}\""
                            ),
                                ))
                            }
                        },
                    }
                }
                "format" => {
                    let value = option.arg.as_ref().map(argument_text).unwrap_or_default();
                    parsed.format = match value.as_str() {
                        "text" => Format::Text,
                        "xml" => Format::Xml,
                        "json" => Format::Json,
                        "yaml" => Format::Yaml,
                        _ => {
                            return Err(option_error(
                                "22023",
                                format!(
                                    "unrecognized value for EXPLAIN option \"format\": \"{value}\""
                                ),
                            ))
                        }
                    }
                }
                _ => {
                    return Err(option_error(
                        "42601",
                        format!("unrecognized EXPLAIN option \"{name}\""),
                    ))
                }
            }
        }
        if parsed.wal && !parsed.analyze {
            return Err(option_error(
                "22023",
                "EXPLAIN option WAL requires ANALYZE".to_string(),
            ));
        }
        parsed.timing = timing.unwrap_or(parsed.analyze);
        parsed.buffers = buffers.unwrap_or(parsed.analyze);
        if parsed.timing && !parsed.analyze {
            return Err(option_error(
                "22023",
                "EXPLAIN option TIMING requires ANALYZE".to_string(),
            ));
        }
        if parsed.serialize && !parsed.analyze {
            return Err(option_error(
                "22023",
                "EXPLAIN option SERIALIZE requires ANALYZE".to_string(),
            ));
        }
        if parsed.generic_plan && parsed.analyze {
            return Err(option_error(
                "22023",
                "EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together".to_string(),
            ));
        }
        parsed.summary = summary.unwrap_or(parsed.analyze);
        Ok(parsed)
    }
}

/// A node property value.
#[derive(Clone, Debug)]
pub(crate) enum Prop {
    Text(String),
    /// Rendered unquoted in JSON/YAML.
    Number(String),
    Bool(bool),
    List(Vec<String>),
}

/// One plan node: PostgreSQL's node name, the text-format header details,
/// and its properties in output order.
#[derive(Clone, Debug, Default)]
pub(crate) struct PlanNode {
    /// `Node Type` (`Seq Scan`, `Hash Join`, `Aggregate`, ...).
    pub(crate) node_type: String,
    /// Text-format name when it differs from the node type
    /// (`HashAggregate`, `Hash Left Join`, `Index Scan Backward`).
    pub(crate) text_name: Option<String>,
    /// Non-text-format properties emitted before the costs
    /// (`Strategy`, `Operation`, `Join Type`, `Relation Name`, `Alias`, ...).
    pub(crate) head: Vec<(&'static str, Prop)>,
    /// The text-format suffix after the name: ` using idx on t alias`.
    pub(crate) target: String,
    /// Planner estimates (startup cost, total cost, rows, width).
    pub(crate) costs: Option<(f64, f64, f64, i64)>,
    /// Actual rows and loops (EXPLAIN ANALYZE).
    pub(crate) actual: Option<(f64, f64)>,
    pub(crate) props: Vec<(&'static str, Prop)>,
    /// The session disabled this node's plan type (`SET enable_seqscan =
    /// off`) and the node runs anyway: PostgreSQL's `Disabled: true`.
    pub(crate) disabled: bool,
    /// Child plans with their parent relationship (`Outer`, `Inner`,
    /// `Member`, `Subquery`, `InitPlan`, `SubPlan`) and subplan name.
    pub(crate) children: Vec<(&'static str, Option<String>, PlanNode)>,
}

impl PlanNode {
    pub(crate) fn new(node_type: &str) -> Self {
        Self {
            node_type: node_type.to_string(),
            ..Self::default()
        }
    }

    pub(crate) fn text_name(mut self, name: impl Into<String>) -> Self {
        self.text_name = Some(name.into());
        self
    }

    pub(crate) fn head(mut self, label: &'static str, value: Prop) -> Self {
        self.head.push((label, value));
        self
    }

    pub(crate) fn prop(mut self, label: &'static str, value: impl Into<String>) -> Self {
        self.props.push((label, Prop::Text(value.into())));
        self
    }

    pub(crate) fn prop_value(mut self, label: &'static str, value: Prop) -> Self {
        self.props.push((label, value));
        self
    }

    pub(crate) fn child(mut self, relationship: &'static str, child: PlanNode) -> Self {
        self.children.push((relationship, None, child));
        self
    }

    pub(crate) fn subplan(
        mut self,
        relationship: &'static str,
        name: String,
        child: PlanNode,
    ) -> Self {
        self.children.push((relationship, Some(name), child));
        self
    }
}

/// `escape_xml`.
fn escape_xml(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#x0d;"),
            _ => out.push(ch),
        }
    }
    out
}

fn escape_json(text: &str) -> String {
    serde_json::to_string(text).expect("string serialization is infallible")
}

/// `ExplainXMLTag`'s tag-name sanitizing.
fn xml_tag(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

/// PostgreSQL's ExplainState output machinery.
struct Writer<'a> {
    options: &'a ExplainOptions,
    out: String,
    indent: usize,
    grouping: Vec<bool>,
}

impl Writer<'_> {
    fn spaces(&mut self, count: usize) {
        self.out.extend(std::iter::repeat_n(' ', count));
    }

    fn indent_text(&mut self) {
        if self.out.is_empty() || self.out.ends_with('\n') {
            self.spaces(self.indent * 2);
        }
    }

    fn json_line_ending(&mut self) {
        let top = self.grouping.last_mut().expect("grouping stack");
        if *top {
            self.out.push(',');
        } else {
            *top = true;
        }
        self.out.push('\n');
    }

    fn yaml_line_starting(&mut self) {
        let top = self.grouping.last_mut().expect("grouping stack");
        if !*top {
            *top = true;
        } else {
            self.out.push('\n');
            let indent = self.indent * 2;
            self.spaces(indent);
        }
    }

    fn xml_tag(&mut self, name: &str, closing: bool, whitespace: bool) {
        if whitespace {
            let indent = self.indent * 2;
            self.spaces(indent);
        }
        self.out.push('<');
        if closing {
            self.out.push('/');
        }
        self.out.push_str(&xml_tag(name));
        self.out.push('>');
        if whitespace {
            self.out.push('\n');
        }
    }

    fn property(&mut self, label: &str, value: &Prop) {
        if let Prop::List(items) = value {
            return self.property_list(label, items);
        }
        let (text, numeric) = match value {
            Prop::Text(text) => (text.clone(), false),
            Prop::Number(text) => (text.clone(), true),
            Prop::Bool(flag) => (flag.to_string(), true),
            Prop::List(_) => unreachable!(),
        };
        match self.options.format {
            Format::Text => {
                self.indent_text();
                self.out.push_str(&format!("{label}: {text}\n"));
            }
            Format::Xml => {
                let indent = self.indent * 2;
                self.spaces(indent);
                self.xml_tag(label, false, false);
                self.out.push_str(&escape_xml(&text));
                self.xml_tag(label, true, false);
                self.out.push('\n');
            }
            Format::Json => {
                self.json_line_ending();
                let indent = self.indent * 2;
                self.spaces(indent);
                self.out.push_str(&escape_json(label));
                self.out.push_str(": ");
                self.out
                    .push_str(&if numeric { text } else { escape_json(&text) });
            }
            Format::Yaml => {
                self.yaml_line_starting();
                self.out.push_str(&format!("{label}: "));
                self.out
                    .push_str(&if numeric { text } else { escape_json(&text) });
            }
        }
    }

    fn property_list(&mut self, label: &str, items: &[String]) {
        match self.options.format {
            Format::Text => {
                self.indent_text();
                self.out
                    .push_str(&format!("{label}: {}\n", items.join(", ")));
            }
            Format::Xml => {
                self.xml_tag(label, false, true);
                for item in items {
                    let indent = self.indent * 2 + 2;
                    self.spaces(indent);
                    self.out
                        .push_str(&format!("<Item>{}</Item>\n", escape_xml(item)));
                }
                self.xml_tag(label, true, true);
            }
            Format::Json => {
                self.json_line_ending();
                let indent = self.indent * 2;
                self.spaces(indent);
                self.out.push_str(&escape_json(label));
                self.out.push_str(": [");
                self.out.push_str(
                    &items
                        .iter()
                        .map(|item| escape_json(item))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                self.out.push(']');
            }
            Format::Yaml => {
                self.yaml_line_starting();
                self.out.push_str(&format!("{label}: "));
                for item in items {
                    self.out.push('\n');
                    let indent = self.indent * 2 + 2;
                    self.spaces(indent);
                    self.out.push_str("- ");
                    self.out.push_str(&escape_json(item));
                }
            }
        }
    }

    fn open_group(&mut self, objtype: &str, label: Option<&str>, labeled: bool) {
        match self.options.format {
            Format::Text => {}
            Format::Xml => {
                self.xml_tag(objtype, false, true);
                self.indent += 1;
            }
            Format::Json => {
                self.json_line_ending();
                let indent = self.indent * 2;
                self.spaces(indent);
                if let Some(label) = label {
                    self.out.push_str(&escape_json(label));
                    self.out.push_str(": ");
                }
                self.out.push(if labeled { '{' } else { '[' });
                self.grouping.push(false);
                self.indent += 1;
            }
            Format::Yaml => {
                self.yaml_line_starting();
                if let Some(label) = label {
                    self.out.push_str(&format!("{label}: "));
                    self.grouping.push(true);
                } else {
                    self.out.push_str("- ");
                    self.grouping.push(false);
                }
                self.indent += 1;
            }
        }
    }

    fn close_group(&mut self, objtype: &str, labeled: bool) {
        match self.options.format {
            Format::Text => {}
            Format::Xml => {
                self.indent -= 1;
                self.xml_tag(objtype, true, true);
            }
            Format::Json => {
                self.indent -= 1;
                self.out.push('\n');
                let indent = self.indent * 2;
                self.spaces(indent);
                self.out.push(if labeled { '}' } else { ']' });
                self.grouping.pop();
            }
            Format::Yaml => {
                self.indent -= 1;
                self.grouping.pop();
            }
        }
    }

    /// `ExplainNode`'s output of one node and its children.
    fn node(&mut self, node: &PlanNode, relationship: Option<&str>, plan_name: Option<&str>) {
        let save_indent = self.indent;
        self.open_group(
            "Plan",
            if relationship.is_some() {
                None
            } else {
                Some("Plan")
            },
            true,
        );
        let text = self.options.format == Format::Text;
        if text {
            if let Some(plan_name) = plan_name {
                self.indent_text();
                self.out.push_str(plan_name);
                self.out.push('\n');
                self.indent += 1;
            }
            if self.indent > 0 {
                self.indent_text();
                self.out.push_str("->  ");
                self.indent += 2;
            }
            self.out
                .push_str(node.text_name.as_deref().unwrap_or(&node.node_type));
            self.out.push_str(&node.target);
            self.indent += 1;
        } else {
            self.property("Node Type", &Prop::Text(node.node_type.clone()));
            for (label, value) in node
                .head
                .iter()
                .filter(|(label, _)| matches!(*label, "Strategy" | "Partial Mode" | "Operation"))
            {
                self.property(label, value);
            }
            if let Some(relationship) = relationship {
                self.property("Parent Relationship", &Prop::Text(relationship.to_string()));
            }
            if let Some(plan_name) = plan_name {
                self.property("Subplan Name", &Prop::Text(plan_name.to_string()));
            }
            self.property("Parallel Aware", &Prop::Bool(false));
            self.property("Async Capable", &Prop::Bool(false));
            for (label, value) in node
                .head
                .iter()
                .filter(|(label, _)| !matches!(*label, "Strategy" | "Partial Mode" | "Operation"))
            {
                self.property(label, value);
            }
        }
        if self.options.costs {
            if let Some((startup, total, rows, width)) = node.costs {
                if text {
                    self.out.push_str(&format!(
                        "  (cost={startup:.2}..{total:.2} rows={rows:.0} width={width})"
                    ));
                } else {
                    self.property("Startup Cost", &Prop::Number(format!("{startup:.2}")));
                    self.property("Total Cost", &Prop::Number(format!("{total:.2}")));
                    self.property("Plan Rows", &Prop::Number(format!("{rows:.0}")));
                    self.property("Plan Width", &Prop::Number(width.to_string()));
                }
            }
        }
        if self.options.analyze {
            let (rows, loops) = node.actual.unwrap_or((0.0, 1.0));
            if text {
                if node.actual.is_none() {
                    self.out.push_str(" (never executed)");
                } else {
                    self.out
                        .push_str(&format!(" (actual rows={rows:.2} loops={loops:.0})"));
                }
            } else {
                self.property("Actual Rows", &Prop::Number(format!("{rows:.2}")));
                self.property("Actual Loops", &Prop::Number(format!("{loops:.0}")));
            }
        }
        if text {
            self.out.push('\n');
            if node.disabled {
                self.property("Disabled", &Prop::Bool(true));
            }
        } else {
            self.property("Disabled", &Prop::Bool(node.disabled));
        }
        for (label, value) in &node.props {
            self.property(label, value);
        }
        if !node.children.is_empty() {
            self.open_group("Plans", Some("Plans"), false);
            for (relationship, name, child) in &node.children {
                self.node(child, Some(relationship), name.as_deref());
            }
            self.close_group("Plans", false);
        }
        if text {
            self.indent = save_indent;
        }
        self.close_group("Plan", true);
    }
}

/// The output rows of EXPLAIN: one row per text line, or one row holding
/// the whole XML/JSON/YAML document.
pub(crate) fn render(
    options: &ExplainOptions,
    plan: &PlanNode,
    trailer: &[(&'static str, Prop)],
) -> Vec<String> {
    let mut writer = Writer {
        options,
        out: String::new(),
        indent: 0,
        grouping: Vec::new(),
    };
    match options.format {
        Format::Text => {}
        Format::Xml => {
            writer
                .out
                .push_str("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
            writer.indent += 1;
        }
        Format::Json => {
            writer.out.push('[');
            writer.grouping.push(false);
            writer.indent += 1;
        }
        Format::Yaml => writer.grouping.push(false),
    }
    writer.open_group("Query", None, true);
    writer.node(plan, None, None);
    for (label, value) in trailer {
        writer.property(label, value);
    }
    writer.close_group("Query", true);
    match options.format {
        Format::Text => {}
        Format::Xml => {
            writer.indent -= 1;
            writer.out.push_str("</explain>");
        }
        Format::Json => {
            writer.indent -= 1;
            writer.out.push_str("\n]");
            writer.grouping.pop();
        }
        Format::Yaml => {
            writer.grouping.pop();
        }
    }
    if options.format == Format::Text {
        writer
            .out
            .trim_end_matches('\n')
            .split('\n')
            .map(str::to_string)
            .collect()
    } else {
        vec![writer.out]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan() -> PlanNode {
        PlanNode::new("Seq Scan")
            .head("Relation Name", Prop::Text("t".into()))
            .head("Alias", Prop::Text("t".into()))
            .prop("Filter", "(a = 1)")
    }

    fn options(format: Format) -> ExplainOptions {
        ExplainOptions {
            costs: false,
            format,
            ..ExplainOptions::legacy(false, false)
        }
    }

    #[test]
    fn text_format_indents_children_as_postgres() {
        let mut join = PlanNode::new("Hash Join").prop("Hash Cond", "(t1.a = t2.a)");
        join.target = String::new();
        let mut left = scan();
        left.target = " on t1".into();
        let mut right = scan();
        right.target = " on t2".into();
        let plan = join
            .child("Outer", left)
            .child("Inner", PlanNode::new("Hash").child("Outer", right));
        assert_eq!(
            render(&options(Format::Text), &plan, &[]),
            vec![
                "Hash Join",
                "  Hash Cond: (t1.a = t2.a)",
                "  ->  Seq Scan on t1",
                "        Filter: (a = 1)",
                "  ->  Hash",
                "        ->  Seq Scan on t2",
                "              Filter: (a = 1)",
            ]
        );
    }

    #[test]
    fn structured_formats_follow_explain_format_c() {
        let json = render(&options(Format::Json), &scan(), &[]);
        assert_eq!(
            json[0],
            "[\n  {\n    \"Plan\": {\n      \"Node Type\": \"Seq Scan\",\n      \"Parallel Aware\": false,\n      \"Async Capable\": false,\n      \"Relation Name\": \"t\",\n      \"Alias\": \"t\",\n      \"Disabled\": false,\n      \"Filter\": \"(a = 1)\"\n    }\n  }\n]"
        );
        let yaml = render(&options(Format::Yaml), &scan(), &[]);
        assert_eq!(
            yaml[0],
            "- Plan: \n    Node Type: \"Seq Scan\"\n    Parallel Aware: false\n    Async Capable: false\n    Relation Name: \"t\"\n    Alias: \"t\"\n    Disabled: false\n    Filter: \"(a = 1)\""
        );
        let xml = render(&options(Format::Xml), &scan(), &[]);
        assert!(xml[0].starts_with("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n  <Query>\n    <Plan>\n      <Node-Type>Seq Scan</Node-Type>"));
    }

    #[test]
    fn options_follow_parse_explain_option_list() {
        let parse = |sql: &str| {
            let statement = sqlparser::parser::Parser::parse_sql(
                &sqlparser::dialect::PostgreSqlDialect {},
                sql,
            )
            .unwrap()
            .remove(0);
            let sqlparser::ast::Statement::Explain { options, .. } = statement else {
                panic!("not explain");
            };
            ExplainOptions::parse(&options.unwrap_or_default())
        };
        let options =
            parse("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, FORMAT JSON) SELECT 1").unwrap();
        assert!(options.analyze && !options.costs && !options.timing && options.buffers);
        assert_eq!(options.format, Format::Json);
        for (sql, message) in [
            (
                "EXPLAIN (BOGUS) SELECT 1",
                "unrecognized EXPLAIN option \"bogus\"",
            ),
            (
                "EXPLAIN (FORMAT CSV) SELECT 1",
                "unrecognized value for EXPLAIN option \"format\": \"csv\"",
            ),
            (
                "EXPLAIN (TIMING) SELECT 1",
                "EXPLAIN option TIMING requires ANALYZE",
            ),
            (
                "EXPLAIN (ANALYZE, GENERIC_PLAN) SELECT 1",
                "cannot be used together",
            ),
        ] {
            let error = parse(sql).unwrap_err();
            assert!(error.to_string().contains(message), "{sql}: {error}");
        }
    }
}
