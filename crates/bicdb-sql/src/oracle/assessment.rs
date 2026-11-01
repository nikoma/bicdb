//! Offline lexical inventory. No SQL is executed and no readiness is inferred
//! from the absence of known constructs. Runtime traces and semantic analysis
//! remain separate requirements.
use serde::Serialize;
use sqlparser::{
    dialect::OracleDialect,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

pub const MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_FINDINGS: usize = 20_000;

#[derive(Debug, Serialize)]
pub struct Assessment {
    pub format: u32,
    pub source: String,
    pub bytes: usize,
    pub target_profile: &'static str,
    pub analysis: &'static str,
    pub lexical_scan_complete: bool,
    pub findings_truncated: bool,
    pub migration_ready: bool,
    pub runtime_assessment_required: bool,
    pub objects: Vec<SchemaObject>,
    pub dependencies: Vec<Dependency>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Serialize)]
pub struct SchemaObject {
    pub kind: String,
    pub name: String,
    pub line: u64,
    pub column: u64,
}

#[derive(Debug, Serialize)]
pub struct Dependency {
    pub context: &'static str,
    pub name: String,
    pub line: u64,
    pub column: u64,
}

#[derive(Debug, Serialize)]
pub struct Finding {
    pub id: &'static str,
    pub severity: &'static str,
    pub line: u64,
    pub column: u64,
    pub action: &'static str,
}

struct Rule {
    words: &'static [&'static str],
    id: &'static str,
    blocker: bool,
    action: &'static str,
}

const RULES: &[Rule] = &[
    Rule { words: &["PACKAGE"], id: "plsql-package", blocker: true, action: "Migrate package specifications, bodies and session state; HR v1 does not implement packages." },
    Rule { words: &["AUTONOMOUS_TRANSACTION"], id: "autonomous-transaction", blocker: true, action: "Requires independent transaction and lock semantics; do not replace with the caller transaction." },
    Rule { words: &["EXECUTE", "IMMEDIATE"], id: "dynamic-sql", blocker: true, action: "Capture runtime statements and bind types; static source cannot determine the executed SQL." },
    Rule { words: &["WRAPPED"], id: "wrapped-source", blocker: true, action: "Obtain inspectable source or a supported replacement from the owner." },
    Rule { words: &["CONNECT", "BY"], id: "hierarchical-query", blocker: true, action: "Qualify or migrate Oracle hierarchy, ordering, cycles and pseudocolumn behavior." },
    Rule { words: &["ROWNUM"], id: "rownum", blocker: true, action: "Migrate row-number filtering and ordering semantics; LIMIT is not a general replacement." },
    Rule { words: &["SYNONYM"], id: "synonym", blocker: true, action: "Resolve synonym targets, ownership, grants and cross-schema name resolution." },
    Rule { words: &["TRIGGER"], id: "oracle-trigger", blocker: true, action: "Qualify Oracle trigger timing, row/statement state, privileges and mutation restrictions." },
    Rule { words: &["MERGE"], id: "oracle-merge", blocker: true, action: "Qualify Oracle MERGE matching, duplicate matches, constraints and trigger behavior." },
    Rule { words: &["INSERT", "ALL"], id: "multitable-insert", blocker: true, action: "Implement or migrate multitable insertion with equivalent transactional effects." },
    Rule { words: &["INSERT", "FIRST"], id: "multitable-insert", blocker: true, action: "Implement or migrate conditional first-match multitable insertion." },
    Rule { words: &["ALTER", "SESSION"], id: "session-settings", blocker: true, action: "Inventory NLS, current schema, time zone and other session requirements." },
    Rule { words: &["PARTITION", "BY"], id: "partition-semantics", blocker: false, action: "Distinguish analytic windows from physical partitioning and qualify the applicable behavior." },
    Rule { words: &["MATERIALIZED", "VIEW"], id: "materialized-view", blocker: true, action: "Qualify storage, refresh, refresh logs and transaction consistency." },
    Rule { words: &["ORGANIZATION", "INDEX"], id: "index-organized-storage", blocker: true, action: "Choose and validate an explicit heap-storage migration or implement index-organized behavior." },
    Rule { words: &["BULK", "COLLECT"], id: "bulk-plsql", blocker: true, action: "Qualify bulk collection types, ordering, limits, exceptions and memory bounds." },
    Rule { words: &["FORALL"], id: "bulk-plsql", blocker: true, action: "Qualify bulk binding, SAVE EXCEPTIONS and per-item error/row-count semantics." },
    Rule { words: &["ROWTYPE"], id: "anchored-row-type", blocker: true, action: "Qualify anchored row records, field access and dependency invalidation." },
    Rule { words: &["SYS_REFCURSOR"], id: "reference-cursor", blocker: true, action: "Qualify cursor type, binding, result metadata, fetch and lifetime semantics." },
    Rule { words: &["NVL2"], id: "oracle-nvl2", blocker: true, action: "Implement Oracle evaluation and conversion rules; this function is outside HR v1." },
    Rule { words: &["DECODE"], id: "oracle-decode", blocker: true, action: "Requires Oracle null matching, short-circuiting and result conversions; PostgreSQL decode is a different function." },
    Rule { words: &["NUMBER"], id: "number-semantics", blocker: false, action: "Qualify precision, exponent, scale and conversion boundaries against the source version." },
    Rule { words: &["VARCHAR2"], id: "character-semantics", blocker: false, action: "Qualify BYTE/CHAR lengths, encoding, padding, empty strings and NLS." },
    Rule { words: &["DATE"], id: "date-semantics", blocker: false, action: "Oracle DATE includes time; qualify conversion formats, NLS and date arithmetic." },
    Rule { words: &["CLOB"], id: "lob-semantics", blocker: true, action: "Inventory LOB APIs, locators, streaming, transactions and source-to-target reconciliation." },
    Rule { words: &["BLOB"], id: "lob-semantics", blocker: true, action: "Inventory binary LOB APIs, locators, streaming and reconciliation." },
];

fn keyword(token: &TokenWithSpan, value: &str) -> bool {
    matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
}

fn name_at(tokens: &[TokenWithSpan], start: usize) -> Option<String> {
    let mut parts = Vec::new();
    let mut index = start;
    loop {
        let Token::Word(word) = &tokens.get(index)?.token else {
            return None;
        };
        parts.push(if word.quote_style.is_some() {
            format!("\"{}\"", word.value.replace('"', "\"\""))
        } else {
            word.value.to_uppercase()
        });
        if tokens
            .get(index + 1)
            .is_some_and(|token| token.token == Token::Period)
        {
            index += 2;
        } else {
            return Some(parts.join("."));
        }
    }
}

fn finding(
    token: &TokenWithSpan,
    id: &'static str,
    blocker: bool,
    action: &'static str,
) -> Finding {
    Finding {
        id,
        severity: if blocker { "blocker" } else { "review" },
        line: token.span.start.line,
        column: token.span.start.column,
        action,
    }
}

// Mask whole client lines only at SQL boundaries outside literals, comments
// and procedural units. Keep source positions; never expand or follow scripts.
fn mask_client_lines(source: &str) -> (String, Vec<(u64, u64)>) {
    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Sql,
        Single,
        Double,
        Block,
        Alternative(char),
    }
    let mut mode = Mode::Sql;
    let mut boundary = true;
    let mut procedural = false;
    let mut masked = String::with_capacity(source.len());
    let mut commands = Vec::new();
    for (line_number, line) in source.split_inclusive('\n').enumerate() {
        let text = line.trim_start_matches([' ', '\t']);
        if mode == Mode::Sql && text.trim() == "/" {
            boundary = true;
            procedural = false;
            masked.push_str(line);
            continue;
        }
        if mode == Mode::Sql && boundary && !procedural {
            let words = text
                .split_whitespace()
                .take(7)
                .map(str::to_ascii_uppercase)
                .collect::<Vec<_>>();
            let first = words.first().map(String::as_str).unwrap_or("");
            let client_set = first == "SET"
                && words.get(1).is_some_and(|word| {
                    matches!(
                        word.as_str(),
                        "ECHO"
                            | "FEEDBACK"
                            | "VERIFY"
                            | "HEADING"
                            | "PAGESIZE"
                            | "LINESIZE"
                            | "SERVEROUTPUT"
                            | "TERMOUT"
                            | "DEFINE"
                            | "SQLBLANKLINES"
                    )
                });
            if matches!(
                first,
                "REM"
                    | "REMARK"
                    | "PROMPT"
                    | "SPOOL"
                    | "DEFINE"
                    | "UNDEFINE"
                    | "ACCEPT"
                    | "WHENEVER"
                    | "START"
            ) || first.starts_with('@')
                || client_set
            {
                if !matches!(first, "REM" | "REMARK" | "PROMPT") && commands.len() < MAX_FINDINGS {
                    commands.push((
                        line_number as u64 + 1,
                        line.chars().count() as u64 - text.chars().count() as u64 + 1,
                    ));
                }
                masked.extend(
                    line.chars()
                        .map(|c| if c == '\n' || c == '\r' { c } else { ' ' }),
                );
                continue;
            }
            if matches!(first, "BEGIN" | "DECLARE")
                || (first == "CREATE"
                    && words.iter().skip(1).any(|word| {
                        matches!(
                            word.as_str(),
                            "PACKAGE" | "PROCEDURE" | "FUNCTION" | "TRIGGER" | "TYPE"
                        )
                    }))
            {
                procedural = true;
            }
        }
        let chars = line.chars().collect::<Vec<_>>();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            match mode {
                Mode::Sql if c == '-' && next == Some('-') => break,
                Mode::Sql if c == '/' && next == Some('*') => {
                    mode = Mode::Block;
                    i += 1;
                }
                Mode::Sql
                    if matches!(c, 'q' | 'Q')
                        && next == Some('\'')
                        && chars.get(i + 2).is_some() =>
                {
                    let delimiter = chars[i + 2];
                    mode = Mode::Alternative(match delimiter {
                        '[' => ']',
                        '{' => '}',
                        '(' => ')',
                        '<' => '>',
                        other => other,
                    });
                    boundary = false;
                    i += 2;
                }
                Mode::Sql if c == '\'' => {
                    mode = Mode::Single;
                    boundary = false;
                }
                Mode::Sql if c == '"' => {
                    mode = Mode::Double;
                    boundary = false;
                }
                Mode::Sql if c == ';' => boundary = true,
                Mode::Sql if !c.is_whitespace() => boundary = false,
                Mode::Block if c == '*' && next == Some('/') => {
                    mode = Mode::Sql;
                    i += 1;
                }
                Mode::Single if c == '\'' => {
                    if next == Some('\'') {
                        i += 1;
                    } else {
                        mode = Mode::Sql;
                    }
                }
                Mode::Double if c == '"' => {
                    if next == Some('"') {
                        i += 1;
                    } else {
                        mode = Mode::Sql;
                    }
                }
                Mode::Alternative(end) if c == end && next == Some('\'') => {
                    mode = Mode::Sql;
                    i += 1;
                }
                _ => {}
            }
            i += 1;
        }
        masked.push_str(line);
    }
    (masked, commands)
}

/// Inventory raw SQL/PLSQL text, not SQL*Plus client execution. Comments and
/// literals are tokenized, so feature words inside them are not findings.
/// Findings are conservative lexical leads, not a complete dependency graph.
pub fn assess_source(source_name: &str, source: &str) -> Assessment {
    let mut report = Assessment {
        format: 1,
        source: source_name.to_string(),
        bytes: source.len(),
        target_profile: super::ORACLE_HR_PROFILE,
        analysis: "lexical-inventory-v1",
        lexical_scan_complete: false,
        findings_truncated: false,
        migration_ready: false,
        runtime_assessment_required: true,
        objects: Vec::new(),
        dependencies: Vec::new(),
        findings: Vec::new(),
    };
    if source.len() > MAX_SOURCE_BYTES {
        report.findings.push(Finding {
            id: "source-size-limit",
            severity: "blocker",
            line: 1,
            column: 1,
            action: "Split source into files of at most 16 MiB and assess every file.",
        });
        return report;
    }
    let (masked, commands) = mask_client_lines(source);
    for (line, column) in commands {
        report.findings.push(Finding { id: "client-command", severity: "review", line, column,
            action: "SQL*Plus client line was excluded from SQL inventory; script includes and substitutions are not expanded. Assess all referenced SQL source separately." });
        if report.findings.len() >= MAX_FINDINGS {
            report.findings_truncated = true;
            return report;
        }
    }
    let tokens = match Tokenizer::new(&OracleDialect {}, &masked).tokenize_with_location() {
        Ok(tokens) => tokens
            .into_iter()
            .filter(|t| !matches!(t.token, Token::Whitespace(_)))
            .collect::<Vec<_>>(),
        Err(error) => {
            report.findings.push(Finding { id: "tokenization-failed", severity: "blocker",
                line: error.location.line, column: error.location.column,
                action: "Source could not be tokenized; preprocess client commands or correct syntax, then reassess. No source or literal was included in this diagnostic." });
            return report;
        }
    };
    for (i, token) in tokens.iter().enumerate() {
        if report.findings.len() + report.objects.len() + report.dependencies.len() >= MAX_FINDINGS
        {
            report.findings_truncated = true;
            report.findings.push(finding(
                token,
                "inventory-limit",
                true,
                "Split this source further; the inventory reached its output limit.",
            ));
            return report;
        }
        for rule in RULES {
            if rule
                .words
                .iter()
                .enumerate()
                .all(|(offset, word)| tokens.get(i + offset).is_some_and(|t| keyword(t, word)))
            {
                report
                    .findings
                    .push(finding(token, rule.id, rule.blocker, rule.action));
            }
        }
        if let Token::Word(word) = &token.token {
            if word.quote_style.is_none()
                && (word.value.to_ascii_uppercase().starts_with("DBMS_")
                    || word.value.to_ascii_uppercase().starts_with("UTL_"))
                && tokens.get(i + 1).is_some_and(|t| t.token == Token::Period)
            {
                report.findings.push(finding(token, "supplied-package-api", true,
                    "Inventory the called package APIs, arguments, privileges and external resources; the HR profile does not supply these packages."));
            }
        }
        if keyword(token, "CREATE") {
            let mut j = i + 1;
            if tokens.get(j).is_some_and(|t| keyword(t, "OR"))
                && tokens.get(j + 1).is_some_and(|t| keyword(t, "REPLACE"))
            {
                j += 2;
            }
            while tokens.get(j).is_some_and(|t| {
                [
                    "EDITIONABLE",
                    "NONEDITIONABLE",
                    "FORCE",
                    "GLOBAL",
                    "TEMPORARY",
                    "UNIQUE",
                    "PUBLIC",
                ]
                .iter()
                .any(|word| keyword(t, word))
            }) {
                j += 1;
            }
            let mut kind = String::new();
            if tokens.get(j).is_some_and(|t| keyword(t, "MATERIALIZED")) {
                kind.push_str("MATERIALIZED ");
                j += 1;
            }
            if let Some(kind_token) = tokens.get(j) {
                if [
                    "TABLE",
                    "VIEW",
                    "INDEX",
                    "SEQUENCE",
                    "PROCEDURE",
                    "FUNCTION",
                    "PACKAGE",
                    "TRIGGER",
                    "TYPE",
                    "SYNONYM",
                ]
                .iter()
                .any(|word| keyword(kind_token, word))
                {
                    kind.push_str(&kind_token.token.to_string().to_uppercase());
                    j += 1;
                    if tokens.get(j).is_some_and(|t| keyword(t, "BODY")) {
                        kind.push_str(" BODY");
                        j += 1;
                    }
                    if let Some(name) = name_at(&tokens, j) {
                        if kind == "FUNCTION" {
                            report.findings.push(finding(token, "oracle-stored-function", true,
                                "General Oracle stored functions and their PL/SQL bodies are outside HR v1; inventory and qualify the full routine."));
                        }
                        report.objects.push(SchemaObject {
                            kind,
                            name,
                            line: token.span.start.line,
                            column: token.span.start.column,
                        });
                    }
                }
            }
        }
        for (word, context) in [
            ("REFERENCES", "foreign-key"),
            ("FROM", "from"),
            ("JOIN", "join"),
            ("CALL", "call"),
        ] {
            if keyword(token, word) {
                if let Some(name) = name_at(&tokens, i + 1) {
                    report.dependencies.push(Dependency {
                        context,
                        name,
                        line: token.span.start.line,
                        column: token.span.start.column,
                    });
                }
            }
        }
    }
    report.lexical_scan_complete = true;
    report
        .findings
        .sort_by_key(|finding| (finding.line, finding.column, finding.id));
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_records_quoted_names_dependencies_and_locations() {
        let source = "-- header\nCREATE TABLE hr.\"Mixed\" (id NUMBER PRIMARY KEY, parent NUMBER REFERENCES hr.parent(id));\nCREATE OR REPLACE PACKAGE BODY hr.util IS BEGIN NULL; END;";
        let report = assess_source("schema.sql", source);
        assert!(report.lexical_scan_complete);
        assert_eq!(report.objects[0].name, "HR.\"Mixed\"");
        assert_eq!(report.objects[0].line, 2);
        assert_eq!(report.objects[1].kind, "PACKAGE BODY");
        assert_eq!(report.objects[1].name, "HR.UTIL");
        assert_eq!(report.dependencies[0].name, "HR.PARENT");
        let package = report
            .findings
            .iter()
            .find(|f| f.id == "plsql-package")
            .unwrap();
        assert_eq!(package.line, 3);
        assert_eq!(package.column, 19);
    }

    #[test]
    fn strings_comments_and_quoted_identifiers_do_not_trigger_keyword_rules() {
        let report = assess_source("values.sql", "-- PACKAGE\n/* EXECUTE IMMEDIATE */ SELECT 'AUTONOMOUS_TRANSACTION', 'DBMS_SQL.PARSE', \"ROWNUM\" FROM DUAL");
        assert!(report.lexical_scan_complete);
        assert!(report.findings.is_empty());
        assert!(!report.migration_ready);
        assert!(report.runtime_assessment_required);
    }

    #[test]
    fn client_comments_do_not_become_features_or_change_locations() {
        let report = assess_source("client.sql", "rem owner's permission to merge and publish\nprompt PACKAGE install\nSET ECHO OFF\nCREATE TABLE t (id NUMBER);\nSELECT 'a\nREM PACKAGE is literal content\nb' FROM DUAL;\nBEGIN\nprompt := 'variable';\nEND;\n/\n@other.sql\n");
        assert!(report.lexical_scan_complete);
        assert_eq!(report.objects[0].line, 4);
        assert!(!report
            .findings
            .iter()
            .any(|f| matches!(f.id, "oracle-merge" | "plsql-package")));
        let client_lines = report
            .findings
            .iter()
            .filter(|f| f.id == "client-command")
            .map(|f| f.line)
            .collect::<Vec<_>>();
        assert_eq!(client_lines, vec![3, 12]);
    }

    #[test]
    fn dynamic_and_wrapped_source_are_reported_without_literal_secrets() {
        let report = assess_source("routines.sql", "CREATE PROCEDURE p WRAPPED a000000\nBEGIN EXECUTE IMMEDIATE 'secret literal'; DBMS_SQL.PARSE(); END;");
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("secret literal"));
        for id in ["dynamic-sql", "wrapped-source", "supplied-package-api"] {
            assert!(json.contains(id));
        }
    }

    #[test]
    fn lexical_errors_and_limits_never_look_like_completed_analysis() {
        let report = assess_source("bad.sql", "SELECT 'private unterminated literal");
        assert!(!report.lexical_scan_complete);
        assert!(!serde_json::to_string(&report).unwrap().contains("private"));
        let report = assess_source("large.sql", &" ".repeat(MAX_SOURCE_BYTES + 1));
        assert!(!report.lexical_scan_complete);
        assert_eq!(report.findings[0].id, "source-size-limit");
        let report = assess_source("dense.sql", &"NUMBER ".repeat(MAX_FINDINGS + 1));
        assert!(report.findings_truncated);
        assert!(!report.lexical_scan_complete);
        assert!(!report.migration_ready);
        assert_eq!(report.findings.last().unwrap().id, "inventory-limit");
    }
}
