//! Parsed-statement cache keyed on a literal-normalized template.
//!
//! Every top-level statement used to be rewritten by ~20 compatibility passes
//! and parsed from text on every execution; for a TPC-C `CALL neword(1, 16,
//! 5, …)` that was ~6% of server CPU spent re-deriving an AST whose only
//! variable parts are its literals. A linear scan lifts plain string and
//! number literals into `$n` placeholders, the template is parsed once per
//! session, and each execution clones the cached AST and binds the literals
//! back in, producing exactly the tree the parser would have built from the
//! original text.
//!
//! The lexer is deliberately conservative: it only lifts a literal that
//! directly follows `(`, `,` or a binary operator, so typed strings
//! (`DATE '2020-01-01'`), escape strings (`E'\n'`), `LIMIT 10` and anything
//! after a keyword stay inline, and any text containing `$` (dollar quotes or
//! existing placeholders) or `?` is left to the ordinary parser.

use crate::*;
use sqlparser::ast::visit_expressions_mut;
use std::ops::ControlFlow;

/// A literal lifted out of the statement text, restored into the AST at bind
/// time as the same `Value` the parser would have produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BoundLiteral {
    /// `'text'` with doubled quotes already unescaped.
    Str(String),
    /// The literal's source text, e.g. `12` or `1.5e3`; sqlparser keeps
    /// numbers as text.
    Num(String),
}

/// Statements whose literals are worth lifting; matches the raw-probe gate.
pub(crate) fn statement_is_cacheable(sql: &str) -> bool {
    leading_keyword_bypasses_raw_probes(sql)
}

const MAX_LIFTED_LITERALS: usize = 64;
pub(crate) const MAX_CACHED_TEMPLATES: usize = 256;

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Bytes after which a literal is in expression position.
fn opens_literal_position(prev: Option<u8>) -> bool {
    matches!(
        prev,
        Some(
            b'(' | b','
                | b'='
                | b'<'
                | b'>'
                | b'+'
                | b'-'
                | b'*'
                | b'/'
                | b'%'
                | b'|'
                | b'&'
                | b'!'
                | b'~'
                | b'['
        )
    )
}

/// Lift plain literals out of `sql`. Returns `None` when the text has no
/// liftable literal or contains constructs the lexer does not model.
pub(crate) fn templatize(sql: &str) -> Option<(String, Vec<BoundLiteral>)> {
    let bytes = sql.as_bytes();
    let mut template = String::with_capacity(sql.len());
    let mut literals = Vec::new();
    let mut idx = 0;
    // Last significant (non-whitespace) byte copied to the template.
    let mut prev: Option<u8> = None;
    while idx < bytes.len() {
        let b = bytes[idx];
        match b {
            b'$' | b'?' => return None,
            b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                let end = bytes[idx..]
                    .iter()
                    .position(|&c| c == b'\n')
                    .map_or(bytes.len(), |p| idx + p);
                template.push_str(&sql[idx..end]);
                idx = end;
            }
            b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                let end = sql[idx + 2..].find("*/").map(|p| idx + 2 + p + 2)?;
                template.push_str(&sql[idx..end]);
                idx = end;
            }
            b'"' => {
                let mut end = idx + 1;
                loop {
                    match bytes.get(end) {
                        None => return None,
                        Some(b'"') if bytes.get(end + 1) == Some(&b'"') => end += 2,
                        Some(b'"') => {
                            end += 1;
                            break;
                        }
                        Some(_) => end += 1,
                    }
                }
                template.push_str(&sql[idx..end]);
                prev = Some(b'"');
                idx = end;
            }
            b'\'' => {
                let mut end = idx + 1;
                let mut doubled = false;
                loop {
                    match bytes.get(end) {
                        None => return None,
                        Some(b'\'') if bytes.get(end + 1) == Some(&b'\'') => {
                            doubled = true;
                            end += 2;
                        }
                        Some(b'\'') => {
                            end += 1;
                            break;
                        }
                        Some(_) => end += 1,
                    }
                }
                let liftable = opens_literal_position(prev)
                    && !prev.is_some_and(is_ident_byte)
                    && literals.len() < MAX_LIFTED_LITERALS;
                if liftable {
                    let inner = &sql[idx + 1..end - 1];
                    let text = if doubled {
                        inner.replace("''", "'")
                    } else {
                        inner.to_string()
                    };
                    literals.push(BoundLiteral::Str(text));
                    template.push('$');
                    template.push_str(&literals.len().to_string());
                } else {
                    template.push_str(&sql[idx..end]);
                }
                prev = Some(b'\'');
                idx = end;
            }
            b'0'..=b'9' if !prev.is_some_and(|p| is_ident_byte(p) || p == b'.') => {
                let start = idx;
                let mut end = idx;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if bytes.get(end) == Some(&b'.') {
                    end += 1;
                    while end < bytes.len() && bytes[end].is_ascii_digit() {
                        end += 1;
                    }
                }
                if matches!(bytes.get(end), Some(b'e' | b'E')) {
                    let mut exp = end + 1;
                    if matches!(bytes.get(exp), Some(b'+' | b'-')) {
                        exp += 1;
                    }
                    if bytes.get(exp).is_some_and(u8::is_ascii_digit) {
                        end = exp;
                        while end < bytes.len() && bytes[end].is_ascii_digit() {
                            end += 1;
                        }
                    }
                }
                let followed_by_ident = bytes
                    .get(end)
                    .is_some_and(|&c| is_ident_byte(c) || c == b'.');
                let liftable = opens_literal_position(prev)
                    && !followed_by_ident
                    && literals.len() < MAX_LIFTED_LITERALS;
                if liftable {
                    literals.push(BoundLiteral::Num(sql[start..end].to_string()));
                    template.push('$');
                    template.push_str(&literals.len().to_string());
                } else {
                    template.push_str(&sql[start..end]);
                }
                prev = Some(b'0');
                idx = end;
            }
            _ => {
                template.push(b as char);
                if !b.is_ascii_whitespace() {
                    prev = Some(b);
                }
                idx += 1;
                // Copy the rest of a multi-byte UTF-8 sequence verbatim.
                while idx < bytes.len() && (bytes[idx] & 0xC0) == 0x80 {
                    template.push(bytes[idx] as char);
                    idx += 1;
                }
            }
        }
    }
    if literals.is_empty() {
        return None;
    }
    // `template.push(b as char)` on raw bytes would mangle multi-byte UTF-8;
    // rebuild from the byte offsets instead when the input is not ASCII.
    if !sql.is_ascii() {
        return None;
    }
    Some((template, literals))
}

/// A parsed statement template owned by the connection's cache. The literal
/// slots are the pre-order ordinals of the `Expr::Value` nodes the lifted
/// literals occupy, recorded once, so each execution rebinds the literals in
/// place (`bind`) and runs the statements by reference — the AST used to be
/// deep-cloned per execution so the placeholders could be replaced in a copy.
#[derive(Debug, Clone)]
pub(crate) struct CachedTemplate {
    pub(crate) statements: Vec<Statement>,
    /// (value-node ordinal in visit order, index into the lifted literals).
    literal_slots: Vec<(usize, usize)>,
}

impl CachedTemplate {
    pub(crate) fn prepare(mut statements: Vec<Statement>, literal_count: usize) -> Result<Self> {
        let mut slots = Vec::new();
        let mut missing = None;
        let mut ordinal = 0usize;
        let _ = visit_expressions_mut(&mut statements, |expr| {
            if let Expr::Value(value) = expr {
                if let Value::Placeholder(name) = &value.value {
                    match name
                        .strip_prefix('$')
                        .and_then(|n| n.parse::<usize>().ok())
                        .and_then(|n| n.checked_sub(1))
                        .filter(|index| *index < literal_count)
                    {
                        Some(index) => slots.push((ordinal, index)),
                        None => missing = Some(name.clone()),
                    }
                }
                ordinal += 1;
            }
            ControlFlow::<()>::Continue(())
        });
        match missing {
            Some(name) => Err(SqlError::InvalidSql(format!(
                "statement template placeholder {name} has no bound literal"
            ))),
            None => Ok(Self {
                statements,
                literal_slots: slots,
            }),
        }
    }

    /// Write this execution's literals into their slots.
    pub(crate) fn bind(&mut self, literals: &[BoundLiteral]) -> Result<()> {
        if literals.len()
            < self
                .literal_slots
                .iter()
                .map(|(_, index)| index + 1)
                .max()
                .unwrap_or(0)
        {
            return Err(SqlError::InvalidSql(
                "statement template literal count changed".to_string(),
            ));
        }
        let mut slot = 0usize;
        let mut ordinal = 0usize;
        let _ = visit_expressions_mut(&mut self.statements, |expr| {
            if let Expr::Value(value) = expr {
                if let Some((target, index)) = self.literal_slots.get(slot) {
                    if *target == ordinal {
                        value.value = match &literals[*index] {
                            BoundLiteral::Str(text) => Value::SingleQuotedString(text.clone()),
                            BoundLiteral::Num(text) => Value::Number(text.clone(), false),
                        };
                        slot += 1;
                    }
                }
                ordinal += 1;
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(())
    }
}

/// Replace `$n` placeholders in a cloned template AST with the lifted literals.
pub(crate) fn bind_literals(
    statements: &mut Vec<Statement>,
    literals: &[BoundLiteral],
) -> Result<()> {
    let mut missing = None;
    let _ = visit_expressions_mut(statements, |expr| {
        if let Expr::Value(value) = expr {
            if let Value::Placeholder(name) = &value.value {
                let bound = name
                    .strip_prefix('$')
                    .and_then(|n| n.parse::<usize>().ok())
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| literals.get(i));
                match bound {
                    Some(BoundLiteral::Str(text)) => {
                        value.value = Value::SingleQuotedString(text.clone());
                    }
                    Some(BoundLiteral::Num(text)) => {
                        value.value = Value::Number(text.clone(), false);
                    }
                    None => missing = Some(name.clone()),
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    match missing {
        Some(name) => Err(SqlError::InvalidSql(format!(
            "statement template placeholder {name} has no bound literal"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tpl(sql: &str) -> Option<(String, Vec<BoundLiteral>)> {
        templatize(sql)
    }

    #[test]
    fn lifts_call_arguments_and_values() {
        let (t, lits) = tpl("call neword(1, 16, 5, 1234, 10, 0, '', 'it''s', 0, now())").unwrap();
        assert_eq!(t, "call neword($1, $2, $3, $4, $5, $6, $7, $8, $9, now())");
        assert_eq!(lits[0], BoundLiteral::Num("1".into()));
        assert_eq!(lits[6], BoundLiteral::Str(String::new()));
        assert_eq!(lits[7], BoundLiteral::Str("it's".into()));
        let (t, _) = tpl("INSERT INTO t VALUES (1, 'a', -2.5e3)").unwrap();
        assert_eq!(t, "INSERT INTO t VALUES ($1, $2, -$3)");
    }

    #[test]
    fn leaves_typed_escape_and_keyword_literals_inline() {
        let (t, lits) =
            tpl("update t set d = date '2020-01-01', n = 3 where id = 7 limit 10").unwrap();
        assert_eq!(
            t,
            "update t set d = date '2020-01-01', n = $1 where id = $2 limit 10"
        );
        assert_eq!(lits.len(), 2);
        let (t, _) = tpl("insert into t values (E'a\\n', 1)").unwrap();
        assert_eq!(t, "insert into t values (E'a\\n', $1)");
        assert_eq!(tpl("update t set a = b"), None);
        assert_eq!(tpl("call p($1)"), None);
        assert_eq!(tpl("call p($$x$$, 1)"), None);
        assert_eq!(tpl("insert into t values ('x' ? 'y')"), None);
        assert_eq!(tpl("update t set a = 'ü'"), None);
        let (t, _) = tpl("update t set a = 1 -- trailing 5\n where b = 2").unwrap();
        assert_eq!(t, "update t set a = $1 -- trailing 5\n where b = $2");
        let (t, _) = tpl("update t set a1 = 5, \"c2\" = 6.0").unwrap();
        assert_eq!(t, "update t set a1 = $1, \"c2\" = $2");
    }

    #[test]
    fn second_execution_with_other_literals_skips_the_parser() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, amount NUMERIC(12,2))")
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE bump(p_id INT, p_by NUMERIC) LANGUAGE plpgsql AS $$ BEGIN \
                 UPDATE t SET amount = amount + p_by WHERE id = p_id; END $$",
            )
            .unwrap();
        session
            .execute("INSERT INTO t VALUES (1, 'a', 1.50)")
            .unwrap();
        crate::reset_sql_parse_statement_calls();
        session
            .execute("INSERT INTO t VALUES (2, 'it''s', 2.25)")
            .unwrap();
        session.execute("CALL bump(1, 10)").unwrap();
        let after_first = crate::sql_parse_statement_calls();
        session.execute("INSERT INTO t VALUES (3, 'c', 0)").unwrap();
        session.execute("CALL bump(2, 0.75)").unwrap();
        session
            .execute("UPDATE t SET name = 'z' WHERE id = 3")
            .unwrap();
        session
            .execute("UPDATE t SET name = 'y' WHERE id = 1")
            .unwrap();
        let after_second = crate::sql_parse_statement_calls();
        // The INSERT and CALL templates were cached by the first round; the
        // UPDATE template is parsed once and then hit.
        assert_eq!(
            after_second - after_first,
            1,
            "only the new UPDATE template parses"
        );
        let rows = session
            .execute("SELECT id, name, amount FROM t ORDER BY id")
            .unwrap()
            .rows;
        let cells = rows
            .iter()
            .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(
            cells,
            vec![
                vec!["1", "y", "11.50"],
                vec!["2", "it's", "3.00"],
                vec!["3", "z", "0.00"],
            ]
            .into_iter()
            .map(|r| r.into_iter().map(String::from).collect::<Vec<_>>())
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn hammerdb_call_text_takes_the_cache_path() {
        let sql = "call neword(6,16,7,89,6,0.0,'','',0.0,0.0,0,TO_TIMESTAMP('20260902115830','YYYYMMDDHH24MISS')::timestamp without time zone)";
        assert!(statement_is_cacheable(sql));
        let (template, literals) = templatize(sql).expect("HammerDB CALL text templatizes");
        assert_eq!(literals.len(), 13, "{template}");
        let mut cached = parse_statements(&template)
            .unwrap_or_else(|e| panic!("template must parse: {e}\n{template}"));
        bind_literals(&mut cached, &literals).unwrap();
        assert_eq!(cached, parse_statements(sql).unwrap());
    }

    #[test]
    fn bound_ast_equals_direct_parse() {
        for sql in [
            "call neword(1, 16, 5, 1234, 10, 0, '', 'it''s', 0, 0, 0, now())",
            "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y''z')",
            "UPDATE t SET a = a + 1.5, b = 'q' WHERE id = 3 AND c > -2",
            "DELETE FROM t WHERE k IN (1, 2, 3) AND s = 'v'",
        ] {
            let (template, literals) = templatize(sql).unwrap();
            let mut cached = parse_statements(&template).unwrap();
            bind_literals(&mut cached, &literals).unwrap();
            let direct = parse_statements(sql).unwrap();
            assert_eq!(cached, direct, "{sql}");
        }
    }
}

#[cfg(test)]
mod expr_type_memo_tests {
    use crate::*;

    /// Inside a routine, expression types are inferred once per IR node and
    /// reused across rows and calls; DDL on the table invalidates the memo.
    #[test]
    fn routine_expression_types_are_inferred_once_and_invalidated_by_ddl() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE stock (s_i_id INT PRIMARY KEY, s_w_id INT, s_quantity INT, s_data TEXT)")
            .unwrap();
        for i in 1..=50 {
            session
                .execute(&format!(
                    "INSERT INTO stock VALUES ({i}, 1, {}, 'd{i}')",
                    (i * 7) % 100
                ))
                .unwrap();
        }
        session
            .execute(
                "CREATE PROCEDURE restock(p_w INT, p_min INT, INOUT touched INT) LANGUAGE plpgsql AS $$ \
                 DECLARE q INT; BEGIN touched := 0; \
                 FOR i IN 1..50 LOOP \
                   SELECT s_quantity INTO q FROM stock WHERE s_i_id = i AND s_w_id = p_w; \
                   IF q < p_min THEN UPDATE stock SET s_quantity = s_quantity + 91 WHERE s_i_id = i AND s_w_id = p_w; touched := touched + 1; END IF; \
                 END LOOP; END $$",
            )
            .unwrap();
        let first = session.execute("CALL restock(1, 50, 0)").unwrap().rows;
        let before = crate::eval::expr_type_uncached_calls();
        let second = session.execute("CALL restock(1, 50, 0)").unwrap().rows;
        let after = crate::eval::expr_type_uncached_calls();
        assert_eq!(
            first,
            vec![vec![SqlValue::Int(28)]],
            "first pass restocks the low rows"
        );
        assert_eq!(
            second,
            vec![vec![SqlValue::Int(0)]],
            "second pass finds nothing left to restock"
        );
        assert_eq!(
            after, before,
            "the second call infers no expression types: every node is memoized"
        );

        // DDL changes the schema generation: the next call re-infers.
        session
            .execute("ALTER TABLE stock ADD COLUMN s_note TEXT")
            .unwrap();
        let third = session.execute("CALL restock(1, 200, 0)").unwrap().rows;
        assert_eq!(third, vec![vec![SqlValue::Int(50)]]);
        assert!(
            crate::eval::expr_type_uncached_calls() > after,
            "after DDL the routine's expression types are inferred again"
        );
    }
}

#[cfg(test)]
mod routine_call_fast_path_tests {
    use crate::*;

    /// Nested function calls resolve through the dispatch memo and frames
    /// come from the cached template; results must match the slow path on
    /// every call, including OUT/INOUT parameters, defaults and `$n` access.
    #[test]
    fn nested_calls_repeat_correctly_through_the_memoized_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE FUNCTION between_ints(INTEGER, INTEGER) RETURNS INTEGER AS $$ \
                 DECLARE lo ALIAS FOR $1; hi ALIAS FOR $2; \
                 BEGIN RETURN trunc(0.5 * (hi - lo + 1) + lo); END $$ LANGUAGE plpgsql STRICT",
            )
            .unwrap();
        session
            .execute(
                "CREATE FUNCTION scaled(x NUMERIC, factor NUMERIC DEFAULT 2) RETURNS NUMERIC AS $$ \
                 BEGIN RETURN x * factor + between_ints(1, 3); END $$ LANGUAGE plpgsql",
            )
            .unwrap();
        session
            .execute(
                "CREATE PROCEDURE tally(p_n INT, INOUT total NUMERIC, OUT calls INT) LANGUAGE plpgsql AS $$ \
                 BEGIN calls := 0; \
                 FOR i IN 1..p_n LOOP total := total + scaled(i) + scaled(i, 3) + between_ints(i, i + 4); calls := calls + 3; END LOOP; \
                 END $$",
            )
            .unwrap();
        // between_ints(1,3) = trunc(0.5*3+1) = 2; scaled(i) = 2i + 2; scaled(i,3) = 3i + 2;
        // between_ints(i, i+4) = trunc(0.5*5 + i) = i + 2  → per i: 6i + 6.
        for round in 0..3 {
            let rows = session.execute("CALL tally(4, 0, 0)").unwrap().rows;
            let total = rows[0][0].to_cell();
            assert_eq!(total, "84", "round {round}: sum of 6i+6 for i=1..4");
            assert_eq!(rows[0][1], SqlValue::Int(12), "round {round}");
        }
        assert_eq!(
            session
                .execute("SELECT scaled(10), scaled(10, 0.5)")
                .unwrap()
                .rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["22", "7.0"]
        );
    }
}
