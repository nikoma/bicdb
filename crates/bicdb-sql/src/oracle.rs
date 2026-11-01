//! Explicit, bounded Oracle HR front end. It lowers into native PostgreSQL
//! statements; the default parser, value codecs and transaction engine do not
//! change. This profile is not a claim of general Oracle compatibility.
use crate::*;
use sqlparser::{dialect::OracleDialect, keywords::Keyword};

pub const ORACLE_HR_PROFILE: &str = "oracle-hr-v1";
pub const POSTGRES_PROFILE: &str = "postgres";
const PROFILE_COLLECTION: &str = "__bicdb_sql_compatibility";

/// Persist the non-default profile with the database, including native backups.
/// Existing PostgreSQL databases cannot be silently converted by a serve flag.
pub fn ensure_database_profile(db: &mut BicDb, profile: &str) -> Result<()> {
    if !matches!(profile, POSTGRES_PROFILE | ORACLE_HR_PROFILE) {
        return Err(unsupported(format!("unknown SQL profile {profile}")));
    }
    if db
        .collections()
        .iter()
        .any(|c| c.name == PROFILE_COLLECTION)
    {
        let marker = db
            .get(PROFILE_COLLECTION, "profile")?
            .ok_or_else(|| unsupported("SQL profile marker is missing"))?;
        if marker.metadata != serde_json::json!({"format":1,"profile":profile}) {
            return Err(unsupported(
                "database SQL profile does not match server configuration",
            ));
        }
        return Ok(());
    }
    if profile == POSTGRES_PROFILE {
        return Ok(());
    }
    if !list_schemas(db)?.is_empty() || db.collections().iter().any(|c| !c.name.starts_with("__")) {
        return Err(unsupported(
            "Oracle profile must be provisioned on an empty database",
        ));
    }
    db.create_collection(PROFILE_COLLECTION)?;
    db.insert(
        PROFILE_COLLECTION,
        bicdb_core::Record::new("profile")
            .with_metadata(serde_json::json!({"format":1,"profile":profile})),
    )?;
    Ok(())
}

fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::Unsupported(format!("{ORACLE_HR_PROFILE}: {}", message.into()))
}

fn word(token: &Token, value: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(value))
}

fn tokens(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(&OracleDialect {}, sql)
        .tokenize()
        .map(|tokens| {
            tokens
                .into_iter()
                .filter(|t| !matches!(t, Token::Whitespace(_)))
                .collect()
        })
        .map_err(|e| unsupported(e.to_string()))
}

fn join(tokens: &[Token]) -> String {
    tokens
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Lower Oracle-profile source before pgwire parameter inference, Describe or
/// execution. The server fixes the profile for the lifetime of the database.
pub fn lower_hr_sql(db: &BicDb, sql: &str) -> Result<String> {
    let source = tokens(sql)?;
    if source.is_empty() {
        return Ok(String::new());
    }
    if source
        .iter()
        .any(|t| word(t, "PACKAGE") || word(t, "AUTONOMOUS_TRANSACTION"))
    {
        return Err(unsupported(
            "packages and autonomous transactions are not implemented",
        ));
    }
    if source
        .windows(2)
        .any(|w| word(&w[0], "CONNECT") && word(&w[1], "BY"))
    {
        return Err(unsupported("hierarchical queries are not implemented"));
    }
    if word(&source[0], "ALTER") && source.get(1).is_some_and(|t| word(t, "SESSION")) {
        return Err(unsupported(
            "ALTER SESSION requires an explicit migration adaptation",
        ));
    }
    let routine = source.iter().take(4).any(|t| word(t, "PROCEDURE"));
    let block = word(&source[0], "BEGIN") && source.iter().any(|t| word(t, "END"));
    if routine || block {
        return lower_block(db, &source, routine);
    }
    let mut canonical = canonical_tokens(db, &source)?;
    expand_references(db, &mut canonical)?;
    if canonical.len() > 3 && word(&canonical[0], "CREATE") && word(&canonical[1], "SEQUENCE") {
        fn rank(t: &Token) -> Option<usize> {
            [
                "INCREMENT",
                "MINVALUE",
                "MAXVALUE",
                "START",
                "CACHE",
                "NO",
                "CYCLE",
            ]
            .iter()
            .position(|s| word(t, s))
        }
        if let Some(first) = (3..canonical.len()).find(|i| rank(&canonical[*i]).is_some()) {
            let starts = (first..canonical.len())
                .filter(|i| rank(&canonical[*i]).is_some())
                .collect::<Vec<_>>();
            let mut options = starts
                .iter()
                .enumerate()
                .map(|(i, start)| {
                    let end = starts.get(i + 1).copied().unwrap_or(canonical.len());
                    (
                        rank(&canonical[*start]).unwrap(),
                        canonical[*start..end].to_vec(),
                    )
                })
                .collect::<Vec<_>>();
            // NO CYCLE is a single option, not two independent clauses.
            for i in (1..options.len()).rev() {
                if options[i - 1].1.last().is_some_and(|t| word(t, "NO")) {
                    let tail = options.remove(i).1;
                    options[i - 1].1.extend(tail);
                }
            }
            options.sort_by_key(|(rank, _)| *rank);
            canonical.truncate(first);
            for (_, option) in options {
                canonical.extend(option);
            }
        }
    }
    // Oracle's parenthesized ADD list becomes separate native ALTERs. Each is
    // ordinary authorized DDL; no constraint is silently dropped.
    if canonical.len() > 5 && word(&canonical[0], "ALTER") && word(&canonical[1], "TABLE") {
        if let Some(add) = canonical.iter().position(|t| word(t, "ADD")) {
            if canonical.get(add + 1) == Some(&Token::LParen) {
                let end = canonical
                    .iter()
                    .rposition(|t| *t == Token::RParen)
                    .ok_or_else(|| unsupported("unclosed ALTER ADD list"))?;
                let prefix = join(&canonical[..add + 1]);
                let groups = split_top_level(&canonical[add + 2..end]);
                return groups
                    .iter()
                    .map(|group| lower_native(&format!("{prefix} {}", join(group))))
                    .collect::<Result<Vec<_>>>()
                    .map(|parts| parts.join("; "));
            }
        }
    }
    lower_native(&join(&canonical))
}

fn expand_references(db: &BicDb, source: &mut Vec<Token>) -> Result<()> {
    let mut insertions = Vec::new();
    for i in 0..source.len() {
        if !word(&source[i], "REFERENCES") {
            continue;
        }
        let mut end = i + 2;
        while source.get(end) == Some(&Token::Period) {
            end += 2;
        }
        if source.get(end) == Some(&Token::LParen) {
            continue;
        }
        let identifiers = source[i + 1..end]
            .iter()
            .filter_map(|token| match token {
                Token::Word(word) => Some(match word.quote_style {
                    Some(q) => Ident::with_quote(q, word.value.clone()),
                    None => Ident::new(word.value.clone()),
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let name = relation_name(&ObjectName::from(identifiers))?;
        let schema = load_schema(db, &name)?
            .ok_or_else(|| unsupported(format!("unknown referenced relation {name}")))?;
        let pending = if source.get(2) == source.get(i + 1) {
            source
                .windows(3)
                .position(|w| word(&w[0], "PRIMARY") && word(&w[1], "KEY") && w[2] == Token::LParen)
                .and_then(|start| {
                    source[start + 3..]
                        .iter()
                        .position(|t| *t == Token::RParen)
                        .map(|len| source[start + 3..start + 3 + len].to_vec())
                })
        } else {
            None
        };
        let columns = if let Some(columns) = pending {
            columns
        } else {
            if schema.has_hidden_primary_key() {
                return Err(unsupported("REFERENCES requires a declared primary key"));
            }
            let keys = primary_key_columns_for_schema(&schema);
            let sql = keys
                .iter()
                .map(|key| format!("\"{}\"", key.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(",");
            tokens(&sql)?
        };
        let mut inserted = vec![Token::LParen];
        inserted.extend(columns);
        inserted.push(Token::RParen);
        insertions.push((end, inserted));
    }
    for (index, tokens) in insertions.into_iter().rev() {
        source.splice(index..index, tokens);
    }
    Ok(())
}

fn split_top_level(source: &[Token]) -> Vec<&[Token]> {
    let (mut depth, mut start) = (0, 0);
    let mut groups = Vec::new();
    for (i, t) in source.iter().enumerate() {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Comma if depth == 0 => {
                groups.push(&source[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    groups.push(&source[start..]);
    groups
}

fn syntax_word(value: &str) -> bool {
    matches!(
        value,
        "CREATE"
            | "OR"
            | "REPLACE"
            | "TABLE"
            | "VIEW"
            | "INDEX"
            | "UNIQUE"
            | "ALTER"
            | "ADD"
            | "DROP"
            | "COLUMN"
            | "CONSTRAINT"
            | "PRIMARY"
            | "KEY"
            | "FOREIGN"
            | "REFERENCES"
            | "CHECK"
            | "NOT"
            | "NULL"
            | "DEFAULT"
            | "ON"
            | "DELETE"
            | "UPDATE"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "SET"
            | "SELECT"
            | "DISTINCT"
            | "ALL"
            | "FROM"
            | "WHERE"
            | "AND"
            | "AS"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "OUTER"
            | "CROSS"
            | "USING"
            | "GROUP"
            | "BY"
            | "HAVING"
            | "ORDER"
            | "ASC"
            | "DESC"
            | "NULLS"
            | "FIRST"
            | "LAST"
            | "LIMIT"
            | "OFFSET"
            | "FETCH"
            | "ROWS"
            | "ROW"
            | "ONLY"
            | "WITH"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "IS"
            | "IN"
            | "BETWEEN"
            | "LIKE"
            | "EXISTS"
            | "TRUE"
            | "FALSE"
            | "CAST"
            | "TEXT"
            | "VARCHAR"
            | "CHAR"
            | "NUMERIC"
            | "DECIMAL"
            | "INT"
            | "INTEGER"
            | "BIGINT"
            | "SMALLINT"
            | "BOOLEAN"
            | "TIMESTAMP"
            | "INTERVAL"
            | "TIME"
            | "ZONE"
            | "WITHOUT"
            | "PRECISION"
            | "SEQUENCE"
            | "START"
            | "INCREMENT"
            | "MINVALUE"
            | "MAXVALUE"
            | "CACHE"
            | "CYCLE"
            | "NO"
            | "COMMENT"
            | "COMMIT"
            | "ROLLBACK"
            | "BEGIN"
            | "SAVEPOINT"
            | "TO"
            | "TRANSACTION"
            | "READ"
            | "WRITE"
            | "FOR"
            | "OF"
            | "NOWAIT"
            | "SKIP"
            | "LOCKED"
            | "PROCEDURE"
            | "FUNCTION"
            | "LANGUAGE"
            | "RETURN"
            | "RETURNS"
            | "DECLARE"
            | "IF"
            | "ELSIF"
            | "LOOP"
            | "WHILE"
            | "EXIT"
            | "CONTINUE"
            | "CALL"
            | "RAISE"
            | "EXCEPTION"
            | "PERFORM"
            | "INOUT"
            | "OUT"
            | "DO"
            | "RESET"
            | "DISCARD"
            | "SHOW"
            | "LOCAL"
            | "CASCADE"
            | "RESTRICT"
            | "TRUNCATE"
            | "RENAME"
            | "ENABLE"
            | "DISABLE"
            | "TRIGGER"
            | "AFTER"
            | "BEFORE"
            | "EACH"
            | "EXECUTE"
            | "OVER"
            | "PARTITION"
            | "RANGE"
            | "CURRENT"
            | "UNBOUNDED"
            | "PRECEDING"
            | "FOLLOWING"
    )
}

fn canonical_tokens(db: &BicDb, source: &[Token]) -> Result<Vec<Token>> {
    let mut pieces = Vec::new();
    let mut i = 0;
    while i < source.len() {
        let t = &source[i];
        if let Token::Word(w) = t {
            let upper = w.value.to_ascii_uppercase();
            if w.quote_style.is_none() {
                if i + 4 < source.len()
                    && source[i + 1] == Token::Period
                    && source[i + 3] == Token::Mod
                    && word(&source[i + 4], "TYPE")
                {
                    let column = match &source[i + 2] {
                        Token::Word(w) => w.value.to_uppercase(),
                        _ => return Err(unsupported("invalid %TYPE column")),
                    };
                    let name = w.value.to_uppercase();
                    let schema = load_schema(db, &name)?
                        .ok_or_else(|| unsupported(format!("unknown %TYPE table {name}")))?;
                    let ty = schema
                        .columns
                        .iter()
                        .find(|c| c.name == column)
                        .map(|c| c.pg_type.clone())
                        .ok_or_else(|| unsupported(format!("unknown %TYPE column {column}")))?;
                    pieces.push(ty);
                    i += 5;
                    continue;
                }
                if source.get(i + 1) == Some(&Token::Period)
                    && source
                        .get(i + 2)
                        .is_some_and(|t| word(t, "NEXTVAL") || word(t, "CURRVAL"))
                {
                    let function = if word(&source[i + 2], "NEXTVAL") {
                        "nextval"
                    } else {
                        "currval"
                    };
                    pieces.push(format!(
                        "{function}('\"{}\"')",
                        w.value.to_uppercase().replace('\'', "''")
                    ));
                    i += 3;
                    continue;
                }
                match upper.as_str() {
                    "NUMBER" => pieces.push("NUMERIC".into()),
                    "VARCHAR2" => pieces.push("VARCHAR".into()),
                    "DATE" => pieces.push("TIMESTAMP(0)".into()),
                    "NOCACHE" => pieces.push("CACHE 1".into()),
                    "NOCYCLE" => pieces.push("NO CYCLE".into()),
                    "SYSDATE" => pieces.push(
                        "CAST(date_trunc('second', clock_timestamp()) AS TIMESTAMP(0))".into(),
                    ),
                    "NVL" => pieces.push("coalesce".into()),
                    "TO_DATE" if source.get(i + 1) == Some(&Token::LParen) => {
                        pieces.push("bicdb_oracle_to_date".into())
                    }
                    "ORGANIZATION" => {
                        return Err(unsupported(
                            "index-organized storage requires an explicit heap-storage migration",
                        ))
                    }
                    _ if source.get(i + 1) == Some(&Token::LParen)
                        && matches!(
                            upper.as_str(),
                            "TO_DATE"
                                | "TO_CHAR"
                                | "TO_NUMBER"
                                | "COALESCE"
                                | "NULLIF"
                                | "COUNT"
                                | "SUM"
                                | "AVG"
                                | "MIN"
                                | "MAX"
                                | "ROUND"
                                | "TRUNC"
                                | "UPPER"
                                | "LOWER"
                                | "LENGTH"
                                | "SUBSTR"
                                | "REPLACE"
                                | "CONCAT"
                                | "CURRENT_SETTING"
                                | "SET_CONFIG"
                                | "PG_TYPEOF"
                        ) =>
                    {
                        pieces.push(w.value.to_lowercase())
                    }
                    _ if !syntax_word(&upper) => pieces.push(format!(
                        "\"{}\"",
                        w.value.to_uppercase().replace('"', "\"\"")
                    )),
                    _ => pieces.push(t.to_string()),
                }
            } else {
                pieces.push(t.to_string());
            }
        } else if matches!(t,Token::SingleQuotedString(value) if value.is_empty()) {
            pieces.push("NULL".into());
        } else {
            pieces.push(t.to_string());
        }
        i += 1;
    }
    Tokenizer::new(&PostgreSqlDialect {}, &pieces.join(" "))
        .tokenize()
        .map(|t| {
            t.into_iter()
                .filter(|t| !matches!(t, Token::Whitespace(_)))
                .collect()
        })
        .map_err(|e| unsupported(e.to_string()))
}

fn lower_block(db: &BicDb, source: &[Token], routine: bool) -> Result<String> {
    if source.iter().any(|t| *t == Token::StringConcat) {
        return Err(unsupported("procedural concatenation is not yet supported"));
    }
    let canonical = canonical_tokens(db, source)?;
    let (header, body) = if routine {
        let boundary = canonical
            .iter()
            .position(|t| word(t, "IS") || word(t, "AS"))
            .ok_or_else(|| unsupported("procedure requires IS or AS"))?;
        (Some(&canonical[..boundary]), &canonical[boundary + 1..])
    } else {
        (None, canonical.as_slice())
    };
    let mut body = body.to_vec();
    // Oracle permits END procedure_name; PL/pgSQL uses END; here.
    if let Some(end) = body.iter().rposition(|t| word(t, "END")) {
        if matches!(body.get(end + 1), Some(Token::Word(_)))
            && body.get(end + 2) == Some(&Token::SemiColon)
        {
            body.remove(end + 1);
        }
    }
    // A standalone PL/SQL procedure invocation becomes native CALL. Expressions
    // and assignments are untouched; this is token based, never string regex.
    for i in (1..body.len()).rev() {
        if (word(&body[i - 1], "BEGIN") || body[i - 1] == Token::SemiColon)
            && matches!(&body[i],Token::Word(w) if w.quote_style.is_some())
            && body.get(i + 1) == Some(&Token::LParen)
        {
            body.insert(
                i,
                Token::Word(sqlparser::tokenizer::Word {
                    value: "CALL".to_string(),
                    quote_style: None,
                    keyword: Keyword::CALL,
                }),
            );
        }
    }
    let body = join(&body);
    let delimiter = (0..100)
        .map(|n| format!("$bicdb_oracle_{n}$"))
        .find(|s| !body.contains(s))
        .ok_or_else(|| unsupported("procedure delimiter exhausted"))?;
    Ok(match header {
        Some(header) => format!(
            "{} LANGUAGE plpgsql AS {delimiter}{body}{delimiter}",
            join(header)
        ),
        None => format!("DO {delimiter}{body}{delimiter}"),
    })
}

fn expression(sql: &str) -> Result<Expr> {
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {sql}"))
        .map_err(|e| unsupported(e.to_string()))?;
    match statements.remove(0) {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(mut select) => match select.projection.remove(0) {
                SelectItem::UnnamedExpr(expr) => Ok(expr),
                _ => Err(unsupported("invalid expression")),
            },
            _ => Err(unsupported("invalid expression")),
        },
        _ => Err(unsupported("invalid expression")),
    }
}

fn lower_native(sql: &str) -> Result<String> {
    let mut statements =
        Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| unsupported(e.to_string()))?;
    let result = visit_expressions_mut(&mut statements, |expr| {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } = expr
        {
            match expression(&format!("NULLIF(concat({left},{right}), '')")) {
                Ok(lowered) => *expr = lowered,
                Err(e) => return ControlFlow::Break(e),
            }
        }
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(e) = result {
        return Err(e);
    }
    struct Dual;
    impl sqlparser::ast::VisitorMut for Dual {
        type Break = SqlError;
        fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
            let only_dual = select.from.len() == 1
                && select.from[0].joins.is_empty()
                && matches!(&select.from[0].relation,TableFactor::Table{name,..} if name.to_string()=="\"DUAL\"");
            if only_dual {
                let mut sequence = false;
                let mut references_row = false;
                let _ = sqlparser::ast::visit_expressions(select, |expr| {
                    match expr {
                        Expr::Function(function)
                            if matches!(
                                function.name.to_string().as_str(),
                                "nextval" | "currval"
                            ) =>
                        {
                            sequence = true
                        }
                        Expr::Identifier(_)
                        | Expr::CompoundIdentifier(_)
                        | Expr::Wildcard(_)
                        | Expr::QualifiedWildcard(..) => references_row = true,
                        _ => {}
                    }
                    ControlFlow::<()>::Continue(())
                });
                if sequence {
                    if references_row {
                        return ControlFlow::Break(unsupported(
                            "sequence projections from DUAL cannot also reference its row",
                        ));
                    }
                    // Stateful sequence evaluation belongs to SqlSession's
                    // scalar SELECT path, rather than a read-only derived row.
                    select.from.clear();
                }
            }
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::Identifier(ident) => fold(ident),
                Expr::CompoundIdentifier(idents) => idents.iter_mut().for_each(fold),
                _ => {}
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
            if let Statement::CreateTable(table) = statement {
                for column in &mut table.columns {
                    fold(&mut column.name);
                }
            }
            ControlFlow::Continue(())
        }
        fn post_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, alias, .. } = table {
                if name.to_string() == "\"DUAL\"" {
                    let mut parsed = Parser::parse_sql(
                        &PostgreSqlDialect {},
                        "SELECT * FROM (SELECT 'X' AS \"DUMMY\") AS \"DUAL\"",
                    )
                    .unwrap();
                    if let Statement::Query(query) = parsed.remove(0) {
                        if let SetExpr::Select(mut select) = *query.body {
                            let mut replacement = select.from.remove(0).relation;
                            if let TableFactor::Derived {
                                alias: new_alias, ..
                            } = &mut replacement
                            {
                                if alias.is_some() {
                                    *new_alias = alias.clone();
                                }
                            }
                            *table = replacement;
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    fn fold(ident: &mut Ident) {
        if ident.quote_style.is_none() {
            *ident = Ident::with_quote('"', ident.value.to_uppercase());
        }
    }
    if let ControlFlow::Break(error) = sqlparser::ast::VisitMut::visit(&mut statements, &mut Dual) {
        return Err(error);
    }
    Ok(statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hr_to_date_preserves_time_without_changing_postgres_to_date() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        for (source, expected) in [
            (
                "SELECT TO_DATE('2020-02-29 12:34:56','YYYY-MM-DD HH24:MI:SS') FROM DUAL",
                "2020-02-29 12:34:56",
            ),
            (
                "SELECT TO_DATE('2020-02-29','YYYY-MM-DD') FROM DUAL",
                "2020-02-29 00:00:00",
            ),
        ] {
            let lowered = lower_hr_sql(&db, source).unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), expected);
            assert_eq!(result.column_types, vec![Some("timestamp".into())]);
        }
        let mut session = SqlSession::new(&mut db);
        let result = session
            .execute("SELECT to_date('2020-02-29 12:34:56','YYYY-MM-DD HH24:MI:SS')")
            .unwrap();
        assert_eq!(result.rows[0][0].to_cell(), "2020-02-29");
        assert_eq!(result.column_types, vec![Some("date".into())]);
    }

    #[test]
    fn hr_front_end_preserves_postgres_and_lowers_oracle() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let sql=lower_hr_sql(&db,"CREATE TABLE employees (employee_id NUMBER(6) PRIMARY KEY, name VARCHAR2(20), hired DATE)").unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
        let insert = lower_hr_sql(
            &db,
            "INSERT INTO employees VALUES (1, '', DATE '2020-01-02')",
        )
        .unwrap();
        SqlSession::new(&mut db).execute(&insert).unwrap();
        let query = lower_hr_sql(
            &db,
            "SELECT NVL(name, 'unknown'), 'A' || NULL FROM employees",
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&query).unwrap();
        assert_eq!(
            result.rows,
            vec![vec![
                SqlValue::String("unknown".into()),
                SqlValue::String("A".into())
            ]]
        );
        let dual = lower_hr_sql(&db, "SELECT DUMMY FROM DUAL").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&dual).unwrap().rows,
            vec![vec![SqlValue::String("X".into())]]
        );
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT '' IS NULL")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Bool(false)]]
        );
    }

    #[test]
    fn published_hr_schema_and_data_execute() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../examples/oracle/hr/migration.json"))
                .unwrap();
        for (index, sql) in fixture["statements"].as_array().unwrap().iter().enumerate() {
            let source = sql.as_str().unwrap();
            let lowered = lower_hr_sql(&db, source)
                .unwrap_or_else(|e| panic!("lower #{index}: {e}; source: {source}"));
            SqlSession::new(&mut db)
                .execute(&lowered)
                .unwrap_or_else(|e| panic!("execute #{index}: {e}; lowered: {lowered}"));
        }
        let query = lower_hr_sql(&db, "SELECT COUNT(*) FROM employees").unwrap();
        let result = SqlSession::new(&mut db).execute(&query).unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Int(107)]]);
        let join=lower_hr_sql(&db,"SELECT e.department_id, d.department_id FROM employees e JOIN departments d ON e.department_id=d.department_id WHERE e.employee_id=100").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&join).unwrap().rows,
            vec![vec![
                SqlValue::String("90".into()),
                SqlValue::String("90".into())
            ]]
        );
        let view = lower_hr_sql(&db, "SELECT COUNT(*) FROM emp_details_view").unwrap();
        assert_eq!(
            SqlSession::new(&mut db).execute(&view).unwrap().rows,
            vec![vec![SqlValue::Int(106)]]
        );
    }

    #[test]
    fn profile_is_durable_and_cannot_convert_existing_postgres_tables() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(root.path()).unwrap();
            ensure_database_profile(&mut db, ORACLE_HR_PROFILE).unwrap();
        }
        let mut db = BicDb::open(root.path()).unwrap();
        assert!(ensure_database_profile(&mut db, POSTGRES_PROFILE).is_err());
        ensure_database_profile(&mut db, ORACLE_HR_PROFILE).unwrap();
        assert!(ensure_database_profile(&mut db, "unknown").is_err());
        let postgres = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(postgres.path()).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE existing(id int PRIMARY KEY)")
            .unwrap();
        assert!(ensure_database_profile(&mut db, ORACLE_HR_PROFILE).is_err());
        ensure_database_profile(&mut db, POSTGRES_PROFILE).unwrap();
    }
}
