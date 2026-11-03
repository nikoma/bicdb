//! Versioned native dynamic SQL; text is parsed at execution, values stay bound.
use super::*;

thread_local! { static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(crate) struct Scope(bool);
impl Scope {
    pub(crate) fn new(enabled: bool) -> Self {
        Self(ENABLED.with(|v| v.replace(enabled)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        ENABLED.with(|v| v.set(self.0));
    }
}
pub(crate) fn enabled() -> bool {
    ENABLED.with(std::cell::Cell::get)
}

#[derive(Clone, Debug)]
pub(crate) struct Immediate {
    pub command: RoutineExpr,
    pub into: Vec<String>,
    pub using: Vec<RoutineExpr>,
    pub namespace: Option<String>,
}

fn expression(source: &[Token]) -> Result<RoutineExpr> {
    let text = join(source);
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .try_with_sql(&text)
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| SqlError::InvalidSql(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlError::InvalidSql(
            "unexpected trailing dynamic SQL expression tokens".into(),
        ));
    }
    Ok(RoutineExpr::unbound(expr))
}

pub(crate) fn parse(source: &str) -> Result<Option<Immediate>> {
    let source = tokens(source)?;
    if !source.first().is_some_and(|t| word(t, "EXECUTE"))
        || !source.get(1).is_some_and(|t| word(t, "IMMEDIATE"))
    {
        return Ok(None);
    }
    let source = &source[2..];
    let mut depth = 0usize;
    let mut into = None;
    let mut using = None;
    for (index, token) in source.iter().enumerate() {
        match token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
            _ if depth == 0 && word(token, "INTO") => {
                if into.replace(index).is_some() || using.is_some() {
                    return Err(unsupported("dynamic INTO must precede USING"));
                }
            }
            _ if depth == 0 && word(token, "USING") => {
                if using.replace(index).is_some() {
                    return Err(unsupported("duplicate dynamic USING"));
                }
            }
            _ if depth == 0 && (word(token, "BULK") || word(token, "RETURNING")) => {
                return Err(unsupported(
                    "dynamic BULK COLLECT and RETURNING binds require separate qualification",
                ))
            }
            _ => {}
        }
    }
    let command = expression(&source[..into.or(using).unwrap_or(source.len())])?;
    let targets = if let Some(index) = into {
        split_top_level(&source[index + 1..using.unwrap_or(source.len())])
            .into_iter()
            .map(|part| match part {
                [Token::Word(_)] => match expression(part)?.expr {
                    Expr::Identifier(name) => Ok(normalize_object_name(&ident_value(&name))),
                    _ => Err(unsupported("invalid dynamic INTO target")),
                },
                _ => Err(unsupported(
                    "dynamic INTO currently requires scalar variable targets",
                )),
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let binds = if let Some(index) = using {
        split_top_level(&source[index + 1..])
            .into_iter()
            .map(|part| {
                if part.first().is_some_and(|t| word(t, "OUT"))
                    || part
                        .windows(2)
                        .any(|w| word(&w[0], "IN") && word(&w[1], "OUT"))
                {
                    return Err(unsupported(
                        "dynamic USING OUT and IN OUT require a PL/SQL bind contract",
                    ));
                }
                let part = if part.first().is_some_and(|t| word(t, "IN")) {
                    &part[1..]
                } else {
                    part
                };
                expression(part)
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    Ok(Some(Immediate {
        command,
        into: targets,
        using: binds,
        namespace: None,
    }))
}

fn data_error(code: i32, state: &'static str, text: &str) -> SqlError {
    errors::database_error(code, state, text)
}

fn bind_sql(sql: &str, kinds: &[String]) -> Result<String> {
    reject_oversized_sql(sql)?;
    reject_deep_parse_nesting(sql)?;
    let mut source = tokens(sql)?;
    if source.last() == Some(&Token::SemiColon) {
        source.pop();
    }
    if source.iter().any(|t| *t == Token::SemiColon) {
        return Err(data_error(
            -3405,
            "42601",
            "string or document not valid for this SQL operation",
        ));
    }
    let mut output = Vec::new();
    let mut index = 0;
    let mut bound = 0;
    while index < source.len() {
        let consumed = match &source[index] {
            Token::Colon
                if source
                    .get(index + 1)
                    .is_some_and(|t| matches!(t, Token::Word(_) | Token::Number(..))) =>
            {
                2
            }
            Token::Placeholder(name) if name.starts_with(':') => 1,
            _ => 0,
        };
        if consumed != 0 {
            let kind = kinds
                .get(bound)
                .ok_or_else(|| data_error(-1008, "07001", "not all variables bound"))?;
            bound += 1;
            output.push(if kind == "unknown" {
                format!("${bound}")
            } else if kind == "national" {
                format!("bicdb_oracle_national_value(${bound})")
            } else {
                format!("CAST(${bound} AS {kind})")
            });
            index += consumed;
        } else {
            output.push(render_token(&source[index]));
            index += 1;
        }
    }
    if bound != kinds.len() {
        return Err(data_error(-1006, "07002", "bind variable does not exist"));
    }
    Ok(output.join(" "))
}

impl SqlSession<'_> {
    /// Validate a SELECT without fetching it. An unfetched dynamic SELECT must
    /// resolve names and permissions but must not evaluate volatile expressions.
    fn prepare_dynamic_query(&self, query: &Query) -> Result<()> {
        struct Metadata<'s, 'e, 'db> {
            session: &'s SqlSession<'db>,
            engine: &'s SqlEngine<'e>,
            scopes: Vec<Vec<RelationColumns>>,
            order_alias_nodes: FxHashSet<usize>,
        }
        impl sqlparser::ast::Visitor for Metadata<'_, '_, '_> {
            type Break = SqlError;
            fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<SqlError> {
                let SetExpr::Select(select) = query.body.as_ref() else {
                    return ControlFlow::Break(unsupported(
                        "metadata-only dynamic set queries need separate binding",
                    ));
                };
                if query.with.is_some() {
                    return ControlFlow::Break(unsupported(
                        "metadata-only dynamic CTE binding is not implemented",
                    ));
                }
                if let Err(error) = self.engine.authorize_read_relations(query) {
                    return ControlFlow::Break(error);
                }
                let aliases = select
                    .projection
                    .iter()
                    .filter_map(|item| match item {
                        SelectItem::ExprWithAlias { alias, .. } => {
                            Some(normalize_object_name(&ident_value(alias)))
                        }
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                if let Some(order) = &query.order_by {
                    if let OrderByKind::Expressions(expressions) = &order.kind {
                        for item in expressions {
                            if let Expr::Identifier(name) = &item.expr {
                                if aliases.contains(&normalize_object_name(&ident_value(name))) {
                                    self.order_alias_nodes
                                        .insert(&item.expr as *const Expr as usize);
                                }
                            }
                        }
                    }
                }
                let Some(columns) = self.engine.from_relation_columns(&select.from) else {
                    return ControlFlow::Break(unsupported(
                        "dynamic query relation metadata could not be resolved",
                    ));
                };
                self.scopes.push(columns);
                ControlFlow::Continue(())
            }
            fn post_visit_query(&mut self, _: &Query) -> ControlFlow<SqlError> {
                self.scopes.pop();
                ControlFlow::Continue(())
            }
            fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<SqlError> {
                if let TableFactor::Table {
                    name, args: None, ..
                } = factor
                {
                    let name = match relation_name(name) {
                        Ok(name) => name,
                        Err(e) => return ControlFlow::Break(e),
                    };
                    if !is_virtual_table(&name) {
                        if let Err(error) = self.engine.require_relation_privilege(&name, "SELECT")
                        {
                            return ControlFlow::Break(error);
                        }
                    }
                }
                ControlFlow::Continue(())
            }
            fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<SqlError> {
                if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                    && !self
                        .order_alias_nodes
                        .contains(&(expr as *const Expr as usize))
                    && !self
                        .scopes
                        .iter()
                        .rev()
                        .any(|env| self.engine.infer_env_expr_type(expr, env).is_some())
                {
                    return ControlFlow::Break(data_error(-904, "42703", "invalid identifier"));
                }
                if let Expr::Function(function) = expr {
                    let name = match object_name(&function.name) {
                        Ok(name) => name,
                        Err(e) => return ControlFlow::Break(e),
                    };
                    match load_routine(self.session.db_ref(), RoutineKind::Function, &name) {
                        Ok(Some(_)) => {
                            if let Err(e) = self.session.ensure_routine_execute_privilege(&name) {
                                return ControlFlow::Break(e);
                            }
                        }
                        Err(e) => return ControlFlow::Break(e),
                        _ => {}
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let engine = self.sql_engine();
        match sqlparser::ast::Visit::visit(
            query,
            &mut Metadata {
                session: self,
                engine: &engine,
                scopes: Vec::new(),
                order_alias_nodes: FxHashSet::default(),
            },
        ) {
            ControlFlow::Break(error) => Err(error),
            ControlFlow::Continue(()) => Ok(()),
        }
    }

    pub(crate) fn execute_oracle_dynamic(
        &mut self,
        frame: &mut RoutineFrame,
        dynamic: &Immediate,
    ) -> Result<RoutineControl> {
        let value = self.eval_routine_expr(frame, &dynamic.command)?;
        let sql = match value {
            SqlValue::Null => {
                return Err(data_error(
                    -6535,
                    "22004",
                    "statement string in EXECUTE IMMEDIATE is NULL or 0 length",
                ))
            }
            SqlValue::String(sql) if !sql.is_empty() => sql,
            _ => {
                return Err(data_error(
                    -6535,
                    "22004",
                    "statement string in EXECUTE IMMEDIATE is NULL or 0 length",
                ))
            }
        };
        let mut values = Vec::new();
        let mut kinds = Vec::new();
        for bind in &dynamic.using {
            let value = self.eval_routine_expr(frame, bind)?;
            let value = match value {
                SqlValue::String(text) if text.is_empty() => SqlValue::Null,
                other => other,
            };
            if matches!(&bind.expr, Expr::Function(function) if object_name_lowercase(&function.name).is_ok_and(|name| name == "bicdb_oracle_national_value"))
            {
                kinds.push("national".into());
                values.push(value);
                continue;
            }
            let declared = match &bind.expr {
                Expr::Identifier(name) => frame
                    .assignment_types
                    .get(&normalize_object_name(&ident_value(name)))
                    .map(|t| t.pg_type.as_str()),
                _ => None,
            };
            let inferred = projected_expr_pg_type_with_db(self.db_ref(), &bind.expr);
            let kind = declared.or(inferred.as_deref()).unwrap_or_else(|| {
                if matches!(&value, SqlValue::Json(v) if v.is_number()) {
                    "numeric"
                } else {
                    projected_value_pg_type(&value)
                }
            });
            let kind = match pg_type_oid_by_name(kind) {
                Some(20 | 21 | 23 | 1700) => "NUMERIC",
                Some(25 | 1042 | 1043) => "VARCHAR",
                Some(16) => "BOOLEAN",
                Some(1082) => "DATE",
                Some(1114) => "TIMESTAMP",
                _ if kind == "unknown" => "unknown",
                _ => {
                    return Err(unsupported(
                        "dynamic bind type requires separate qualification",
                    ))
                }
            };
            kinds.push(kind.to_string());
            values.push(value);
        }
        let sql = bind_sql(&sql, &kinds)?;
        let namespace = dynamic
            .namespace
            .as_ref()
            .ok_or_else(|| unsupported("dynamic SQL has no compiler-bound namespace"))?;
        let sql = lower_profile_sql_in_namespace(
            self.db_ref(),
            &sql,
            ORACLE_PREVIEW_PROFILE,
            Some(namespace),
        )?;
        let mut statements = parse_statements(&sql)?;
        let [statement] = statements.as_mut_slice() else {
            return Err(unsupported("dynamic SQL requires exactly one statement"));
        };
        namespaces::qualify_with_calls(statement, namespace);
        if !matches!(
            statement,
            Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
        ) {
            return Err(unsupported("dynamic DDL, transaction control and PL/SQL blocks require their own execution contracts"));
        }
        let previous_vars = std::mem::replace(
            &mut self.routine_vars,
            Arc::new(
                values
                    .into_iter()
                    .enumerate()
                    .map(|(n, v)| (format!("${}", n + 1), v))
                    .collect(),
            ),
        );
        let previous_slots = self.routine_slots.take();
        let previous_ir = self.current_routine_ir.take();
        let previous_owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = match statement {
            Statement::Query(query) if dynamic.into.is_empty() => self
                .prepare_dynamic_query(query)
                .map(|_| SqlResult::command("SELECT")),
            Statement::Query(query)
                if query.with.is_none() && matches!(query.body.as_ref(), SetExpr::Select(_)) =>
            {
                self.prepare_dynamic_query(query)
                    .and_then(|_| self.execute_parsed_statement(statement))
            }
            _ => self.execute_parsed_statement(statement),
        };
        self.routine_vars = previous_vars;
        self.routine_slots = previous_slots;
        self.current_routine_ir = previous_ir;
        self.ir_owned_statement = previous_owned;
        let result = result?;
        if !dynamic.into.is_empty() {
            if result.rows.is_empty() {
                return Err(SqlError::NoDataFound);
            }
            if result.rows.len() > 1 {
                return Err(SqlError::RaisedException {
                    sqlstate: "P0003".into(),
                    message: "query returned more than one row".into(),
                    detail: None,
                });
            }
            if result.columns.len() != dynamic.into.len() {
                return Err(data_error(-932, "42804", "inconsistent datatypes"));
            }
            assign_routine_targets_with_columns(
                frame,
                &dynamic.into,
                &result.columns,
                result.rows.into_iter().next(),
            )?;
        }
        Ok(RoutineControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(s: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(s.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        s.execute(&sql)
    }
    fn setup() -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        (root, db)
    }
    #[test]
    fn dynamic_unfetched_select_validates_without_executing() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE dynamic_effects(n NUMBER)",
        "CREATE FUNCTION dynamic_effect RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO dynamic_effects VALUES(1);COMMIT;RETURN 7;END;",
        "CREATE PROCEDURE dynamic_prepare IS BEGIN EXECUTE IMMEDIATE 'SELECT dynamic_effect() FROM DUAL';EXECUTE IMMEDIATE 'SELECT 1/0 FROM DUAL';END;",
        "CALL dynamic_prepare()"] {run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert!(run(&mut s, "SELECT n FROM dynamic_effects")
            .unwrap()
            .rows
            .is_empty());
        run(&mut s,"CREATE PROCEDURE dynamic_bad IS BEGIN EXECUTE IMMEDIATE 'SELECT unknown_variable FROM DUAL';END;").unwrap();
        assert_eq!(
            run(&mut s, "CALL dynamic_bad()").unwrap_err().sqlstate(),
            "42703"
        );
        assert_eq!(
            run(&mut s, "SELECT dynamic_effect() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "7"
        );
        assert_eq!(
            run(&mut s, "SELECT n FROM dynamic_effects")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn dynamic_quote_layers_and_sql_no_data_boundary() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s,"CREATE FUNCTION dynamic_nested RETURN VARCHAR2 IS v VARCHAR2(100);BEGIN EXECUTE IMMEDIATE 'SELECT ''O''''Reilly'' FROM DUAL' INTO v;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut s, "SELECT dynamic_nested() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "O'Reilly"
        );
        assert_eq!(
            run(&mut s, "SELECT 'O''''Reilly' FROM DUAL").unwrap().rows[0][0].to_cell(),
            "O''Reilly"
        );
        for sql in ["CREATE FUNCTION dynamic_no_data RETURN NUMBER IS v NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT 1 FROM DUAL WHERE 1=0' INTO v;RETURN v;END;",
            "CREATE FUNCTION dynamic_pl_caller RETURN NUMBER IS v NUMBER;BEGIN v:=dynamic_no_data();RETURN 42;EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 99;END;"] {run(&mut s,sql).unwrap();}
        assert_eq!(
            run(&mut s, "SELECT dynamic_no_data() FROM DUAL")
                .unwrap()
                .rows[0][0],
            SqlValue::Null
        );
        assert_eq!(
            run(&mut s, "SELECT dynamic_pl_caller() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "99"
        );
        s.execute("CREATE FUNCTION native_no_data() RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'native no data' USING ERRCODE='P0002';END$$").unwrap();
        assert_eq!(
            s.execute("SELECT native_no_data()").unwrap_err().sqlstate(),
            "P0002"
        );
    }

    #[test]
    fn dynamic_bind_expression_keeps_chained_character_types() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s,"CREATE FUNCTION dynamic_chain(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(200);cmd VARCHAR2(100);BEGIN cmd:='SELECT :1'||' FROM DUAL';EXECUTE IMMEDIATE cmd INTO v USING 'start:'||p||':end';RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut s, "SELECT dynamic_chain('O''''Reilly') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "start:O''Reilly:end"
        );
    }

    #[test]
    fn character_bind_types_survive_static_query_scopes() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE dynamic_patterns(label VARCHAR2(30))","INSERT INTO dynamic_patterns VALUES('abc')",
            "CREATE FUNCTION dynamic_pattern(p_text IN VARCHAR2) RETURN NUMBER IS n NUMBER;BEGIN SELECT COUNT(*) INTO n FROM dynamic_patterns WHERE lower(label) LIKE lower(SUBSTR(p_text,1,2))||'%';RETURN n;END;"] {
            run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));
        }
        assert_eq!(
            run(&mut s, "SELECT dynamic_pattern('abc') FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
    }

    #[test]
    fn dynamic_positional_binds_and_typed_into() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s,"CREATE FUNCTION dynamic_sum RETURN NUMBER IS v NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT :x+:x FROM DUAL' INTO v USING 2,3;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut s, "SELECT dynamic_sum() FROM DUAL").unwrap().rows[0][0].to_cell(),
            "5"
        );
        run(&mut s,"CREATE FUNCTION dynamic_null RETURN NUMBER IS v NUMBER;n NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT NVL(:1,9) FROM DUAL' INTO v USING n;RETURN v;END;").unwrap();
        assert_eq!(
            run(&mut s, "SELECT dynamic_null() FROM DUAL").unwrap().rows[0][0].to_cell(),
            "9"
        );
    }
    #[test]
    fn dynamic_sql_owns_namespace_and_has_no_caller_variables() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE SCHEMA dyn_one","CREATE SCHEMA dyn_two","CREATE TABLE dyn_one.rows(n NUMBER)","CREATE TABLE dyn_two.rows(n NUMBER)","INSERT INTO dyn_one.rows VALUES(7)","INSERT INTO dyn_two.rows VALUES(99)",
        "CREATE FUNCTION dyn_one.read_value RETURN NUMBER IS n NUMBER:=33;v NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT n FROM rows' INTO v;RETURN v;END;",
        "CREATE FUNCTION dyn_one.external_value RETURN NUMBER IS v NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT n FROM dyn_two.rows' INTO v;RETURN v;END;"] {run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert_eq!(
            run(&mut s, "SELECT dyn_one.read_value() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "7"
        );
        assert_eq!(
            run(&mut s, "SELECT dyn_one.external_value() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "99"
        );
    }
    #[test]
    fn dynamic_dml_rollback_and_unreached_branch() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE dynamic_rows(n NUMBER PRIMARY KEY)",
        "CREATE PROCEDURE dynamic_write(p NUMBER) IS BEGIN EXECUTE IMMEDIATE 'INSERT INTO dynamic_rows VALUES(:1)' USING p;END;",
        "CREATE FUNCTION dynamic_static(p NUMBER) RETURN NUMBER IS v NUMBER;BEGIN IF p=1 THEN RETURN 7;ELSE EXECUTE IMMEDIATE 'SELECT :1 FROM DUAL' INTO v USING 9;RETURN v;END IF;END;",
        "BEGIN","CALL dynamic_write(4)"] {run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        run(&mut s, "ROLLBACK").unwrap();
        assert!(run(&mut s, "SELECT n FROM dynamic_rows")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(
            run(&mut s, "SELECT dynamic_static(1) FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "7"
        );
        assert_eq!(
            run(&mut s, "SELECT dynamic_static(2) FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "9"
        );
    }
    #[test]
    fn dynamic_binds_are_values_and_bind_count_is_positional() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        assert_eq!(
            run(&mut s, "SELECT 'x'');DELETE FROM anything;--' FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "x');DELETE FROM anything;--"
        );
        run(&mut s,"CREATE FUNCTION dynamic_text(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN EXECUTE IMMEDIATE 'SELECT :name FROM DUAL' INTO v USING IN p;RETURN v;END;").unwrap();
        assert_eq!(
            run(
                &mut s,
                "SELECT dynamic_text('x'');DELETE FROM anything;--') FROM DUAL"
            )
            .unwrap()
            .rows[0][0]
                .to_cell(),
            "x');DELETE FROM anything;--"
        );
        for (name, binds, code) in [("short", "2", "07001"), ("long", "2,3,4", "07002")] {
            run(&mut s,&format!("CREATE FUNCTION dynamic_{name} RETURN NUMBER IS v NUMBER;BEGIN EXECUTE IMMEDIATE 'SELECT :x+:x FROM DUAL' INTO v USING {binds};RETURN v;END;")).unwrap();
            assert_eq!(
                run(&mut s, &format!("SELECT dynamic_{name}() FROM DUAL"))
                    .unwrap_err()
                    .sqlstate(),
                code
            );
        }
    }
}
