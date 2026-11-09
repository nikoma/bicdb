//! Convert conjunctive Oracle (+) predicates to explicit outer joins.
use super::*;

fn marked<T: sqlparser::ast::Visit>(node: &T) -> bool {
    let mut result = false;
    let _ = sqlparser::ast::visit_expressions(node, |expr| {
        result |= matches!(expr, Expr::OuterJoin(_));
        ControlFlow::<()>::Continue(())
    });
    result
}

#[derive(Clone)]
struct Source {
    factor: TableFactor,
    alias: Ident,
    path: Vec<String>,
    explicit_alias: bool,
    columns: BTreeSet<String>,
}

fn oracle_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_uppercase()
    }
}

fn source(
    db: &BicDb,
    factor: &TableFactor,
    namespace: Option<&str>,
    query: &Query,
) -> Result<Source> {
    let (name, alias) = match factor {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => (Some(name), alias.as_ref()),
        TableFactor::Derived {
            alias: Some(alias), ..
        } => (None, Some(alias)),
        _ => {
            return Err(unsupported(
                "legacy outer joins require named tables or aliased derived queries",
            ))
        }
    };
    let id = alias
        .map(|a| a.name.clone())
        .or_else(|| name.and_then(|n| n.0.last()?.as_ident().cloned()))
        .ok_or_else(|| unsupported("legacy outer join source requires a name"))?;
    let mut qualified = name.cloned();
    if let (Some(name), Some(namespace)) = (&mut qualified, namespace) {
        if name.0.len() == 1 {
            name.0.insert(
                0,
                sqlparser::ast::ObjectNamePart::Identifier(Ident::with_quote('"', namespace)),
            );
        }
    }
    let path = qualified
        .as_ref()
        .map(|name| {
            name.0
                .iter()
                .filter_map(|part| part.as_ident())
                .map(oracle_ident)
                .collect()
        })
        .unwrap_or_default();
    let columns = qualified
        .as_ref()
        .and_then(|name| relation_name(name).ok())
        .map(|name| load_schema(db, &name))
        .transpose()?
        .flatten()
        .map(|schema| {
            schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<BTreeSet<_>>()
        });
    let columns = match columns {
        Some(columns) => columns,
        None => {
            // Derived/CTE sources retain query-local names. Inference is only
            // used for unqualified columns; qualified aliases need no guessing.
            let sql = format!(
                "{} SELECT * FROM {}",
                query.with.as_ref().map(render_sql).unwrap_or_default(),
                render_sql(factor)
            );
            infer_query_result_columns(db, &sql)
                .ok()
                .flatten()
                .unwrap_or_default()
                .into_iter()
                .map(|(name, _)| name)
                .collect()
        }
    };
    let mut factor = factor.clone();
    if let TableFactor::Table { alias: target, .. } = &mut factor {
        if target.is_none() {
            *target = Some(TableAlias {
                explicit: true,
                name: id.clone(),
                columns: Vec::new(),
                at: None,
            });
        }
    }
    Ok(Source {
        factor,
        alias: id,
        path,
        explicit_alias: alias.is_some(),
        columns,
    })
}

fn owner(sources: &[Source], parts: &[Ident], required: bool) -> Result<Option<usize>> {
    let names = parts.iter().map(oracle_ident).collect::<Vec<_>>();
    let Some((column, qualifier)) = names.split_last() else {
        return Err(unsupported("empty outer join column"));
    };
    let candidates = sources
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            if qualifier.is_empty() {
                s.columns.contains(column)
            } else {
                qualifier == [oracle_ident(&s.alias)]
                    || (!s.explicit_alias && qualifier == s.path.as_slice())
            }
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [index] => Ok(Some(*index)),
        [] if !required => Ok(None),
        [] => Err(unsupported(
            "legacy outer join column has no resolved source",
        )),
        _ => Err(SqlError::data_exception(
            "42702",
            "ambiguous legacy outer join column",
            None,
        )),
    }
}

fn conjunctions(expr: Expr) -> Vec<Expr> {
    let mut stack = vec![expr];
    let mut result = Vec::new();
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                stack.push(*right);
                stack.push(*left);
            }
            Expr::Nested(inner)
                if matches!(
                    inner.as_ref(),
                    Expr::BinaryOp {
                        op: BinaryOperator::And,
                        ..
                    }
                ) =>
            {
                stack.push(*inner)
            }
            expr => result.push(expr),
        }
    }
    result
}

fn and(expressions: Vec<Expr>) -> Option<Expr> {
    expressions
        .into_iter()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        })
}

fn query(db: &BicDb, query: &mut Query, namespace: Option<&str>) -> Result<()> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    let Some(selection) = select.selection.as_ref().filter(|expr| marked(*expr)) else {
        return Ok(());
    };
    if select.from.iter().any(|from| !from.joins.is_empty()) {
        return Err(unsupported(
            "legacy (+) and ANSI join syntax cannot share a query block",
        ));
    }
    let sources = select
        .from
        .iter()
        .map(|from| source(db, &from.relation, namespace, query))
        .collect::<Result<Vec<_>>>()?;
    let mut aliases = BTreeSet::new();
    if sources
        .iter()
        .any(|source| !aliases.insert(oracle_ident(&source.alias)))
    {
        return Err(SqlError::data_exception(
            "42712",
            "duplicate legacy outer join source alias",
            None,
        ));
    }
    let mut optional = BTreeSet::new();
    let mut conditions: BTreeMap<usize, Vec<Expr>> = BTreeMap::new();
    let mut dependencies: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    let mut remaining = Vec::new();
    for predicate in conjunctions(selection.clone()) {
        if !marked(&predicate) {
            remaining.push(predicate);
            continue;
        }
        let mut targets = BTreeSet::new();
        let mut references = BTreeSet::new();
        let result = sqlparser::ast::visit_expressions(&predicate, |expr| {
            let resolved = match expr {
                Expr::OuterJoin(inner) => {
                    let parts = match inner.as_ref() {
                        Expr::Identifier(id) => std::slice::from_ref(id),
                        Expr::CompoundIdentifier(parts) => parts.as_slice(),
                        _ => return ControlFlow::Break(unsupported("(+) must mark a column")),
                    };
                    match owner(&sources, parts, true) {
                        Ok(Some(index)) => {
                            targets.insert(index);
                            Ok(Some(index))
                        }
                        error => error,
                    }
                }
                Expr::Identifier(id) => owner(&sources, std::slice::from_ref(id), false),
                Expr::CompoundIdentifier(parts) => owner(&sources, parts, false),
                Expr::BinaryOp {
                    op: BinaryOperator::Or,
                    ..
                }
                | Expr::InList { .. } => {
                    return ControlFlow::Break(unsupported(
                        "legacy (+) predicates with OR or IN are not supported",
                    ));
                }
                _ => Ok(None),
            };
            match resolved {
                Ok(Some(index)) => {
                    references.insert(index);
                    ControlFlow::Continue(())
                }
                Ok(None) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        });
        if let ControlFlow::Break(error) = result {
            return Err(error);
        }
        let target = match targets.iter().copied().collect::<Vec<_>>().as_slice() {
            [target] => *target,
            _ => {
                return Err(unsupported(
                    "one predicate cannot mark more than one outer join source",
                ))
            }
        };
        references.remove(&target);
        dependencies.entry(target).or_default().extend(references);
        optional.insert(target);
        let mut predicate = predicate;
        let _ = visit_expressions_mut(&mut predicate, |expr| {
            if let Expr::OuterJoin(inner) = expr {
                *expr = *inner.clone();
            }
            ControlFlow::<()>::Continue(())
        });
        conditions.entry(target).or_default().push(predicate);
    }
    let mut mandatory = sources
        .iter()
        .enumerate()
        .filter(|(i, _)| !optional.contains(i));
    let Some((first, first_source)) = mandatory.next() else {
        return Err(unsupported("legacy outer join requires a preserved source"));
    };
    let mut joined = BTreeSet::from([first]);
    let mut from = TableWithJoins {
        relation: first_source.factor.clone(),
        joins: Vec::new(),
    };
    for (index, source) in mandatory {
        from.joins.push(sqlparser::ast::Join {
            relation: source.factor.clone(),
            global: false,
            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
        });
        joined.insert(index);
    }
    while !optional.is_empty() {
        let Some(index) = optional.iter().copied().find(|index| {
            dependencies
                .get(index)
                .is_none_or(|deps| deps.is_subset(&joined))
        }) else {
            return Err(unsupported("cyclic legacy outer join dependencies"));
        };
        let condition =
            and(conditions.remove(&index).unwrap_or_default()).expect("marked join predicate");
        from.joins.push(sqlparser::ast::Join {
            relation: sources[index].factor.clone(),
            global: false,
            join_operator: JoinOperator::LeftOuter(JoinConstraint::On(condition)),
        });
        joined.insert(index);
        optional.remove(&index);
    }
    let SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        if let SelectItem::Wildcard(options) = item {
            if options != sqlparser::ast::WildcardAdditionalOptions::default() {
                return Err(unsupported(
                    "legacy outer join wildcard modifiers are not supported",
                ));
            }
            for source in &sources {
                projection.push(SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(
                        vec![source.alias.clone()],
                    )),
                    options.clone(),
                ));
            }
        } else {
            projection.push(item);
        }
    }
    select.projection = projection;
    select.from = vec![from];
    select.selection = and(remaining);
    Ok(())
}

pub(super) fn lower<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    namespace: Option<&str>,
) -> Result<()> {
    struct Lower<'a> {
        db: &'a BicDb,
        namespace: Option<&'a str>,
    }
    impl sqlparser::ast::VisitorMut for Lower<'_> {
        type Break = SqlError;
        fn post_visit_query(&mut self, node: &mut Query) -> ControlFlow<SqlError> {
            match query(self.db, node, self.namespace) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }
    if let ControlFlow::Break(error) = node.visit(&mut Lower { db, namespace }) {
        return Err(error);
    }
    let mut remaining = false;
    let _ = visit_expressions_mut(node, |expr| {
        remaining |= matches!(expr, Expr::OuterJoin(_));
        ControlFlow::<()>::Continue(())
    });
    if remaining {
        return Err(unsupported(
            "(+) is supported only in conjunctive WHERE predicates",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_profile_sql(session.db_ref(), sql, ORACLE_PREVIEW_PROFILE)?;
        session.execute(&sql)
    }

    #[test]
    fn grammar_outer_join_null_extension_filters_dependencies_and_star_order() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE parent_rows(k NUMBER, value NUMBER)",
            "CREATE TABLE child_rows(parent_k NUMBER, v NUMBER)",
            "CREATE TABLE detail_rows(child_v NUMBER, label VARCHAR2(10))",
            "INSERT INTO parent_rows VALUES(1,10),(2,20),(3,30)",
            "INSERT INTO child_rows VALUES(1,100),(1,101),(2,200)",
            "INSERT INTO detail_rows VALUES(100,'one')",
        ] {
            run(&mut session, sql).unwrap();
        }
        for (sql, expected) in [
            ("SELECT p.k,c.v FROM parent_rows p,child_rows c WHERE p.k=c.parent_k(+) ORDER BY p.k,c.v",vec![vec!["1","100"],vec!["1","101"],vec!["2","200"],vec!["3",""]]),
            ("SELECT p.k,c.v FROM parent_rows p,child_rows c WHERE p.k=c.parent_k(+) AND c.v(+)=100 ORDER BY p.k",vec![vec!["1","100"],vec!["2",""],vec!["3",""]]),
            ("SELECT p.k,c.v FROM parent_rows p,child_rows c WHERE p.k=c.parent_k(+) AND c.v=100 ORDER BY p.k",vec![vec!["1","100"]]),
            ("SELECT k,v FROM parent_rows,child_rows WHERE k=parent_k(+) ORDER BY k,v",vec![vec!["1","100"],vec!["1","101"],vec!["2","200"],vec!["3",""]]),
            ("SELECT p.k,d.label FROM detail_rows d,child_rows c,parent_rows p WHERE p.k=c.parent_k(+) AND c.v=d.child_v(+) ORDER BY p.k,c.v",vec![vec!["1","one"],vec!["1",""],vec!["2",""],vec!["3",""]]),
            ("SELECT * FROM child_rows c,parent_rows p WHERE p.k=c.parent_k(+) AND p.k=3",vec![vec!["","","3","30"]]),
            ("SELECT p.value,c.v FROM parent_rows p,child_rows c WHERE p.value=c.v(+) ORDER BY p.value",vec![vec!["10",""],vec!["20",""],vec!["30",""]]),
        ] {
            let rows=run(&mut session,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));
            let rows=rows.rows.iter().map(|r|r.iter().map(SqlValue::to_cell).collect::<Vec<_>>()).collect::<Vec<_>>();
            assert_eq!(rows,expected,"{sql}");
        }
    }

    #[test]
    fn grammar_outer_join_rejections_do_not_enable_native_syntax() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE a(k NUMBER)", "CREATE TABLE b(k NUMBER)"] {
            run(&mut session, sql).unwrap();
        }
        for sql in [
            "SELECT a.k FROM a,b WHERE a.k(+)=b.k(+)",
            "SELECT a.k FROM a JOIN b ON a.k=b.k WHERE a.k=b.k(+)",
            "SELECT a.k FROM a,b WHERE a.k=b.k(+) OR a.k=2",
            "SELECT a.k FROM a,b WHERE a.k=b.k(+) AND b.k=a.k(+)",
            "SELECT a.k FROM a,b WHERE a.k=k(+)",
        ] {
            assert!(run(&mut session, sql).is_err(), "{sql}");
        }
        assert!(parse_statements("SELECT a.k FROM a,b WHERE a.k=b.k(+)").is_err());
        assert!(!super::super::syntax::enabled());
    }

    #[test]
    fn grammar_cursor_mixed_case_loops_and_multiple_column_locks_execute() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE parent_rows(k NUMBER PRIMARY KEY, v NUMBER)",
            "CREATE TABLE child_rows(parent_k NUMBER, v NUMBER)",
            "INSERT INTO parent_rows VALUES(1,10),(2,20)",
            "INSERT INTO child_rows VALUES(1,5)",
            "CREATE FUNCTION cursor_total RETURN NUMBER IS total NUMBER:=0; CURSOR rows_to_read IS SELECT p.v pv,c.v cv FROM parent_rows p,child_rows c WHERE p.k=c.parent_k(+); BEGIN For row_value IN rows_to_read LOOP total:=total+row_value.pv+NVL(row_value.cv,0); EnD\n LoOp; RETURN total; END;",
            "CREATE PROCEDURE locked_values(p_key NUMBER,p_left OUT NUMBER,p_right OUT NUMBER) IS BEGIN SELECT k,v INTO p_left,p_right FROM parent_rows WHERE k=p_key FOR UPDATE OF k,v; END;",
            "CREATE TABLE keyword_lock(k NUMBER PRIMARY KEY,value NUMBER)",
            "INSERT INTO keyword_lock VALUES(1,42)",
            "CREATE PROCEDURE locked_keyword(p_value OUT NUMBER) IS BEGIN SELECT value INTO p_value FROM keyword_lock FOR UPDATE OF k,value NOWAIT; END;",
        ] {run(&mut session,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert_eq!(
            run(&mut session, "SELECT cursor_total() FROM DUAL")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "35"
        );
        let rows = run(&mut session, "CALL locked_values(2,NULL,NULL)")
            .unwrap()
            .rows;
        assert_eq!(
            rows[0].iter().map(SqlValue::to_cell).collect::<Vec<_>>(),
            vec!["2", "20"]
        );
        assert_eq!(
            run(&mut session, "CALL locked_keyword(NULL)").unwrap().rows[0][0].to_cell(),
            "42"
        );
        for sql in [
            "DELETE parent_rows WHERE k=2",
            "DELETE FROM child_rows WHERE parent_k=1",
        ] {
            run(&mut session, sql).unwrap();
        }
        assert_eq!(
            run(&mut session, "SELECT COUNT(*) FROM parent_rows")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
    }
}
