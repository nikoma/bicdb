//! Predicate propagation through ordinary view projections.
use super::*;
use sqlparser::ast::{Visit, Visitor};
use std::ops::ControlFlow;

fn constant(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Nested(expr) => constant(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => constant(expr),
        Expr::Cast {
            expr, data_type, ..
        } => {
            if matches!(data_type, DataType::Custom(..)) {
                return false;
            }
            let name = data_type.to_string().to_ascii_lowercase();
            let builtin = [
                "int2",
                "int4",
                "int8",
                "float4",
                "float8",
                "bpchar",
                "bool",
                "smallint",
                "integer",
                "int",
                "bigint",
                "numeric",
                "decimal",
                "real",
                "double precision",
                "boolean",
                "text",
                "varchar",
                "character varying",
                "character",
                "char",
                "date",
                "timestamp",
            ]
            .iter()
            .any(|base| name == *base || name.starts_with(&format!("{base}(")));
            builtin && constant(expr)
        }
        _ => false,
    }
}

fn passthrough(expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Some(expr.clone()),
        Expr::Nested(expr) => passthrough(expr),
        _ => None,
    }
}

fn source_column(
    expr: &Expr,
    alias: &str,
    columns: &[(String, Expr)],
    unqualified: bool,
) -> Option<Expr> {
    let name = match expr {
        Expr::Identifier(ident) if unqualified => ident_value(ident),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let owner = parts[..parts.len() - 1]
                .iter()
                .map(ident_value)
                .collect::<Vec<_>>()
                .join(".");
            if owner != alias {
                return None;
            }
            ident_value(parts.last()?)
        }
        Expr::Nested(expr) => return source_column(expr, alias, columns, unqualified),
        _ => return None,
    };
    let mut matches = columns.iter().filter(|(column, _)| *column == name);
    let (_, source) = matches.next()?;
    matches.next().is_none().then(|| source.clone())
}

fn predicate(
    expr: &Expr,
    alias: &str,
    columns: &[(String, Expr)],
    unqualified: bool,
) -> Option<Expr> {
    match expr {
        Expr::Nested(expr) => predicate(expr, alias, columns, unqualified),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            match (
                predicate(left, alias, columns, unqualified),
                predicate(right, alias, columns, unqualified),
            ) {
                (Some(left), Some(right)) => Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                }),
                (left, right) => left.or(right),
            }
        }
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
            ) =>
        {
            if constant(right) {
                Some(Expr::BinaryOp {
                    left: Box::new(source_column(left, alias, columns, unqualified)?),
                    op: op.clone(),
                    right: right.clone(),
                })
            } else if constant(left) {
                Some(Expr::BinaryOp {
                    left: left.clone(),
                    op: op.clone(),
                    right: Box::new(source_column(right, alias, columns, unqualified)?),
                })
            } else {
                None
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } if list.iter().all(constant) => Some(Expr::InList {
            expr: Box::new(source_column(expr, alias, columns, unqualified)?),
            list: list.clone(),
            negated: *negated,
        }),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } if constant(low) && constant(high) => Some(Expr::Between {
            expr: Box::new(source_column(expr, alias, columns, unqualified)?),
            negated: *negated,
            low: low.clone(),
            high: high.clone(),
        }),
        Expr::IsNotNull(expr) => Some(Expr::IsNotNull(Box::new(source_column(
            expr,
            alias,
            columns,
            unqualified,
        )?))),
        // IS NULL and null-safe comparisons are not null-rejecting: pushing
        // them into the nullable input of an outer join can manufacture rows.
        _ => None,
    }
}

pub(super) fn propagate(
    db: &BicDb,
    query: &mut Query,
    view: &ViewSchema,
    alias: &str,
    table_alias: Option<&sqlparser::ast::TableAlias>,
    unqualified: bool,
    selection: Option<&Expr>,
) -> Result<()> {
    let Some(selection) = selection else {
        return Ok(());
    };
    if view.materialized
        || view.security_barrier
        || query.with.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || query.order_by.is_some()
        || !query.locks.is_empty()
        || !row_eval::subquery_is_memoizable(db, query)
    {
        return Ok(());
    }
    // Nested queries have their own binding, cardinality and effect boundaries.
    struct NoNestedQuery(bool);
    impl Visitor for NoNestedQuery {
        type Break = ();
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
            if self.0 {
                ControlFlow::Break(())
            } else {
                self.0 = true;
                ControlFlow::Continue(())
            }
        }
    }
    if query.visit(&mut NoNestedQuery(false)).is_break() {
        return Ok(());
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.having.is_some()
        || select.qualify.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || !query_group_aggregate_functions(select, query)?.is_empty()
        || select_has_window_functions(select, query)?
        || select.projection.len() != view.columns.len()
    {
        return Ok(());
    }
    let names = table_alias_columns(
        &view.name,
        alias,
        table_alias
            .map(|alias| alias.columns.as_slice())
            .unwrap_or(&[]),
        &view
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
    )?;
    let names = if let Some(table_alias) = table_alias.filter(|alias| !alias.columns.is_empty()) {
        table_alias
            .columns
            .iter()
            .map(|column| ident_value(&column.name))
            .collect::<Vec<_>>()
    } else {
        names
    };
    let normalized_alias = table_alias
        .map(|alias| ident_value(&alias.name))
        .unwrap_or_else(|| alias.to_string());
    if names
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != names.len()
    {
        return Ok(());
    }
    let columns = names
        .into_iter()
        .zip(&select.projection)
        .filter_map(|(name, item)| {
            let source = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    passthrough(expr)?
                }
                _ => return None,
            };
            Some((name, source))
        })
        .collect::<Vec<_>>();
    let Some(pushed) = predicate(selection, &normalized_alias, &columns, unqualified) else {
        return Ok(());
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(existing))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(pushed))),
        },
        None => pushed,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn execute(db: &mut BicDb, sql: &str, profile: &str) -> Result<SqlResult> {
        let sql = if oracle::is_oracle_profile(profile) {
            oracle::lower_session_sql(db, sql, profile, None, "bicdb")?
        } else {
            sql.to_string()
        };
        SqlSession::new(db).execute(&sql)
    }

    #[test]
    fn ordinary_union_views_derive_typed_metadata_without_executing_their_definitions() {
        for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut db = BicDb::open(root.path()).unwrap();
                oracle::ensure_database_profile(&mut db, profile).unwrap();
                for sql in [
                    "CREATE TABLE metadata_source(id INTEGER,v NUMERIC(10,2))",
                    "INSERT INTO metadata_source VALUES(1,1.25)",
                    "CREATE VIEW union_metadata(\"RowKey\",\"Amount\") AS SELECT id,v FROM metadata_source UNION ALL SELECT id+1,v+1 FROM metadata_source",
                    "CREATE VIEW deferred_error AS SELECT id,CAST(1 AS NUMERIC)/0 AS value FROM metadata_source UNION ALL SELECT id,v FROM metadata_source",
                ] { execute(&mut db,sql,profile).unwrap(); }
            }
            let mut db = BicDb::open(root.path()).unwrap();
            let rows =
                execute(&mut db, "SELECT * FROM union_metadata ORDER BY 1", profile).unwrap();
            assert_eq!(rows.columns, vec!["RowKey", "Amount"]);
            assert_eq!(
                rows.rows
                    .iter()
                    .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                vec![
                    vec!["1".to_string(), "1.25".to_string()],
                    vec!["2".to_string(), "2.25".to_string()]
                ]
            );
            assert_eq!(rows.column_types[1], Some("numeric".into()));
            let error = execute(&mut db, "SELECT * FROM deferred_error", profile).unwrap_err();
            assert!(
                error.to_string().contains("division by zero"),
                "{profile}: {error}"
            );
        }
    }

    #[test]
    fn joined_scalar_subqueries_keep_declared_types_with_empty_correlations_and_reopen() {
        for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
            let root = tempfile::tempdir().unwrap();
            {
                let mut db = BicDb::open(root.path()).unwrap();
                oracle::ensure_database_profile(&mut db, profile).unwrap();
                for sql in [
                    "CREATE TABLE scalar_outer(id INTEGER)",
                    "CREATE TABLE scalar_versions(id INTEGER)",
                    "CREATE TABLE scalar_values(version_id INTEGER,owner_id INTEGER,amount NUMERIC(10,2),seen TIMESTAMP,marker VARCHAR(8))",
                    "INSERT INTO scalar_outer VALUES(1),(2)",
                    "INSERT INTO scalar_versions VALUES(11)",
                    "INSERT INTO scalar_values VALUES(11,1,12.50,TIMESTAMP '2024-02-29 12:34:56','match')",
                ] {execute(&mut db,sql,profile).unwrap();}
                let inner="FROM scalar_versions v JOIN scalar_values x ON v.id=x.version_id WHERE x.owner_id=o.id";
                let sql=format!("CREATE VIEW scalar_metadata AS SELECT o.id,(SELECT x.amount {inner}) AS amount,(SELECT x.seen {inner}) AS seen,(SELECT x.marker {inner}) AS marker FROM scalar_outer o");
                execute(&mut db, &sql, profile).unwrap();
            }
            let mut db = BicDb::open(root.path()).unwrap();
            let result =
                execute(&mut db, "SELECT * FROM scalar_metadata ORDER BY 1", profile).unwrap();
            assert_eq!(
                &result.column_types[1..],
                &[
                    Some("numeric".into()),
                    Some("timestamp".into()),
                    Some("varchar".into())
                ]
            );
            assert_eq!(result.rows[0][1].to_cell(), "12.50");
            assert_eq!(result.rows[0][2].to_cell(), "2024-02-29 12:34:56");
            assert_eq!(result.rows[0][3].to_cell(), "match");
            assert_eq!(
                &result.rows[1][1..],
                &[SqlValue::Null, SqlValue::Null, SqlValue::Null]
            );
            let empty =
                execute(&mut db, "SELECT * FROM scalar_metadata WHERE 1=0", profile).unwrap();
            assert!(empty.rows.is_empty());
            assert_eq!(empty.column_types, result.column_types);
        }
    }

    fn fixture(profile: &str) -> (tempfile::TempDir, BicDb) {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path()).unwrap();
        oracle::ensure_database_profile(&mut db, profile).unwrap();
        for sql in [
            "CREATE TABLE fields(id INT PRIMARY KEY, owner_key INT, label VARCHAR(40), active VARCHAR(1))",
            "CREATE TABLE fallbacks(id INT PRIMARY KEY, label VARCHAR(40))",
            "CREATE INDEX fields_owner ON fields(owner_key)",
        ] { execute(&mut db, sql, profile).unwrap(); }
        let fields = (1..=200)
            .map(|id| format!("({id},{},NULL,'Y')", (id - 1) / 10 + 1))
            .collect::<Vec<_>>()
            .join(",");
        execute(
            &mut db,
            &format!("INSERT INTO fields VALUES {fields}"),
            profile,
        )
        .unwrap();
        let defaults = (1..=20)
            .map(|id| format!("({id},'label {id}')"))
            .collect::<Vec<_>>()
            .join(",");
        execute(
            &mut db,
            &format!("INSERT INTO fallbacks VALUES {defaults}"),
            profile,
        )
        .unwrap();
        execute(&mut db, "CREATE VIEW lookup_view AS SELECT f.owner_key AS owner_key, f.id AS field_id, COALESCE(f.label,d.label) AS label FROM fields f LEFT JOIN fallbacks d ON f.owner_key=d.id WHERE f.active='Y'", profile).unwrap();
        (directory, db)
    }

    fn selective_join_view(profile: &str) {
        let (_directory, mut db) = fixture(profile);
        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let result = execute(
            &mut db,
            "SELECT field_id,label FROM lookup_view WHERE owner_key=7 ORDER BY field_id",
            profile,
        );
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
        let result = result.unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            (61..=70)
                .map(|id| vec![id.to_string(), "label 7".into()])
                .collect::<Vec<_>>()
        );
        eprintln!(
            "view_filter_profile={profile} rows_materialized={}",
            stats.rows_materialized
        );
        assert!(
            stats.rows_materialized < 150,
            "{profile}: selective view materialized {} rows",
            stats.rows_materialized
        );
    }

    #[test]
    fn postgres_selective_join_view_avoids_full_materialization() {
        selective_join_view(oracle::POSTGRES_PROFILE);
    }

    #[test]
    fn oracle_selective_join_view_avoids_full_materialization() {
        selective_join_view(oracle::ORACLE_PREVIEW_PROFILE);
    }

    fn ids(db: &mut BicDb, query: &str, profile: &str) -> Vec<String> {
        execute(db, query, profile)
            .unwrap()
            .rows
            .iter()
            .map(|row| row[0].to_cell())
            .collect()
    }

    fn comparison_matrix(profile: &str) {
        let (_directory, mut db) = fixture(profile);
        let ten = (61..=70).map(|x| x.to_string()).collect::<Vec<_>>();
        let twenty = (51..=70).map(|x| x.to_string()).collect::<Vec<_>>();
        for (predicate, expected) in [
            ("v.owner_key=7", ten.clone()),
            ("7=v.owner_key", ten.clone()),
            ("v.owner_key=CAST(7 AS INTEGER)", ten.clone()),
            ("v.owner_key BETWEEN 6 AND 7", twenty.clone()),
            ("v.owner_key IN (6,7)", twenty),
            ("v.owner_key IN (7,NULL)", ten.clone()),
            ("v.owner_key NOT IN (7,NULL)", vec![]),
            ("v.owner_key=NULL", vec![]),
            ("v.owner_key=7 AND v.label IS NULL", vec![]),
            (
                "v.owner_key=7 OR v.owner_key=8",
                (61..=80).map(|x| x.to_string()).collect(),
            ),
        ] {
            assert_eq!(
                ids(
                    &mut db,
                    &format!(
                        "SELECT field_id FROM lookup_view v WHERE {predicate} ORDER BY field_id"
                    ),
                    profile
                ),
                expected,
                "{profile}: {predicate}"
            );
        }
        execute(&mut db,"CREATE VIEW renamed(owner_alias,field_alias,label_alias) AS SELECT f.owner_key,f.id,COALESCE(f.label,d.label) FROM fields f LEFT JOIN fallbacks d ON f.owner_key=d.id",profile).unwrap();
        assert_eq!(
            ids(
                &mut db,
                "SELECT field_alias FROM renamed WHERE owner_alias=7 ORDER BY field_alias",
                profile
            ),
            ten
        );
        assert_eq!(ids(&mut db,"SELECT v.field_id FROM fallbacks d JOIN lookup_view v ON d.id=v.owner_key WHERE v.owner_key=7 ORDER BY v.field_id",profile),(61..=70).map(|x|x.to_string()).collect::<Vec<_>>());
        execute(
            &mut db,
            "INSERT INTO fallbacks VALUES (999,'missing')",
            profile,
        )
        .unwrap();
        assert_eq!(ids(&mut db,"SELECT d.id FROM fallbacks d LEFT JOIN lookup_view v ON d.id=v.field_id WHERE v.owner_key IS NULL ORDER BY d.id",profile),vec!["999"]);
    }

    #[test]
    fn postgres_view_predicates_preserve_aliases_nulls_and_joins() {
        comparison_matrix(oracle::POSTGRES_PROFILE);
    }
    #[test]
    fn oracle_view_predicates_preserve_aliases_nulls_and_joins() {
        comparison_matrix(oracle::ORACLE_PREVIEW_PROFILE);
    }

    #[test]
    fn view_column_aliases_keep_their_projection_positions() {
        let profile = oracle::POSTGRES_PROFILE;
        let (_directory, mut db) = fixture(profile);
        assert_eq!(
            ids(
                &mut db,
                "SELECT x FROM lookup_view v(k,x,t) WHERE v.k=7 ORDER BY x",
                profile
            ),
            (61..=70).map(|x| x.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(ids(&mut db,"SELECT vTag.field_id FROM lookup_view vTag WHERE vTag.owner_key=7 ORDER BY vTag.field_id",profile),(61..=70).map(|x|x.to_string()).collect::<Vec<_>>());
        assert_eq!(ids(&mut db,"SELECT v.fieldAlias FROM lookup_view v(keyAlias,fieldAlias,labelAlias) WHERE v.keyAlias=7 ORDER BY v.fieldAlias",profile),(61..=70).map(|x|x.to_string()).collect::<Vec<_>>());
        assert_eq!(
            ids(
                &mut db,
                r#"SELECT "Owner.Alias".field_id FROM lookup_view "Owner.Alias" WHERE "Owner.Alias".owner_key=7 ORDER BY "Owner.Alias".field_id"#,
                profile
            ),
            (61..=70).map(|x| x.to_string()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn view_limit_offset_and_window_boundaries_preserve_rows() {
        let p = oracle::POSTGRES_PROFILE;
        let (_directory, mut db) = fixture(p);
        execute(
            &mut db,
            "CREATE VIEW first_rows AS SELECT id FROM fields LIMIT 5",
            p,
        )
        .unwrap();
        let first = ids(&mut db, "SELECT id FROM first_rows", p);
        let excluded = (1..=200)
            .map(|x| x.to_string())
            .find(|x| !first.contains(x))
            .unwrap();
        assert!(ids(
            &mut db,
            &format!("SELECT id FROM first_rows WHERE id={excluded}"),
            p
        )
        .is_empty());
        execute(
            &mut db,
            "CREATE VIEW later_rows AS SELECT id FROM fields OFFSET 5",
            p,
        )
        .unwrap();
        let chosen = ids(&mut db, "SELECT id FROM later_rows", p)[0].clone();
        assert_eq!(
            ids(
                &mut db,
                &format!("SELECT id FROM later_rows WHERE id={chosen}"),
                p
            ),
            vec![chosen]
        );
        execute(&mut db,"CREATE VIEW numbered_rows AS SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS ordinal FROM fields",p).unwrap();
        assert_eq!(
            ids(&mut db, "SELECT ordinal FROM numbered_rows WHERE id=10", p),
            vec!["10"]
        );
        execute(&mut db,"CREATE VIEW grouped_rows AS SELECT owner_key,COUNT(*) AS amount FROM fields GROUP BY owner_key",p).unwrap();
        assert_eq!(
            ids(
                &mut db,
                "SELECT amount FROM grouped_rows WHERE owner_key=7",
                p
            ),
            vec!["10"]
        );
    }

    #[test]
    fn security_barrier_and_opaque_functions_keep_existing_boundaries() {
        let p = oracle::POSTGRES_PROFILE;
        let (_directory, mut db) = fixture(p);
        execute(&mut db,"CREATE VIEW barrier_rows WITH (security_barrier=true) AS SELECT id,owner_key FROM fields",p).unwrap();
        SQL_PROFILE_STACK.with(|s| s.borrow_mut().push(SqlProfileStats::default()));
        let result = execute(&mut db, "SELECT id FROM barrier_rows WHERE owner_key=7", p);
        let stats = SQL_PROFILE_STACK.with(|s| s.borrow_mut().pop().unwrap());
        assert_eq!(result.unwrap().rows.len(), 10);
        assert!(stats.rows_materialized >= 200);
        execute(&mut db,"CREATE FUNCTION echo_field(n INT) RETURNS INT LANGUAGE sql IMMUTABLE AS $$ SELECT n $$",p).unwrap();
        execute(
            &mut db,
            "CREATE VIEW opaque_rows AS SELECT id,echo_field(owner_key) AS observed FROM fields",
            p,
        )
        .unwrap();
        SQL_PROFILE_STACK.with(|s| s.borrow_mut().push(SqlProfileStats::default()));
        let result = execute(&mut db, "SELECT id FROM opaque_rows WHERE id=7", p);
        let stats = SQL_PROFILE_STACK.with(|s| s.borrow_mut().pop().unwrap());
        assert_eq!(result.unwrap().rows.len(), 1);
        assert!(stats.rows_materialized >= 200);
    }
}
