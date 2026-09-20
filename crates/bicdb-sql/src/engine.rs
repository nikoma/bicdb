//! Query planning and execution engine: QueryPlan/PlanKind, index bound matching, record locators, and the SqlEngine implementation (SELECT/JOIN/CTE execution, index selection, row materialization, subquery evaluation, catalog view materialization).
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use engine::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;
mod joins_windows;
pub(crate) mod materialize;
mod pg_catalog;
mod predicates_locators;
mod records_plan;
mod row_eval;
mod row_locks;
mod scan_fts;

/// Rows resolved per batch by the streaming scan path.
///
/// Bounds peak memory to one batch of decoded records. Large enough that the
/// per-batch key re-scan is amortized, small enough that a wide row set cannot
/// push the process past its buffer-pool envelope.
const STREAMING_SCAN_BATCH: usize = 1024;

#[derive(Clone, Debug)]
struct RelationColumnMetadata {
    alias: String,
    columns: Vec<(String, SqlColumnMetadata)>,
}

/// Resolve PostgreSQL parameter types from statement, schema, and expression
/// context. Entries remain `None` only when PostgreSQL's unknown-input rules
/// have no stronger context; the wire layer then applies the protocol default.
pub fn infer_parameter_types(db: &BicDb, sql: &str) -> Result<Vec<Option<String>>> {
    let parse_sql = rewrite_postgres_parse_compat(sql);
    let sql_for_parse = parse_sql.as_deref().unwrap_or(sql);
    reject_oversized_sql(sql_for_parse)?;
    reject_deep_parse_nesting(sql_for_parse)?;
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql_for_parse)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let engine = SqlEngine::new(db);
    let mut resolver = ParameterTypeResolver {
        engine: &engine,
        types: Vec::new(),
        ctes: BTreeMap::new(),
    };
    for statement in &statements {
        resolver.statement(statement);
    }
    Ok(resolver.types)
}

/// Resolve a query's logical PostgreSQL output types without executing it.
/// Extended-protocol Describe uses this when its null binds produce no rows.
pub fn infer_query_result_types(db: &BicDb, sql: &str) -> Result<Option<Vec<Option<String>>>> {
    Ok(infer_query_result_columns(db, sql)?
        .map(|columns| columns.into_iter().map(|(_, pg_type)| pg_type).collect()))
}

/// Resolve a query's logical PostgreSQL output names and types without
/// executing it. Protocol Describe must prefer this path because a SELECT can
/// invoke a volatile or write-capable stored function.
pub fn infer_query_result_columns(
    db: &BicDb,
    sql: &str,
) -> Result<Option<Vec<(String, Option<String>)>>> {
    let parse_sql = rewrite_postgres_parse_compat(sql);
    let sql_for_parse = parse_sql.as_deref().unwrap_or(sql);
    reject_oversized_sql(sql_for_parse)?;
    reject_deep_parse_nesting(sql_for_parse)?;
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql_for_parse)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    reject_deep_expressions(&statements)?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Ok(None);
    };
    let engine = SqlEngine::new(db);
    Ok(engine.query_output_columns(&query))
}

/// Resolve a projection that has no top-level FROM clause. This is the subset
/// protocol Describe can answer statically without risking mismatched wildcard
/// expansion against the executor's materialized row shape. Locking queries
/// (including locks in derived tables) also use metadata-only description:
/// Describe must never acquire a row lock or fail because another session
/// currently holds one.
pub fn infer_query_result_columns_without_from(
    db: &BicDb,
    sql: &str,
) -> Result<Option<Vec<(String, Option<String>)>>> {
    let parse_sql = rewrite_postgres_parse_compat(sql);
    let sql_for_parse = parse_sql.as_deref().unwrap_or(sql);
    reject_oversized_sql(sql_for_parse)?;
    reject_deep_parse_nesting(sql_for_parse)?;
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql_for_parse)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    reject_deep_expressions(&statements)?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.from.is_empty() && !query_has_row_locks(&query) {
        return Ok(None);
    }
    let engine = SqlEngine::new(db);
    Ok(engine.query_output_columns(&query))
}

struct ParameterTypeResolver<'engine, 'db> {
    engine: &'engine SqlEngine<'db>,
    types: Vec<Option<String>>,
    ctes: BTreeMap<String, Vec<(String, Option<String>)>>,
}

impl ParameterTypeResolver<'_, '_> {
    fn statement(&mut self, statement: &Statement) {
        match statement {
            Statement::Query(query) => self.query(query, None),
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::Call(function) => {
                self.function(function, &[], None);
            }
            _ => {}
        }
    }

    fn insert(&mut self, insert: &Insert) {
        let TableObject::TableName(table_name) = &insert.table else {
            return;
        };
        let Some(table) = relation_name(table_name).ok() else {
            return;
        };
        let Some((schema_columns, _)) = self.engine.relation_table_columns(&table, &[]) else {
            return;
        };
        let target_types = if insert.columns.is_empty() {
            schema_columns
                .iter()
                .map(|(_, pg_type)| pg_type.clone())
                .collect::<Vec<_>>()
        } else {
            insert
                .columns
                .iter()
                .map(|column| {
                    let column = object_name(column).ok()?;
                    let column = column.rsplit('.').next()?;
                    schema_columns
                        .iter()
                        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
                        .and_then(|(_, pg_type)| pg_type.clone())
                })
                .collect()
        };
        if let Some(source) = &insert.source {
            self.query(source, Some(&target_types));
        }
        for assignment in &insert.assignments {
            let expected = assignment_target_type(&assignment.target, &schema_columns);
            self.expr(&assignment.value, &[], expected.as_deref());
        }
        let relation_alias = insert
            .table_alias
            .as_ref()
            .map(|alias| alias.alias.value.to_ascii_lowercase())
            .unwrap_or_else(|| {
                table
                    .rsplit('.')
                    .next()
                    .unwrap_or(&table)
                    .to_ascii_lowercase()
            });
        let env = vec![
            RelationColumns {
                alias: relation_alias,
                row_type: None,
                columns: schema_columns.clone(),
            },
            RelationColumns {
                alias: "excluded".to_string(),
                row_type: None,
                columns: schema_columns.clone(),
            },
        ];
        if let Some(on_insert) = &insert.on {
            match on_insert {
                OnInsert::DuplicateKeyUpdate(assignments) => {
                    self.assignments(assignments, &schema_columns, &env);
                }
                OnInsert::OnConflict(conflict) => {
                    if let OnConflictAction::DoUpdate(update) = &conflict.action {
                        self.assignments(&update.assignments, &schema_columns, &env);
                        if let Some(selection) = &update.selection {
                            self.expr(selection, &env, Some("bool"));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn assignments(
        &mut self,
        assignments: &[Assignment],
        columns: &[(String, Option<String>)],
        env: &[RelationColumns],
    ) {
        for assignment in assignments {
            let expected = assignment_target_type(&assignment.target, columns);
            self.expr(&assignment.value, env, expected.as_deref());
        }
    }

    fn update(&mut self, update: &sqlparser::ast::Update) {
        let env = self
            .engine
            .from_relation_columns(std::slice::from_ref(&update.table))
            .unwrap_or_default();
        for assignment in &update.assignments {
            let expected = assignment_target_type(&assignment.target, &flatten_relation_env(&env));
            self.expr(&assignment.value, &env, expected.as_deref());
        }
        if let Some(selection) = &update.selection {
            self.expr(selection, &env, Some("bool"));
        }
        if let Some(limit) = &update.limit {
            self.expr(limit, &env, Some("int8"));
        }
    }

    fn delete(&mut self, delete: &Delete) {
        let from = match &delete.from {
            FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
        };
        let mut env = self.engine.from_relation_columns(from).unwrap_or_default();
        if let Some(using) = &delete.using {
            for table in using {
                self.table_factor(&table.relation);
                for join in &table.joins {
                    self.table_factor(&join.relation);
                    if let Some(JoinConstraint::On(expr)) = join_constraint(&join.join_operator) {
                        self.expr(expr, &env, Some("bool"));
                    }
                }
            }
            env.extend(self.engine.from_relation_columns(using).unwrap_or_default());
        }
        if let Some(selection) = &delete.selection {
            self.expr(selection, &env, Some("bool"));
        }
        if let Some(limit) = &delete.limit {
            self.expr(limit, &env, Some("int8"));
        }
    }

    fn query(&mut self, query: &Query, expected: Option<&[Option<String>]>) {
        let outer_ctes = self.ctes.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                if let Some(columns) = self.cte_output_columns(cte) {
                    self.ctes.insert(cte_key(&cte.alias.name.value), columns);
                }
                self.query(&cte.query, None);
            }
        }
        self.set_expr(query.body.as_ref(), expected);
        if let Some(limit_clause) = &query.limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(limit) = limit {
                        self.expr(limit, &[], Some("int8"));
                    }
                    if let Some(offset) = offset {
                        self.expr(&offset.value, &[], Some("int8"));
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    self.expr(offset, &[], Some("int8"));
                    self.expr(limit, &[], Some("int8"));
                }
            }
        }
        self.ctes = outer_ctes;
    }

    fn cte_output_columns(
        &self,
        cte: &sqlparser::ast::Cte,
    ) -> Option<Vec<(String, Option<String>)>> {
        let anchor = match cte.query.body.as_ref() {
            SetExpr::SetOperation {
                left,
                op: SetOperator::Union,
                ..
            } => left.as_ref(),
            body => body,
        };
        let columns = self.engine.set_expr_output_columns(anchor)?;
        rename_relation_columns(columns, &cte.alias.columns)
    }

    fn from_relation_columns(&self, from: &[TableWithJoins]) -> Vec<RelationColumns> {
        let mut env = Vec::new();
        for table in from {
            self.collect_relation_columns(&table.relation, &mut env);
            for join in &table.joins {
                self.collect_relation_columns(&join.relation, &mut env);
            }
        }
        env
    }

    fn collect_relation_columns(&self, factor: &TableFactor, env: &mut Vec<RelationColumns>) {
        if let Some(call) = json_set_returning_call(factor).ok().flatten() {
            if let Ok((alias, columns)) = json_set_function_columns(&call) {
                let Ok(output_types) = json_set_function_output_pg_types(&call) else {
                    return;
                };
                let mut columns = columns
                    .into_iter()
                    .zip(output_types)
                    .map(|(column, pg_type)| (column.to_ascii_lowercase(), Some(pg_type)))
                    .collect::<Vec<_>>();
                if call
                    .alias
                    .as_ref()
                    .is_some_and(|alias| alias.columns.is_empty())
                    && call.function.projection_supported()
                    && call.function.output_pg_types().len() == 1
                    && !columns
                        .iter()
                        .any(|(column, _)| column == &alias.to_ascii_lowercase())
                {
                    columns.push((
                        alias.to_ascii_lowercase(),
                        Some(call.function.pg_type().to_string()),
                    ));
                }
                env.push(RelationColumns {
                    alias: alias.to_ascii_lowercase(),
                    row_type: None,
                    columns,
                });
                return;
            }
        }
        if let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = factor
        {
            if let Ok(table) = relation_name(name) {
                if let Some(columns) = self.ctes.get(&cte_key(&table)) {
                    let alias_name = alias
                        .as_ref()
                        .map(|alias| alias.name.value.clone())
                        .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(&table).to_string());
                    let alias_columns = alias
                        .as_ref()
                        .map(|alias| alias.columns.as_slice())
                        .unwrap_or(&[]);
                    if let Some(columns) = rename_relation_columns(columns.clone(), alias_columns) {
                        env.push(RelationColumns {
                            alias: alias_name.to_ascii_lowercase(),
                            row_type: None,
                            columns,
                        });
                    }
                    return;
                }
            }
        }
        let _ = self.engine.collect_relation_columns(factor, env);
    }

    fn set_expr(&mut self, set_expr: &SetExpr, expected: Option<&[Option<String>]>) {
        match set_expr {
            SetExpr::Select(select) => self.select(select, expected),
            SetExpr::Query(query) => self.query(query, expected),
            SetExpr::SetOperation { left, right, .. } => {
                let resolved = self
                    .engine
                    .set_expr_output_columns(set_expr)
                    .map(|columns| {
                        columns
                            .into_iter()
                            .map(|(_, pg_type)| pg_type)
                            .collect::<Vec<_>>()
                    });
                let expected = resolved.as_deref().or(expected);
                self.set_expr(left, expected);
                self.set_expr(right, expected);
            }
            SetExpr::Values(values) => {
                let resolved = self
                    .engine
                    .set_expr_output_columns(set_expr)
                    .map(|columns| {
                        columns
                            .into_iter()
                            .enumerate()
                            .map(|(index, (_, pg_type))| {
                                let has_concrete_source_type = values.rows.iter().any(|row| {
                                    row.get(index)
                                        .and_then(|expr| self.expr_type(expr, &[]))
                                        .is_some()
                                });
                                if has_concrete_source_type {
                                    pg_type
                                } else {
                                    expected
                                        .and_then(|expected| expected.get(index))
                                        .cloned()
                                        .flatten()
                                        .or(pg_type)
                                }
                            })
                            .collect::<Vec<_>>()
                    });
                let expected = resolved.as_deref().or(expected);
                for row in &values.rows {
                    for (idx, expr) in row.iter().enumerate() {
                        self.expr(
                            expr,
                            &[],
                            expected
                                .and_then(|expected| expected.get(idx))
                                .and_then(Option::as_deref),
                        );
                    }
                }
            }
            SetExpr::Insert(statement)
            | SetExpr::Update(statement)
            | SetExpr::Delete(statement)
            | SetExpr::Merge(statement) => self.statement(statement),
            SetExpr::Table(_) => {}
        }
    }

    fn select(&mut self, select: &Select, expected: Option<&[Option<String>]>) {
        let env = self.from_relation_columns(&select.from);
        for table in &select.from {
            self.table_factor(&table.relation);
            for join in &table.joins {
                self.table_factor(&join.relation);
                if let Some(constraint) = join_constraint(&join.join_operator) {
                    if let JoinConstraint::On(expr) = constraint {
                        self.expr(expr, &env, Some("bool"));
                    }
                }
            }
        }
        for (idx, item) in select.projection.iter().enumerate() {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            let expected = expected
                .and_then(|expected| expected.get(idx))
                .and_then(Option::as_deref);
            self.expr(expr, &env, expected);
        }
        if let Some(selection) = &select.selection {
            self.expr(selection, &env, Some("bool"));
        }
        if let Some(prewhere) = &select.prewhere {
            self.expr(prewhere, &env, Some("bool"));
        }
        if let Some(having) = &select.having {
            self.expr(having, &env, Some("bool"));
        }
        if let Some(qualify) = &select.qualify {
            self.expr(qualify, &env, Some("bool"));
        }
    }

    fn table_factor(&mut self, factor: &TableFactor) {
        match factor {
            TableFactor::Table {
                args: Some(args), ..
            } => {
                if let Ok(args) = table_function_expr_args(args) {
                    for arg in args {
                        self.expr(&arg, &[], None);
                    }
                }
            }
            TableFactor::TableFunction { expr, .. } => self.expr(expr, &[], None),
            TableFactor::Function { args, .. } => {
                for arg in args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                        self.expr(expr, &[], None);
                    }
                }
            }
            TableFactor::UNNEST { array_exprs, .. } => {
                for expr in array_exprs {
                    self.expr(expr, &[], None);
                }
            }
            TableFactor::Derived { subquery, .. } => self.query(subquery, None),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.table_factor(&table_with_joins.relation);
                for join in &table_with_joins.joins {
                    self.table_factor(&join.relation);
                }
            }
            _ => {}
        }
    }

    fn expr(&mut self, expr: &Expr, env: &[RelationColumns], expected: Option<&str>) {
        if let Some(index) = placeholder_index(expr) {
            self.assign(index, expected);
            return;
        }
        match expr {
            Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => {
                self.expr(inner, env, expected);
            }
            Expr::Cast {
                expr: inner,
                data_type,
                ..
            } => {
                let cast_type = pg_type_from_data_type(data_type)
                    .ok()
                    .map(|(pg_type, _)| pg_type);
                self.expr(inner, env, cast_type.as_deref().or(expected));
            }
            Expr::BinaryOp { left, op, right } => {
                let left_type = self.expr_type(left, env);
                let right_type = self.expr_type(right, env);
                let (left_expected, right_expected) =
                    binary_operand_types(op, left_type.as_deref(), right_type.as_deref(), expected);
                self.expr(left, env, left_expected.as_deref());
                self.expr(right, env, right_expected.as_deref());
            }
            Expr::UnaryOp { op, expr: inner } => {
                let op = op.to_string();
                let expected = if op.eq_ignore_ascii_case("NOT") {
                    Some("bool")
                } else {
                    expected
                };
                self.expr(inner, env, expected);
            }
            Expr::Trim {
                expr: inner,
                trim_what,
                trim_characters,
                ..
            } => {
                self.expr(inner, env, Some("text"));
                if let Some(trim_what) = trim_what {
                    self.expr(trim_what, env, Some("text"));
                }
                if let Some(trim_characters) = trim_characters {
                    for trim_character in trim_characters {
                        self.expr(trim_character, env, Some("text"));
                    }
                }
            }
            Expr::Function(function) => self.function(function, env, expected),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    self.expr(operand, env, None);
                }
                // PostgreSQL considers ELSE first, then WHEN results, and
                // resolves an all-unknown CASE to text.
                let mut branch_types = Vec::with_capacity(conditions.len() + 1);
                branch_types.push(
                    else_result
                        .as_deref()
                        .and_then(|result| self.expr_type(result, env)),
                );
                branch_types.extend(
                    conditions
                        .iter()
                        .map(|condition| self.expr_type(&condition.result, env)),
                );
                let branch_type =
                    select_common_pg_type(self.engine.db_ref(), &branch_types, "CASE")
                        .ok()
                        .or_else(|| expected.map(str::to_string));
                for condition in conditions {
                    self.expr(&condition.condition, env, Some("bool"));
                    self.expr(&condition.result, env, branch_type.as_deref());
                }
                if let Some(else_result) = else_result {
                    self.expr(else_result, env, branch_type.as_deref());
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let value_type = self.expr_type(expr, env);
                self.expr(expr, env, value_type.as_deref());
                self.expr(low, env, value_type.as_deref());
                self.expr(high, env, value_type.as_deref());
            }
            Expr::InList { expr, list, .. } => {
                let value_type = self.expr_type(expr, env);
                self.expr(expr, env, value_type.as_deref());
                for item in list {
                    self.expr(item, env, value_type.as_deref());
                }
            }
            Expr::Like { expr, pattern, .. }
            | Expr::ILike { expr, pattern, .. }
            | Expr::SimilarTo { expr, pattern, .. } => {
                self.expr(expr, env, Some("text"));
                self.expr(pattern, env, Some("text"));
            }
            Expr::IsNull(inner)
            | Expr::IsNotNull(inner)
            | Expr::IsTrue(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsNotFalse(inner) => self.expr(inner, env, None),
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                let left_type = self.expr_type(left, env);
                let right_type = self.expr_type(right, env);
                self.expr(left, env, right_type.as_deref());
                self.expr(right, env, left_type.as_deref());
            }
            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                let left_type = self.expr_type(left, env);
                let right_type = self.expr_type(right, env);
                let element_type = right_type
                    .as_deref()
                    .and_then(|pg_type| pg_type.strip_suffix("[]"));
                let array_type = right_type
                    .clone()
                    .or_else(|| left_type.as_ref().map(|pg_type| format!("{pg_type}[]")));
                self.expr(left, env, element_type.or(left_type.as_deref()));
                self.expr(right, env, array_type.as_deref());
            }
            Expr::Array(array) => {
                let element_type = expected
                    .and_then(array_element_pg_type)
                    .map(str::to_string)
                    .or_else(|| {
                        let element_types = array
                            .elem
                            .iter()
                            .map(|element| self.expr_type(element, env))
                            .collect::<Vec<_>>();
                        select_common_pg_type(self.engine.db_ref(), &element_types, "ARRAY").ok()
                    });
                for element in &array.elem {
                    self.expr(element, env, element_type.as_deref());
                }
            }
            Expr::Subquery(query) => self.query(query, None),
            Expr::Exists { subquery, .. } => self.query(subquery, None),
            _ => {}
        }
    }

    fn function(&mut self, function: &Function, env: &[RelationColumns], expected: Option<&str>) {
        let name = object_name(&function.name)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let args = function_args(function);
        let arg_types = args
            .iter()
            .map(|arg| self.expr_type(arg, env))
            .collect::<Vec<_>>();
        let common = select_common_pg_type(self.engine.db_ref(), &arg_types, "function")
            .ok()
            .or_else(|| expected.map(str::to_string));
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        if self.polymorphic_array_function(bare_name, &args, env, expected) {
            return;
        }
        if bare_name == "split_part" {
            for (index, arg) in args.iter().enumerate() {
                self.expr(arg, env, Some(if index == 2 { "int4" } else { "text" }));
            }
            return;
        }
        let argument_type = match bare_name {
            "pg_advisory_lock"
            | "pg_advisory_lock_shared"
            | "pg_try_advisory_lock"
            | "pg_try_advisory_lock_shared"
            | "pg_advisory_xact_lock"
            | "pg_advisory_xact_lock_shared"
            | "pg_try_advisory_xact_lock"
            | "pg_try_advisory_xact_lock_shared"
            | "pg_advisory_unlock"
            | "pg_advisory_unlock_shared" => {
                if args.len() == 2 {
                    Some("int4")
                } else {
                    Some("int8")
                }
            }
            "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "substr" | "substring" | "replace"
            | "initcap" | "md5" | "left" | "right" | "repeat" | "reverse" | "length"
            | "char_length" | "character_length" | "position" | "strpos" => Some("text"),
            "set_config" => Some("text"),
            "coalesce" | "nullif" | "greatest" | "least" | "ifnull" => common.as_deref(),
            "abs" | "ceil" | "ceiling" | "floor" | "round" | "mod" | "power" | "sqrt" => {
                common.as_deref().or(Some("numeric"))
            }
            _ => None,
        };
        for arg in args {
            self.expr(&arg, env, argument_type);
        }
    }

    fn polymorphic_array_function(
        &mut self,
        name: &str,
        args: &[Expr],
        env: &[RelationColumns],
        expected: Option<&str>,
    ) -> bool {
        let arg_types = args
            .iter()
            .map(|arg| self.expr_type(arg, env))
            .collect::<Vec<_>>();
        let mut element_types = arg_types
            .iter()
            .filter_map(|pg_type| {
                pg_type
                    .as_deref()
                    .and_then(array_element_pg_type)
                    .map(str::to_string)
            })
            .map(Some)
            .collect::<Vec<_>>();
        match name {
            "array_append" | "array_remove" | "array_position" | "array_positions" => {
                element_types.push(arg_types.get(1).cloned().flatten());
            }
            "array_prepend" => element_types.push(arg_types.first().cloned().flatten()),
            "array_replace" => element_types.extend(arg_types.iter().skip(1).cloned()),
            _ => {}
        }
        if let Some(expected_element) = expected.and_then(array_element_pg_type) {
            element_types.push(Some(expected_element.to_string()));
        }
        let element = (!element_types.is_empty())
            .then(|| select_common_pg_type(self.engine.db_ref(), &element_types, name).ok())
            .flatten();
        let inferred_array = element
            .as_ref()
            .map(|element| format!("{element}[]"))
            .or_else(|| {
                expected
                    .filter(|pg_type| is_array_pg_type(pg_type))
                    .map(str::to_string)
            });
        match name {
            "unnest" | "cardinality" | "array_ndims" | "array_dims" => {
                for arg in args {
                    self.expr(arg, env, inferred_array.as_deref());
                }
            }
            "array_length" | "array_lower" | "array_upper" => {
                if let Some(array) = args.first() {
                    self.expr(array, env, inferred_array.as_deref());
                }
                if let Some(dimension) = args.get(1) {
                    self.expr(dimension, env, Some("int4"));
                }
            }
            "array_append" | "array_remove" | "array_position" | "array_positions" => {
                if let Some(array) = args.first() {
                    self.expr(array, env, inferred_array.as_deref());
                }
                if let Some(value) = args.get(1) {
                    self.expr(value, env, element.as_deref());
                }
            }
            "array_prepend" => {
                if let Some(value) = args.first() {
                    self.expr(value, env, element.as_deref());
                }
                if let Some(array) = args.get(1) {
                    self.expr(array, env, inferred_array.as_deref());
                }
            }
            "array_cat" => {
                for arg in args {
                    self.expr(arg, env, inferred_array.as_deref());
                }
            }
            "array_replace" => {
                if let Some(array) = args.first() {
                    self.expr(array, env, inferred_array.as_deref());
                }
                for value in args.iter().skip(1) {
                    self.expr(value, env, element.as_deref());
                }
            }
            _ => return false,
        }
        true
    }

    fn expr_type(&self, expr: &Expr, env: &[RelationColumns]) -> Option<String> {
        if let Expr::Array(array) = expr {
            return array
                .elem
                .iter()
                .find_map(|element| self.expr_type(element, env))
                .map(|element| format!("{element}[]"));
        }
        self.engine
            .infer_env_expr_type(expr, env)
            .or_else(|| literal_expr_type(expr))
    }

    fn assign(&mut self, one_based: usize, expected: Option<&str>) {
        if one_based == 0 {
            return;
        }
        if self.types.len() < one_based {
            self.types.resize(one_based, None);
        }
        let Some(expected) = expected else {
            return;
        };
        let slot = &mut self.types[one_based - 1];
        match slot {
            None => *slot = Some(expected.to_string()),
            Some(current) if current == expected => {}
            Some(current) => {
                if let Some(combined) = numeric_combine_pg_type(Some(current), Some(expected)) {
                    *current = combined;
                } else if current == "text" {
                    *current = expected.to_string();
                }
            }
        }
    }
}

fn array_element_pg_type(pg_type: &str) -> Option<&str> {
    pg_type.strip_suffix("[]")
}

fn is_array_pg_type(pg_type: &str) -> bool {
    array_element_pg_type(pg_type).is_some()
}

fn placeholder_index(expr: &Expr) -> Option<usize> {
    let Expr::Value(value) = expr else {
        return None;
    };
    let Value::Placeholder(value) = &value.value else {
        return None;
    };
    value.strip_prefix('$')?.parse().ok()
}

fn literal_expr_type(expr: &Expr) -> Option<String> {
    let Expr::Value(value) = expr else {
        return None;
    };
    match &value.value {
        Value::Number(value, _) => Some(
            if value.contains(['.', 'e', 'E']) {
                "numeric"
            } else if value.parse::<i32>().is_ok() {
                "int4"
            } else if value.parse::<i64>().is_ok() {
                "int8"
            } else {
                "numeric"
            }
            .to_string(),
        ),
        Value::Boolean(_) => Some("bool".to_string()),
        _ => None,
    }
}

fn binary_operand_types(
    op: &BinaryOperator,
    left: Option<&str>,
    right: Option<&str>,
    expected: Option<&str>,
) -> (Option<String>, Option<String>) {
    match op {
        BinaryOperator::And | BinaryOperator::Or => {
            (Some("bool".to_string()), Some("bool".to_string()))
        }
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => (
            right.or(left).map(str::to_string),
            left.or(right).map(str::to_string),
        ),
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => {
            let fallback = left.or(right).or(expected).unwrap_or("int4");
            (
                right.or(Some(fallback)).map(str::to_string),
                left.or(Some(fallback)).map(str::to_string),
            )
        }
        BinaryOperator::StringConcat => (
            Some(left.unwrap_or("text").to_string()),
            Some(right.unwrap_or("text").to_string()),
        ),
        _ => (right.map(str::to_string), left.map(str::to_string)),
    }
}

fn assignment_target_type(
    target: &AssignmentTarget,
    columns: &[(String, Option<String>)],
) -> Option<String> {
    let AssignmentTarget::ColumnName(column) = target else {
        return None;
    };
    let column = object_name(column).ok()?;
    let column = column.rsplit('.').next()?;
    columns
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
        .and_then(|(_, pg_type)| pg_type.clone())
}

fn flatten_relation_env(env: &[RelationColumns]) -> Vec<(String, Option<String>)> {
    env.iter()
        .flat_map(|relation| relation.columns.iter().cloned())
        .collect()
}

fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => Some(constraint),
        JoinOperator::AsOf { constraint, .. } => Some(constraint),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JsonSetReturningFunction {
    JsonArrayElements,
    JsonArrayElementsText,
    JsonEach,
    JsonEachText,
    JsonObjectKeys,
    JsonPopulateRecord,
    JsonPopulateRecordset,
    JsonToRecord,
    JsonToRecordset,
    JsonbEach,
    JsonbEachText,
    JsonbPopulateRecord,
    JsonbPopulateRecordset,
    JsonbToRecord,
    JsonbToRecordset,
    ArrayElements,
    ArrayElementsText,
    ObjectKeys,
    JsonbPathQuery,
    JsonbPathQueryTz,
}

impl JsonSetReturningFunction {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name.strip_prefix("pg_catalog.").unwrap_or(name) {
            "json_array_elements" => Some(Self::JsonArrayElements),
            "json_array_elements_text" => Some(Self::JsonArrayElementsText),
            "json_each" => Some(Self::JsonEach),
            "json_each_text" => Some(Self::JsonEachText),
            "json_object_keys" => Some(Self::JsonObjectKeys),
            "json_populate_record" => Some(Self::JsonPopulateRecord),
            "json_populate_recordset" => Some(Self::JsonPopulateRecordset),
            "json_to_record" => Some(Self::JsonToRecord),
            "json_to_recordset" => Some(Self::JsonToRecordset),
            "jsonb_array_elements" => Some(Self::ArrayElements),
            "jsonb_array_elements_text" => Some(Self::ArrayElementsText),
            "jsonb_each" => Some(Self::JsonbEach),
            "jsonb_each_text" => Some(Self::JsonbEachText),
            "jsonb_object_keys" => Some(Self::ObjectKeys),
            "jsonb_path_query" => Some(Self::JsonbPathQuery),
            "jsonb_path_query_tz" => Some(Self::JsonbPathQueryTz),
            "jsonb_populate_record" => Some(Self::JsonbPopulateRecord),
            "jsonb_populate_recordset" => Some(Self::JsonbPopulateRecordset),
            "jsonb_to_record" => Some(Self::JsonbToRecord),
            "jsonb_to_recordset" => Some(Self::JsonbToRecordset),
            _ => None,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::JsonArrayElements => "json_array_elements",
            Self::JsonArrayElementsText => "json_array_elements_text",
            Self::JsonEach => "json_each",
            Self::JsonEachText => "json_each_text",
            Self::JsonObjectKeys => "json_object_keys",
            Self::JsonPopulateRecord => "json_populate_record",
            Self::JsonPopulateRecordset => "json_populate_recordset",
            Self::JsonToRecord => "json_to_record",
            Self::JsonToRecordset => "json_to_recordset",
            Self::JsonbEach => "jsonb_each",
            Self::JsonbEachText => "jsonb_each_text",
            Self::JsonbPopulateRecord => "jsonb_populate_record",
            Self::JsonbPopulateRecordset => "jsonb_populate_recordset",
            Self::JsonbToRecord => "jsonb_to_record",
            Self::JsonbToRecordset => "jsonb_to_recordset",
            Self::ArrayElements => "jsonb_array_elements",
            Self::ArrayElementsText => "jsonb_array_elements_text",
            Self::ObjectKeys => "jsonb_object_keys",
            Self::JsonbPathQuery => "jsonb_path_query",
            Self::JsonbPathQueryTz => "jsonb_path_query_tz",
        }
    }

    pub(crate) fn pg_type(self) -> &'static str {
        match self {
            Self::JsonArrayElements => "json",
            Self::ArrayElements | Self::JsonbPathQuery | Self::JsonbPathQueryTz => "jsonb",
            Self::JsonArrayElementsText
            | Self::JsonEach
            | Self::JsonEachText
            | Self::JsonbEach
            | Self::JsonbEachText
            | Self::JsonObjectKeys
            | Self::ArrayElementsText
            | Self::ObjectKeys => "text",
            Self::JsonPopulateRecord
            | Self::JsonPopulateRecordset
            | Self::JsonToRecord
            | Self::JsonToRecordset
            | Self::JsonbPopulateRecord
            | Self::JsonbPopulateRecordset
            | Self::JsonbToRecord
            | Self::JsonbToRecordset => "record",
        }
    }

    fn accepts_json_text(self) -> bool {
        matches!(
            self,
            Self::JsonArrayElements
                | Self::JsonArrayElementsText
                | Self::JsonEach
                | Self::JsonEachText
                | Self::JsonObjectKeys
                | Self::JsonPopulateRecord
                | Self::JsonPopulateRecordset
                | Self::JsonToRecord
                | Self::JsonToRecordset
        )
    }

    fn projection_supported(self) -> bool {
        !matches!(
            self,
            Self::JsonEach
                | Self::JsonEachText
                | Self::JsonbEach
                | Self::JsonbEachText
                | Self::JsonPopulateRecord
                | Self::JsonPopulateRecordset
                | Self::JsonToRecord
                | Self::JsonToRecordset
                | Self::JsonbPopulateRecord
                | Self::JsonbPopulateRecordset
                | Self::JsonbToRecord
                | Self::JsonbToRecordset
        )
    }

    fn output_pg_types(self) -> Vec<&'static str> {
        match self {
            Self::JsonEach => vec!["text", "json"],
            Self::JsonEachText => vec!["text", "text"],
            Self::JsonbEach => vec!["text", "jsonb"],
            Self::JsonbEachText => vec!["text", "text"],
            Self::JsonPopulateRecord
            | Self::JsonPopulateRecordset
            | Self::JsonToRecord
            | Self::JsonToRecordset
            | Self::JsonbPopulateRecord
            | Self::JsonbPopulateRecordset
            | Self::JsonbToRecord
            | Self::JsonbToRecordset => vec!["record"],
            _ => vec![self.pg_type()],
        }
    }

    fn argument_count(self) -> usize {
        if matches!(
            self,
            Self::JsonPopulateRecord
                | Self::JsonPopulateRecordset
                | Self::JsonbPopulateRecord
                | Self::JsonbPopulateRecordset
        ) {
            2
        } else {
            1
        }
    }

    fn accepts_argument_count(self, actual: usize) -> bool {
        if matches!(self, Self::JsonbPathQuery | Self::JsonbPathQueryTz) {
            (2..=4).contains(&actual)
        } else if matches!(self, Self::JsonPopulateRecord | Self::JsonPopulateRecordset) {
            matches!(actual, 2 | 3)
        } else {
            actual == self.argument_count()
        }
    }

    fn returns_record(self) -> bool {
        matches!(
            self,
            Self::JsonPopulateRecord
                | Self::JsonPopulateRecordset
                | Self::JsonToRecord
                | Self::JsonToRecordset
                | Self::JsonbPopulateRecord
                | Self::JsonbPopulateRecordset
                | Self::JsonbToRecord
                | Self::JsonbToRecordset
        )
    }

    fn returns_recordset(self) -> bool {
        matches!(
            self,
            Self::JsonPopulateRecordset
                | Self::JsonToRecordset
                | Self::JsonbPopulateRecordset
                | Self::JsonbToRecordset
        )
    }

    fn json_argument_index(self) -> usize {
        if matches!(
            self,
            Self::JsonPopulateRecord
                | Self::JsonPopulateRecordset
                | Self::JsonbPopulateRecord
                | Self::JsonbPopulateRecordset
        ) {
            1
        } else {
            0
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct JsonSetReturningCall {
    pub(crate) function: JsonSetReturningFunction,
    pub(crate) alias: Option<TableAlias>,
    pub(crate) args: Vec<Expr>,
    pub(crate) with_ordinality: bool,
    pub(crate) permits_correlation: bool,
}

pub(crate) fn json_set_returning_call(
    relation: &TableFactor,
) -> Result<Option<JsonSetReturningCall>> {
    match relation {
        TableFactor::Table {
            name,
            alias,
            args: Some(args),
            with_ordinality,
            ..
        } => {
            let name = relation_name(name)?.to_ascii_lowercase();
            let Some(function) = JsonSetReturningFunction::from_name(&name) else {
                return Ok(None);
            };
            Ok(Some(JsonSetReturningCall {
                function,
                alias: alias.clone(),
                args: table_function_expr_args(args)?,
                with_ordinality: *with_ordinality,
                // PostgreSQL table functions are implicitly lateral, even when
                // the LATERAL keyword is omitted.
                permits_correlation: true,
            }))
        }
        TableFactor::Function {
            lateral,
            name,
            args,
            with_ordinality,
            alias,
        } => {
            let name = object_name(name)?.to_ascii_lowercase();
            let Some(function) = JsonSetReturningFunction::from_name(&name) else {
                return Ok(None);
            };
            Ok(Some(JsonSetReturningCall {
                function,
                alias: alias.clone(),
                args: positional_json_set_table_args(args)?,
                with_ordinality: *with_ordinality,
                permits_correlation: *lateral,
            }))
        }
        _ => Ok(None),
    }
}

pub(crate) fn positional_json_set_table_args(args: &[FunctionArg]) -> Result<Vec<Expr>> {
    args.iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
            _ => Err(SqlError::Unsupported(
                "JSON set-returning functions support positional expression arguments only"
                    .to_string(),
            )),
        })
        .collect()
}

pub(crate) fn positional_json_set_function_args(args: &FunctionArguments) -> Result<Vec<Expr>> {
    let FunctionArguments::List(list) = args else {
        return Err(SqlError::InvalidSql(
            "JSON set-returning function requires an argument list".to_string(),
        ));
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(SqlError::Unsupported(
            "DISTINCT and argument clauses are not supported for JSON set-returning functions"
                .to_string(),
        ));
    }
    positional_json_set_table_args(&list.args)
}

pub(crate) fn json_set_function_argument_count_error(
    function: JsonSetReturningFunction,
    actual: usize,
) -> SqlError {
    let expected = if matches!(
        function,
        JsonSetReturningFunction::JsonPopulateRecord
            | JsonSetReturningFunction::JsonPopulateRecordset
    ) {
        "2 or 3 arguments".to_string()
    } else {
        format!(
            "{} argument{}",
            function.argument_count(),
            if function.argument_count() == 1 {
                ""
            } else {
                "s"
            }
        )
    };
    SqlError::InvalidSql(format!(
        "{} expects {expected}, got {actual}",
        function.name()
    ))
}

fn validate_json_populate_legacy_flag(
    call: &JsonSetReturningCall,
    flag: Option<&SqlValue>,
) -> Result<()> {
    if !matches!(
        call.function,
        JsonSetReturningFunction::JsonPopulateRecord
            | JsonSetReturningFunction::JsonPopulateRecordset
    ) || flag.is_none()
    {
        return Ok(());
    }
    match flag.expect("checked optional legacy flag") {
        SqlValue::Bool(_) | SqlValue::Null => Ok(()),
        _ => Err(json_set_function_type_error(
            call.function,
            "use_json_as_text must be boolean",
        )),
    }
}

pub(crate) fn json_set_function_type_error(
    function: JsonSetReturningFunction,
    message: impl Into<String>,
) -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate: "22023",
        message: format!("{}: {}", function.name(), message.into()),
        table: None,
        column: None,
        constraint: None,
    }
}

pub(crate) fn json_set_function_values(
    function: JsonSetReturningFunction,
    value: SqlValue,
) -> Result<Vec<SqlValue>> {
    if let SqlValue::JsonText(value) = &value {
        return json_text_set_function_values(function, value);
    }
    let json = match value {
        SqlValue::Null => return Ok(Vec::new()),
        SqlValue::Json(value) if !function.accepts_json_text() => value,
        _ => {
            return Err(json_set_function_type_error(
                function,
                if function.accepts_json_text() {
                    "argument must be json"
                } else {
                    "argument must be jsonb"
                },
            ));
        }
    };
    match function {
        JsonSetReturningFunction::JsonEach
        | JsonSetReturningFunction::JsonEachText
        | JsonSetReturningFunction::JsonbEach
        | JsonSetReturningFunction::JsonbEachText => {
            unreachable!("multi-column JSON functions use json_set_function_rows")
        }
        JsonSetReturningFunction::JsonPopulateRecord
        | JsonSetReturningFunction::JsonPopulateRecordset
        | JsonSetReturningFunction::JsonToRecord
        | JsonSetReturningFunction::JsonToRecordset
        | JsonSetReturningFunction::JsonbPopulateRecord
        | JsonSetReturningFunction::JsonbPopulateRecordset
        | JsonSetReturningFunction::JsonbToRecord
        | JsonSetReturningFunction::JsonbToRecordset => {
            unreachable!("typed JSON record functions use json_record_function_rows")
        }
        JsonSetReturningFunction::JsonbPathQuery | JsonSetReturningFunction::JsonbPathQueryTz => {
            unreachable!("jsonpath query requires its complete argument list")
        }
        JsonSetReturningFunction::JsonArrayElements | JsonSetReturningFunction::ArrayElements => {
            match json {
                JsonValue::Array(values) => Ok(values
                    .into_iter()
                    .map(|value| {
                        if function.accepts_json_text() {
                            SqlValue::JsonText(PgJsonText::from_value(value))
                        } else {
                            SqlValue::Json(value)
                        }
                    })
                    .collect()),
                JsonValue::Object(_) => Err(json_set_function_type_error(
                    function,
                    "cannot extract elements from an object",
                )),
                _ => Err(json_set_function_type_error(
                    function,
                    "cannot extract elements from a scalar",
                )),
            }
        }
        JsonSetReturningFunction::JsonArrayElementsText
        | JsonSetReturningFunction::ArrayElementsText => match json {
            JsonValue::Array(values) => Ok(values
                .into_iter()
                .map(|value| match value {
                    JsonValue::Null => SqlValue::Null,
                    JsonValue::String(value) => SqlValue::String(value),
                    value => SqlValue::String(postgres_jsonb_text(&value)),
                })
                .collect()),
            JsonValue::Object(_) => Err(json_set_function_type_error(
                function,
                "cannot extract elements from an object",
            )),
            _ => Err(json_set_function_type_error(
                function,
                "cannot extract elements from a scalar",
            )),
        },
        JsonSetReturningFunction::JsonObjectKeys | JsonSetReturningFunction::ObjectKeys => {
            match json {
                JsonValue::Object(object) => {
                    let mut keys = object.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
                    if !function.accepts_json_text() {
                        keys.sort_by(|left, right| {
                            left.len()
                                .cmp(&right.len())
                                .then_with(|| left.as_bytes().cmp(right.as_bytes()))
                        });
                    }
                    Ok(keys.into_iter().map(SqlValue::String).collect())
                }
                JsonValue::Array(_) => Err(json_set_function_type_error(
                    function,
                    "cannot call jsonb_object_keys on an array",
                )),
                _ => Err(json_set_function_type_error(
                    function,
                    "cannot call jsonb_object_keys on a scalar",
                )),
            }
        }
    }
}

fn json_set_function_values_from_args(
    function: JsonSetReturningFunction,
    arguments: &[SqlValue],
) -> Result<Vec<SqlValue>> {
    if matches!(
        function,
        JsonSetReturningFunction::JsonbPathQuery | JsonSetReturningFunction::JsonbPathQueryTz
    ) {
        return eval_jsonpath_query_values(function.name(), arguments);
    }
    let [argument] = arguments else {
        return Err(json_set_function_argument_count_error(
            function,
            arguments.len(),
        ));
    };
    json_set_function_values(function, argument.clone())
}

pub(crate) fn json_set_function_rows(
    function: JsonSetReturningFunction,
    value: SqlValue,
) -> Result<Vec<Vec<SqlValue>>> {
    if !matches!(
        function,
        JsonSetReturningFunction::JsonEach
            | JsonSetReturningFunction::JsonEachText
            | JsonSetReturningFunction::JsonbEach
            | JsonSetReturningFunction::JsonbEachText
    ) {
        return json_set_function_values(function, value)
            .map(|values| values.into_iter().map(|value| vec![value]).collect());
    }
    if matches!(
        function,
        JsonSetReturningFunction::JsonbEach | JsonSetReturningFunction::JsonbEachText
    ) {
        let value = match value {
            SqlValue::Null => return Ok(Vec::new()),
            SqlValue::Json(value) => value,
            _ => {
                return Err(json_set_function_type_error(
                    function,
                    "argument must be jsonb",
                ));
            }
        };
        let JsonValue::Object(object) = value else {
            return Err(json_set_function_type_error(
                function,
                if value.is_array() {
                    "cannot deconstruct an array as an object"
                } else {
                    "cannot deconstruct a scalar"
                },
            ));
        };
        let mut entries = object.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| {
            left.len()
                .cmp(&right.len())
                .then_with(|| left.as_bytes().cmp(right.as_bytes()))
        });
        return Ok(entries
            .into_iter()
            .map(|(key, value)| {
                let value = if matches!(function, JsonSetReturningFunction::JsonbEach) {
                    SqlValue::Json(value)
                } else {
                    match value {
                        JsonValue::Null => SqlValue::Null,
                        JsonValue::String(value) => SqlValue::String(value),
                        value => SqlValue::String(postgres_jsonb_text(&value)),
                    }
                };
                vec![SqlValue::String(key), value]
            })
            .collect());
    }
    let value = match value {
        SqlValue::Null => return Ok(Vec::new()),
        SqlValue::JsonText(value) => value,
        _ => {
            return Err(json_set_function_type_error(
                function,
                "argument must be json",
            ));
        }
    };
    if !value.parsed().is_object() {
        return Err(json_set_function_type_error(
            function,
            if value.parsed().is_array() {
                "cannot deconstruct an array as an object"
            } else {
                "cannot deconstruct a scalar"
            },
        ));
    }

    json_text_object_entries(value.raw())?
        .into_iter()
        .map(|(key, raw)| {
            let raw = PgJsonText::parse(raw.get().to_string())
                .map_err(|error| json_set_function_type_error(function, error.to_string()))?;
            let value = if matches!(function, JsonSetReturningFunction::JsonEach) {
                SqlValue::JsonText(raw)
            } else {
                if raw.has_invalid_unicode_escape() {
                    return Err(json_set_function_type_error(
                        function,
                        "unsupported Unicode escape sequence",
                    ));
                }
                match raw.parsed() {
                    JsonValue::Null => SqlValue::Null,
                    JsonValue::String(value) => SqlValue::String(value.clone()),
                    _ => SqlValue::String(raw.raw().to_string()),
                }
            };
            Ok(vec![SqlValue::String(key), value])
        })
        .collect()
}

pub(crate) fn json_record_function_rows(
    call: &JsonSetReturningCall,
    base: Option<SqlValue>,
    value: SqlValue,
) -> Result<Vec<Vec<SqlValue>>> {
    let alias = call.alias.as_ref().ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "a column definition list is required for {}",
            call.function.name()
        ))
    })?;
    let base_row = match base {
        None | Some(SqlValue::Null) => vec![SqlValue::Null; alias.columns.len()],
        Some(SqlValue::Json(JsonValue::Object(object))) => alias
            .columns
            .iter()
            .map(|column| {
                object
                    .get(&column.name.value)
                    .map(json_to_sql_value)
                    .unwrap_or(SqlValue::Null)
            })
            .collect(),
        Some(_) => {
            return Err(json_set_function_type_error(
                call.function,
                "base argument must be a composite value",
            ));
        }
    };
    let value = match value {
        SqlValue::Null => {
            return Ok(if call.function.returns_recordset() {
                Vec::new()
            } else {
                vec![base_row]
            });
        }
        SqlValue::JsonText(value) => value,
        SqlValue::Json(value) if !call.function.accepts_json_text() => {
            PgJsonText::from_value(value)
        }
        _ => {
            return Err(json_set_function_type_error(
                call.function,
                if call.function.accepts_json_text() {
                    "argument must be json"
                } else {
                    "argument must be jsonb"
                },
            ));
        }
    };
    if value.has_invalid_unicode_escape() {
        return Err(json_set_function_type_error(
            call.function,
            "unsupported Unicode escape sequence",
        ));
    }

    let objects = if !call.function.returns_recordset() {
        if !value.parsed().is_object() {
            return Err(json_set_function_type_error(
                call.function,
                "argument must be an object",
            ));
        }
        vec![value.raw().to_string()]
    } else {
        if !value.parsed().is_array() {
            return Err(json_set_function_type_error(
                call.function,
                "cannot call on a non-array",
            ));
        }
        serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(value.raw())
            .map_err(|error| json_set_function_type_error(call.function, error.to_string()))?
            .into_iter()
            .map(|value| value.get().to_string())
            .collect()
    };
    objects
        .into_iter()
        .map(|raw| {
            let parsed = PgJsonText::parse(raw.clone())
                .map_err(|error| json_set_function_type_error(call.function, error.to_string()))?;
            if !parsed.parsed().is_object() {
                return Err(json_set_function_type_error(
                    call.function,
                    "argument must be an object",
                ));
            }
            let object = json_text_object_entries(&raw)?
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>();
            alias
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let Some(value) = object.get(&column.name.value) else {
                        return Ok(base_row[index].clone());
                    };
                    let data_type = column.data_type.as_ref().ok_or_else(|| {
                        SqlError::InvalidSql(format!("column {} requires a data type", column.name))
                    })?;
                    let pg_type = pg_type_from_data_type(data_type)?.0;
                    cast_value(json_record_raw_input(value.get(), &pg_type)?, data_type).map_err(
                        |error| match error {
                            SqlError::InvalidSql(message) => {
                                SqlError::InvalidTextRepresentation(message)
                            }
                            error => error,
                        },
                    )
                })
                .collect()
        })
        .collect()
}

fn json_record_raw_input(raw: &str, pg_type: &str) -> Result<SqlValue> {
    let value = PgJsonText::parse(raw.to_string())
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?;
    Ok(match value.parsed() {
        JsonValue::Null => SqlValue::Null,
        JsonValue::String(value) => SqlValue::String(value.clone()),
        JsonValue::Array(_) if pg_type.ends_with("[]") => SqlValue::Json(value.parsed().clone()),
        _ if pg_type == "json" => SqlValue::JsonText(value),
        _ if pg_type == "jsonb" => SqlValue::JsonText(value),
        _ => SqlValue::String(value.raw().trim().to_string()),
    })
}

fn json_text_set_function_values(
    function: JsonSetReturningFunction,
    value: &PgJsonText,
) -> Result<Vec<SqlValue>> {
    if !function.accepts_json_text() {
        return Err(json_set_function_type_error(
            function,
            "argument must be jsonb",
        ));
    }
    match function {
        JsonSetReturningFunction::JsonArrayElements
        | JsonSetReturningFunction::JsonArrayElementsText => {
            if !value.parsed().is_array() {
                return Err(json_set_function_type_error(
                    function,
                    if value.parsed().is_object() {
                        "cannot extract elements from an object"
                    } else {
                        "cannot extract elements from a scalar"
                    },
                ));
            }
            let elements =
                serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(value.raw())
                    .map_err(|error| json_set_function_type_error(function, error.to_string()))?;
            elements
                .into_iter()
                .map(|element| {
                    let element =
                        PgJsonText::parse(element.get().to_string()).map_err(|error| {
                            json_set_function_type_error(function, error.to_string())
                        })?;
                    if matches!(function, JsonSetReturningFunction::JsonArrayElements) {
                        return Ok(SqlValue::JsonText(element));
                    }
                    if element.has_invalid_unicode_escape() {
                        return Err(json_set_function_type_error(
                            function,
                            "unsupported Unicode escape sequence",
                        ));
                    }
                    Ok(match element.parsed() {
                        JsonValue::Null => SqlValue::Null,
                        JsonValue::String(value) => SqlValue::String(value.clone()),
                        _ => SqlValue::String(element.raw().to_string()),
                    })
                })
                .collect()
        }
        JsonSetReturningFunction::JsonObjectKeys => {
            if !value.parsed().is_object() {
                return Err(json_set_function_type_error(
                    function,
                    if value.parsed().is_array() {
                        "cannot call json_object_keys on an array"
                    } else {
                        "cannot call json_object_keys on a scalar"
                    },
                ));
            }
            Ok(json_text_object_entries(value.raw())?
                .into_iter()
                .map(|(key, _)| SqlValue::String(key))
                .collect())
        }
        JsonSetReturningFunction::JsonEach | JsonSetReturningFunction::JsonEachText => {
            unreachable!("multi-column JSON functions use json_set_function_rows")
        }
        JsonSetReturningFunction::JsonPopulateRecord
        | JsonSetReturningFunction::JsonPopulateRecordset
        | JsonSetReturningFunction::JsonToRecord
        | JsonSetReturningFunction::JsonToRecordset => {
            unreachable!("typed JSON record functions use json_record_function_rows")
        }
        JsonSetReturningFunction::JsonbPathQuery | JsonSetReturningFunction::JsonbPathQueryTz => {
            unreachable!("jsonpath query does not accept json input")
        }
        _ => unreachable!("JSON text variants handled above"),
    }
}

struct JsonTextObjectEntries(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> serde::Deserialize<'de> for JsonTextObjectEntries {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = JsonTextObjectEntries;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or_default());
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<Box<serde_json::value::RawValue>>()?;
                    entries.push((key, value));
                }
                Ok(JsonTextObjectEntries(entries))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn json_text_object_entries(raw: &str) -> Result<Vec<(String, Box<serde_json::value::RawValue>)>> {
    serde_json::from_str::<JsonTextObjectEntries>(raw)
        .map(|entries| entries.0)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))
}

fn whole_row_json_function_alias<'a>(name: &str, args: &'a [Expr]) -> Option<(&'a str, bool)> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    let valid_arity = match name {
        "row_to_json" => matches!(args.len(), 1 | 2),
        "to_json" | "to_jsonb" => args.len() == 1,
        _ => false,
    };
    if !valid_arity {
        return None;
    }

    fn alias(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.as_str()),
            Expr::Nested(expr) => alias(expr),
            _ => None,
        }
    }

    alias(&args[0]).map(|alias| (alias, name == "to_jsonb"))
}

fn whole_slot_row_json_value(
    alias: &str,
    columns: &[String],
    row: &[SqlValue],
    binary: bool,
    pretty: bool,
) -> Result<Option<SqlValue>> {
    let mut fields = Vec::new();
    for (column, value) in columns.iter().zip(row) {
        let Some((qualifier, field)) = column.split_once('.') else {
            continue;
        };
        if qualifier.eq_ignore_ascii_case(alias) && !is_postgres_system_column(field) {
            fields.push((field.to_string(), value.clone()));
        }
    }
    if fields.is_empty() {
        Ok(None)
    } else {
        composite_to_json(&fields, binary, pretty).map(Some)
    }
}

fn whole_sql_row_json_value(
    alias: &str,
    row: &SqlRow,
    binary: bool,
    pretty: bool,
) -> Result<Option<SqlValue>> {
    let mut fields = row
        .iter()
        .filter_map(|(column, value)| {
            let (qualifier, field) = column.split_once('.')?;
            (qualifier.eq_ignore_ascii_case(alias) && !is_postgres_system_column(field))
                .then(|| (field, value))
        })
        .collect::<Vec<_>>();
    fields.sort_by(|(left, _), (right, _)| left.cmp(right));

    let fields = fields
        .into_iter()
        .map(|(field, value)| (field.to_string(), value.clone()))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        Ok(None)
    } else {
        composite_to_json(&fields, binary, pretty).map(Some)
    }
}

fn scalar_sql_row_value_for_relation_alias(row: &SqlRow, alias: &str) -> Option<SqlValue> {
    let mut matched = None;
    for (column, value) in row {
        let Some((qualifier, _)) = column.split_once('.') else {
            continue;
        };
        if !qualifier.eq_ignore_ascii_case(alias) {
            continue;
        }
        if matched.is_some() {
            return None;
        }
        matched = Some(value.clone());
    }
    matched
}

fn composite_sql_row_value_for_relation_alias(row: &SqlRow, alias: &str) -> Option<SqlValue> {
    let mut fields = row
        .iter()
        .filter_map(|(column, value)| {
            let (qualifier, field) = column.split_once('.')?;
            (qualifier.eq_ignore_ascii_case(alias) && !is_postgres_system_column(field)).then(
                || SqlCompositeField {
                    name: field.to_string(),
                    pg_type: "unknown".to_string(),
                    value: value.clone(),
                },
            )
        })
        .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    (!fields.is_empty()).then(|| {
        SqlValue::Composite(SqlComposite {
            type_oid: None,
            type_name: alias.to_string(),
            fields,
        })
    })
}

fn hydrate_relation_composite(db: &BicDb, value: SqlValue) -> SqlValue {
    let SqlValue::Composite(mut composite) = value else {
        return value;
    };
    let schema = load_schema(db, &composite.type_name)
        .ok()
        .flatten()
        .or_else(|| {
            let field_names = composite
                .fields
                .iter()
                .map(|field| field.name.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let mut matches = list_schemas(db).ok()?.into_iter().filter(|schema| {
                schema
                    .columns
                    .iter()
                    .filter(|column| !column.hidden)
                    .map(|column| column.name.to_ascii_lowercase())
                    .eq(field_names.iter().cloned())
            });
            let found = matches.next()?;
            matches.next().is_none().then_some(found)
        });
    let Some(schema) = schema else {
        return SqlValue::Composite(composite);
    };
    composite.type_oid = u32::try_from(schema.row_type_oid()).ok();
    composite.type_name = schema.name.clone();
    for field in &mut composite.fields {
        if let Some(column) = schema.column(&field.name) {
            field.pg_type = column.pg_type.clone();
        }
    }
    SqlValue::Composite(composite)
}

pub(crate) fn json_set_function_columns(
    call: &JsonSetReturningCall,
) -> Result<(String, Vec<String>)> {
    let alias_name = call
        .alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| call.function.name().to_string());
    if matches!(
        call.function,
        JsonSetReturningFunction::JsonPopulateRecord
            | JsonSetReturningFunction::JsonPopulateRecordset
            | JsonSetReturningFunction::JsonToRecord
            | JsonSetReturningFunction::JsonToRecordset
            | JsonSetReturningFunction::JsonbPopulateRecord
            | JsonSetReturningFunction::JsonbPopulateRecordset
            | JsonSetReturningFunction::JsonbToRecord
            | JsonSetReturningFunction::JsonbToRecordset
    ) {
        let alias = call.alias.as_ref().ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "a column definition list is required for functions returning record: {}",
                call.function.name()
            ))
        })?;
        if alias.columns.is_empty()
            || alias
                .columns
                .iter()
                .any(|column| column.data_type.is_none())
        {
            return Err(SqlError::InvalidSql(format!(
                "a typed column definition list is required for {}",
                call.function.name()
            )));
        }
        let mut columns = alias
            .columns
            .iter()
            .map(|column| column.name.value.clone())
            .collect::<Vec<_>>();
        if call.with_ordinality {
            columns.push("ordinality".to_string());
        }
        return Ok((alias_name, columns));
    }
    let mut columns = vec![match call.function {
        JsonSetReturningFunction::JsonArrayElements
        | JsonSetReturningFunction::JsonArrayElementsText
        | JsonSetReturningFunction::ArrayElements
        | JsonSetReturningFunction::ArrayElementsText
        | JsonSetReturningFunction::JsonbPathQuery
        | JsonSetReturningFunction::JsonbPathQueryTz => "value".to_string(),
        JsonSetReturningFunction::JsonEach
        | JsonSetReturningFunction::JsonEachText
        | JsonSetReturningFunction::JsonbEach
        | JsonSetReturningFunction::JsonbEachText => "key".to_string(),
        JsonSetReturningFunction::JsonObjectKeys | JsonSetReturningFunction::ObjectKeys => {
            call.function.name().to_string()
        }
        JsonSetReturningFunction::JsonPopulateRecord
        | JsonSetReturningFunction::JsonPopulateRecordset
        | JsonSetReturningFunction::JsonToRecord
        | JsonSetReturningFunction::JsonToRecordset
        | JsonSetReturningFunction::JsonbPopulateRecord
        | JsonSetReturningFunction::JsonbPopulateRecordset
        | JsonSetReturningFunction::JsonbToRecord
        | JsonSetReturningFunction::JsonbToRecordset => {
            unreachable!("JSON record columns handled above")
        }
    }];
    if matches!(
        call.function,
        JsonSetReturningFunction::JsonEach
            | JsonSetReturningFunction::JsonEachText
            | JsonSetReturningFunction::JsonbEach
            | JsonSetReturningFunction::JsonbEachText
    ) {
        columns.push("value".to_string());
    }
    if call.with_ordinality {
        columns.push("ordinality".to_string());
    }
    if let Some(alias) = &call.alias {
        if alias.columns.len() > columns.len() {
            return Err(SqlError::InvalidSql(format!(
                "table alias \"{}\" has {} columns available but {} columns specified for \"{}\"",
                alias_name,
                columns.len(),
                alias.columns.len(),
                call.function.name()
            )));
        }
        if alias.columns.is_empty()
            && matches!(
                call.function,
                JsonSetReturningFunction::JsonObjectKeys | JsonSetReturningFunction::ObjectKeys
            )
        {
            columns[0] = alias_name.clone();
        } else if !alias.columns.is_empty() {
            for (column, alias) in columns.iter_mut().zip(&alias.columns) {
                *column = alias.name.value.clone();
            }
        }
    }
    Ok((alias_name, columns))
}

pub(crate) fn json_set_function_output_pg_types(
    call: &JsonSetReturningCall,
) -> Result<Vec<String>> {
    let mut types = if matches!(
        call.function,
        JsonSetReturningFunction::JsonPopulateRecord
            | JsonSetReturningFunction::JsonPopulateRecordset
            | JsonSetReturningFunction::JsonToRecord
            | JsonSetReturningFunction::JsonToRecordset
            | JsonSetReturningFunction::JsonbPopulateRecord
            | JsonSetReturningFunction::JsonbPopulateRecordset
            | JsonSetReturningFunction::JsonbToRecord
            | JsonSetReturningFunction::JsonbToRecordset
    ) {
        call.alias
            .as_ref()
            .ok_or_else(|| SqlError::InvalidSql("missing JSON record alias".to_string()))?
            .columns
            .iter()
            .map(|column| {
                pg_type_from_data_type(column.data_type.as_ref().ok_or_else(|| {
                    SqlError::InvalidSql("missing JSON record column type".to_string())
                })?)
                .map(|(pg_type, _)| pg_type)
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        call.function
            .output_pg_types()
            .into_iter()
            .map(str::to_string)
            .collect()
    };
    if call.with_ordinality {
        types.push("int8".to_string());
    }
    Ok(types)
}

#[derive(Clone, Debug)]
pub(crate) struct JsonProjectionSetReturningCall {
    pub(crate) projection_index: usize,
    pub(crate) function: JsonSetReturningFunction,
    pub(crate) arguments: Vec<Expr>,
}

pub(crate) fn json_projection_set_returning_call(
    projection: &[SelectItem],
) -> Result<Option<JsonProjectionSetReturningCall>> {
    let mut result = None;
    for (projection_index, item) in projection.iter().enumerate() {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let Expr::Function(function) = expr else {
            continue;
        };
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let Some(kind) = JsonSetReturningFunction::from_name(&name) else {
            continue;
        };
        if !kind.projection_supported() {
            continue;
        }
        if result.is_some() {
            return Err(SqlError::Unsupported(
                "multiple JSON set-returning functions in one SELECT list are not supported"
                    .to_string(),
            ));
        }
        let args = positional_json_set_function_args(&function.args)?;
        if !kind.accepts_argument_count(args.len()) {
            return Err(json_set_function_argument_count_error(kind, args.len()));
        }
        result = Some(JsonProjectionSetReturningCall {
            projection_index,
            function: kind,
            arguments: args,
        });
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ArrayProjectionSetReturningFunction {
    Unnest,
    GenerateSubscripts,
    PgSnapshotXip,
    TxidSnapshotXip,
}

#[derive(Clone, Debug)]
pub(crate) struct ArrayProjectionSetReturningCall {
    pub(crate) projection_index: usize,
    pub(crate) function: ArrayProjectionSetReturningFunction,
    pub(crate) arguments: Vec<Expr>,
}

pub(crate) fn array_projection_set_returning_calls(
    projection: &[SelectItem],
) -> Result<Vec<ArrayProjectionSetReturningCall>> {
    let mut result = Vec::new();
    for (projection_index, item) in projection.iter().enumerate() {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        let Expr::Function(function) = expr else {
            continue;
        };
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        let kind = match name {
            "unnest" => ArrayProjectionSetReturningFunction::Unnest,
            "generate_subscripts" => ArrayProjectionSetReturningFunction::GenerateSubscripts,
            "pg_snapshot_xip" => ArrayProjectionSetReturningFunction::PgSnapshotXip,
            "txid_snapshot_xip" => ArrayProjectionSetReturningFunction::TxidSnapshotXip,
            _ => continue,
        };
        let arguments = function_args(function);
        match kind {
            ArrayProjectionSetReturningFunction::Unnest if arguments.len() != 1 => {
                return Err(SqlError::InvalidSql(format!(
                    "UNNEST expects one array argument in a SELECT list, got {}",
                    arguments.len()
                )));
            }
            ArrayProjectionSetReturningFunction::GenerateSubscripts
                if !(2..=3).contains(&arguments.len()) =>
            {
                return Err(SqlError::InvalidSql(format!(
                    "generate_subscripts expects two or three arguments, got {}",
                    arguments.len()
                )));
            }
            ArrayProjectionSetReturningFunction::PgSnapshotXip
            | ArrayProjectionSetReturningFunction::TxidSnapshotXip
                if arguments.len() != 1 =>
            {
                return Err(SqlError::InvalidSql(format!(
                    "{name} expects one snapshot argument, got {}",
                    arguments.len()
                )));
            }
            _ => {}
        }
        result.push(ArrayProjectionSetReturningCall {
            projection_index,
            function: kind,
            arguments,
        });
    }
    Ok(result)
}

pub(crate) fn array_projection_set_values(
    function: ArrayProjectionSetReturningFunction,
    arguments: Vec<SqlValue>,
) -> Result<Vec<SqlValue>> {
    match function {
        ArrayProjectionSetReturningFunction::Unnest => {
            unnest_values_from_sql(arguments.into_iter().next().unwrap_or(SqlValue::Null))
        }
        ArrayProjectionSetReturningFunction::GenerateSubscripts => {
            let array = arguments.first().cloned().unwrap_or(SqlValue::Null);
            if matches!(array, SqlValue::Null) {
                return Ok(Vec::new());
            }
            let dimension = arguments.get(1).and_then(sql_value_i64).ok_or_else(|| {
                SqlError::InvalidSql("generate_subscripts dimension must be an integer".to_string())
            })?;
            if dimension <= 0 {
                return Ok(Vec::new());
            }
            let reverse = arguments
                .get(2)
                .map(|value| sql_value_truth(value.clone()))
                .transpose()?
                .flatten()
                .unwrap_or(false);
            let (dimensions, lower_bounds) = match array {
                SqlValue::Json(value) => (array_json_dimensions(&value), None),
                SqlValue::String(value) => match parse_pg_array_value(&value) {
                    Ok(parsed) => (
                        array_json_dimensions(&parsed.value),
                        parsed
                            .lower_bounds
                            .map(|bounds| bounds.into_iter().map(i64::from).collect::<Vec<_>>()),
                    ),
                    Err(error) => match parse_pg_int_vector(&value) {
                        Some(values) => (Some(vec![values.len()]), None),
                        None => return Err(error),
                    },
                },
                other => {
                    return Err(SqlError::InvalidSql(format!(
                        "generate_subscripts expects an array, got {}",
                        other.to_cell()
                    )));
                }
            };
            let index = usize::try_from(dimension - 1).map_err(|_| {
                SqlError::numeric_value_out_of_range(
                    "generate_subscripts dimension is out of range",
                )
            })?;
            let Some(length) = dimensions.and_then(|dimensions| dimensions.get(index).copied())
            else {
                return Ok(Vec::new());
            };
            let lower = lower_bounds
                .and_then(|bounds| bounds.get(index).copied())
                .unwrap_or(1);
            let upper = lower
                .checked_add(i64::try_from(length).unwrap_or(i64::MAX))
                .and_then(|value| value.checked_sub(1))
                .ok_or_else(|| {
                    SqlError::numeric_value_out_of_range(
                        "generate_subscripts result is out of range",
                    )
                })?;
            let mut values = (lower..=upper).map(SqlValue::Int).collect::<Vec<_>>();
            if reverse {
                values.reverse();
            }
            Ok(values)
        }
        ArrayProjectionSetReturningFunction::PgSnapshotXip => pg_snapshot_xip_values(
            "pg_snapshot_xip",
            arguments.first().unwrap_or(&SqlValue::Null),
        ),
        ArrayProjectionSetReturningFunction::TxidSnapshotXip => pg_snapshot_xip_values(
            "txid_snapshot_xip",
            arguments.first().unwrap_or(&SqlValue::Null),
        ),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueryPlan {
    pub(crate) kind: PlanKind,
    pub(crate) estimated_rows: usize,
    pub(crate) estimated_cost: f64,
}

#[derive(Clone, Debug)]
pub(crate) enum PlanKind {
    PrimaryKeyLookup {
        record_id: String,
    },
    /// A non-negated `id IN (...)` conjunct: one point lookup per literal
    /// instead of a full scan re-evaluating the list against every row.
    PrimaryKeyInLookup {
        record_ids: Vec<String>,
    },
    PrimaryKeyPrefixLookup {
        prefix: String,
        prefix_values: Vec<SqlValue>,
    },
    IndexLookup {
        index_name: String,
        prefix: Vec<IndexValue>,
    },
    IndexRange {
        index_name: String,
        lower: Option<IndexValue>,
        upper: Option<IndexValue>,
    },
    OrderedIndexScan {
        index_name: String,
        descending: bool,
        limit: Option<usize>,
    },
    /// Equality prefix on an index's leading fields, then ORDER BY the NEXT index
    /// field with a LIMIT: returns only the first `limit` ids in key order from the
    /// prefix's single shard, instead of fetching every prefix match and sorting.
    /// The "customer's latest order" (`WHERE w=? AND d=? AND c=? ORDER BY
    /// o_id DESC LIMIT 1`) shape. Only emitted when the WHERE is EXACTLY the
    /// equality prefix (no residual filter), so the bounded result is exact.
    IndexPrefixOrderedScan {
        index_name: String,
        prefix: Vec<IndexValue>,
        descending: bool,
        limit: usize,
    },
    SpatialIndexScan {
        index_name: String,
        predicate: SpatialIndexPredicate,
    },
    GeometricIndexScan {
        index_name: String,
        display_name: String,
        operator: String,
        envelope: [f64; 4],
    },
    GeometricKnnIndexScan {
        index_name: String,
        display_name: String,
        point: (f64, f64),
        limit: usize,
    },
    FullTextIndexScan {
        index_name: String,
        /// Candidate pks, computed ONCE at plan time (costing needs the count
        /// anyway); execution consumes them instead of walking the index a
        /// second time.
        ids: std::rc::Rc<BTreeSet<String>>,
    },
    JsonbIndexScan {
        index_name: String,
        candidate: JsonbIndexCandidate,
    },
    ArrayIndexScan {
        index_name: String,
        candidate: JsonbIndexCandidate,
    },
    FullScan,
}

#[derive(Clone, Debug)]
pub(crate) enum SpatialIndexPredicate {
    DWithin {
        lon: f64,
        lat: f64,
        meters: f64,
    },
    IntersectsEnvelope {
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    },
}

fn reverse_geometric_index_operator(operator: &str) -> Option<&'static str> {
    Some(match operator {
        "<<" => ">>",
        "&<" => "&>",
        ">>" => "<<",
        "&>" => "&<",
        "<<|" => "|>>",
        "&<|" => "|&>",
        "|>>" => "<<|",
        "|&>" => "&<|",
        "<^" => ">^",
        ">^" => "<^",
        "@>" => "<@",
        "<@" => "@>",
        "&&" => "&&",
        "~=" => "~=",
        _ => return None,
    })
}

fn geometric_candidate_envelope(operator: &str, bounds: [f64; 4]) -> Option<[f64; 4]> {
    let [min_x, min_y, max_x, max_y] = bounds;
    let low = -f64::MAX;
    let high = f64::MAX;
    Some(match operator {
        "&&" | "~=" | "@>" | "<@" => bounds,
        "<<" => [low, low, min_x, high],
        "&<" => [low, low, max_x, high],
        ">>" => [max_x, low, high, high],
        "&>" => [min_x, low, high, high],
        "<<|" | "<^" => [low, low, high, min_y],
        "&<|" => [low, low, high, max_y],
        "|>>" | ">^" => [low, max_y, high, high],
        "|&>" => [low, min_y, high, high],
        _ => return None,
    })
}

#[derive(Clone, Debug)]
pub(crate) enum JsonbIndexCandidate {
    All(Vec<String>),
    Any(Vec<String>),
}

#[derive(Clone, Debug)]
pub(crate) enum PrimaryKeyAccess {
    Exact {
        record_id: String,
        values: Vec<SqlValue>,
    },
    Prefix {
        prefix: String,
        prefix_values: Vec<SqlValue>,
        matched_columns: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IndexBoundMatch {
    Equality,
    Range,
}

impl IndexBoundMatch {
    pub(crate) fn matches(self, op: &BinaryOperator) -> bool {
        match self {
            Self::Equality => matches!(op, BinaryOperator::Eq),
            Self::Range => matches!(
                op,
                BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ),
        }
    }
}

impl QueryPlan {
    pub(crate) fn full_scan() -> Self {
        Self::full_scan_with_estimate(0)
    }

    pub(crate) fn full_scan_with_estimate(estimated_rows: usize) -> Self {
        Self {
            kind: PlanKind::FullScan,
            estimated_rows,
            estimated_cost: estimated_rows as f64,
        }
    }

    pub(crate) fn explain_lines(&self) -> Vec<String> {
        let mut lines = match &self.kind {
            PlanKind::PrimaryKeyLookup { record_id } => {
                vec![format!("PrimaryKeyLookup id = '{record_id}'")]
            }
            PlanKind::PrimaryKeyInLookup { record_ids } => {
                vec![format!("PrimaryKeyInLookup {} ids", record_ids.len())]
            }
            PlanKind::PrimaryKeyPrefixLookup { prefix, .. } => {
                vec![format!("PrimaryKeyPrefixScan id LIKE '{prefix}%'")]
            }
            PlanKind::IndexLookup { index_name, prefix } => vec![
                format!("IndexScan {index_name}"),
                format!("PrefixKeys {}", prefix.len()),
            ],
            PlanKind::IndexRange {
                index_name,
                lower,
                upper,
            } => vec![
                format!("IndexRangeScan {index_name}"),
                format!(
                    "Bounds lower={} upper={}",
                    lower
                        .as_ref()
                        .map(index_value_label)
                        .unwrap_or_else(|| "-inf".to_string()),
                    upper
                        .as_ref()
                        .map(index_value_label)
                        .unwrap_or_else(|| "+inf".to_string())
                ),
            ],
            PlanKind::OrderedIndexScan {
                index_name,
                descending,
                limit,
            } => vec![
                format!("OrderedIndexScan {index_name}"),
                format!("Direction {}", if *descending { "DESC" } else { "ASC" }),
                format!(
                    "Limit {}",
                    limit
                        .map(|limit| limit.to_string())
                        .unwrap_or_else(|| "none".to_string())
                ),
            ],
            PlanKind::IndexPrefixOrderedScan {
                index_name,
                prefix,
                descending,
                limit,
            } => vec![
                format!("IndexPrefixOrderedScan {index_name}"),
                format!(
                    "Prefix [{}]",
                    prefix
                        .iter()
                        .map(index_value_label)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                format!("Direction {}", if *descending { "DESC" } else { "ASC" }),
                format!("Limit {limit}"),
            ],
            PlanKind::SpatialIndexScan {
                index_name,
                predicate,
            } => {
                let mut lines = vec![format!("SpatialIndexScan {index_name}")];
                match predicate {
                    SpatialIndexPredicate::DWithin { meters, .. } => {
                        lines.push(format!("Predicate ST_DWithin meters={meters}"));
                    }
                    SpatialIndexPredicate::IntersectsEnvelope { .. } => {
                        lines.push("Predicate ST_Intersects envelope".to_string());
                    }
                }
                lines
            }
            PlanKind::GeometricIndexScan {
                display_name,
                operator,
                ..
            } => vec![
                format!("GeometricIndexScan {display_name}"),
                format!("Predicate {operator}"),
                "Recheck true".to_string(),
            ],
            PlanKind::GeometricKnnIndexScan {
                display_name,
                limit,
                ..
            } => vec![
                format!("GeometricKnnIndexScan {display_name}"),
                "OrderBy <->".to_string(),
                format!("Limit {limit}"),
                "Recheck true".to_string(),
            ],
            PlanKind::FullTextIndexScan { index_name, .. } => {
                vec![format!("FullTextIndexScan {index_name}")]
            }
            PlanKind::JsonbIndexScan { index_name, .. } => {
                vec![format!("JsonbIndexScan {index_name}")]
            }
            PlanKind::ArrayIndexScan { index_name, .. } => {
                vec![format!("ArrayIndexScan {index_name}")]
            }
            PlanKind::FullScan => vec!["FullScan".to_string()],
        };
        lines.push(format!("EstimatedRows {}", self.estimated_rows));
        lines.push(format!("Cost {:.3}", self.estimated_cost));
        lines
    }
}

// Runtime A/B gate for the RowId read fast path (`BICDB_ROWID_FAST_PATH`). Read
// ONCE into a cached atomic. Default ON; set `0`/`off`/`false`/`no` to force the
// legacy String read path (which, on this binary, is the pre-Phase-2b baseline
// behavior) so a single binary serves both arms of a same-binary A/B.
pub(crate) const ROWID_FP_UNINIT: u8 = 0;
pub(crate) const ROWID_FP_OFF: u8 = 1;
pub(crate) const ROWID_FP_ON: u8 = 2;
pub(crate) static ROWID_FAST_PATH_STATE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(ROWID_FP_UNINIT);

#[inline]
/// Typed-row read path gate (`BICDB_TYPED_ROWS=1`): build SELECT slot rows
/// straight from the cached flat cell view of each resident record, skipping
/// the metadata JSON -> `serde_json::Value` bridge entirely. Off by default
/// for same-binary A/B.
/// Cell-row read path (`BICDB_CELL_ROWS`, default on; `0`/`off` disables):
/// schema-typed rows are read as borrowed cells from the resident JSON text in
/// one streaming pass — no `serde_json::Value` tree, no per-record cache — for
/// point lookups, index-located row sets and UPDATE candidates. Rows a field
/// cannot be read from fall back to the `Record` path individually.
pub(crate) fn cell_rows_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_CELL_ROWS")
            .map(|value| !matches!(value.as_str(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

pub(crate) fn typed_rows_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_TYPED_ROWS")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    })
}

#[inline]
/// Transaction-scoped index-lookup cache gate (`BICDB_TXN_LOOKUP_CACHE=1`):
/// share the committed-state index lookup caches across all statements of one
/// transaction (keyed by transaction id + catalog generation) instead of
/// rebuilding them per statement, so a proc's SELECT-then-UPDATE of the same
/// row pays one BTree descent instead of two. Own pending writes stay visible
/// because the rowid fast path already excludes tables with buffered writes
/// and the pk-string path merges pending candidates after the cache. Off by
/// default for same-binary A/B.
/// Ceiling on candidate row pairs a single nested-loop (cartesian / non-equi)
/// join stage may enumerate, from `BICDB_MAX_NESTED_JOIN_PAIRS` (default
/// 100_000_000). Cross joins fully materialize in memory and the inner loop is
/// O(left x right), so an unbounded product is both a memory and a CPU
/// exhaustion vector reachable by any authenticated role — e.g. a 5-way
/// `generate_series(1,200)` cross join is 3.2e11 pairs. Equi-joins take the
/// hash path and never reach this stage. `0` disables the ceiling.
pub(crate) fn max_nested_join_pairs() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_NESTED_JOIN_PAIRS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(100_000_000)
    })
}

/// Row ceiling for an aggregate over a WHOLE UNFILTERED TABLE from
/// `BICDB_MAX_AGGREGATE_ROWS` (default 50_000_000; `0` disables). Collecting
/// aggregates (string_agg / array_agg / json_agg) and any non-streaming
/// aggregate materialize their entire input in memory to emit one row. A bare
/// `SELECT string_agg(col, ',') FROM big_table` — no WHERE, no GROUP BY — is an
/// unbounded `O(table)` allocation reachable by any authenticated role; it is
/// refused before the allocation when the estimate exceeds this. Filtered,
/// grouped, and streaming (count/sum/min/max) aggregates are unaffected.
pub(crate) fn max_aggregate_scan_rows() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("BICDB_MAX_AGGREGATE_ROWS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(50_000_000)
    })
}

/// Whether statements inside one transaction share their index and rowid
/// lookup caches (on by default; `BICDB_TXN_LOOKUP_CACHE=0` turns it off).
/// A transaction that reads the same keys from several statements — every
/// TPC-C transaction does — resolves each key once instead of per statement.
pub(crate) fn txn_lookup_cache_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_TXN_LOOKUP_CACHE")
            .map(|value| !matches!(value.as_str(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

pub(crate) fn rowid_fast_path_enabled() -> bool {
    use std::sync::atomic::Ordering;
    match ROWID_FAST_PATH_STATE.load(Ordering::Relaxed) {
        ROWID_FP_ON => true,
        ROWID_FP_OFF => false,
        _ => {
            let on = std::env::var("BICDB_ROWID_FAST_PATH")
                .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                .unwrap_or(true);
            ROWID_FAST_PATH_STATE.store(
                if on { ROWID_FP_ON } else { ROWID_FP_OFF },
                Ordering::Relaxed,
            );
            on
        }
    }
}

pub(crate) fn full_text_index_expression_matches(stored: &str, query: &Expr) -> bool {
    fn key(expr: &Expr) -> String {
        match expr {
            Expr::Nested(inner) => key(inner),
            _ => expr
                .to_string()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase(),
        }
    }

    // Stored index expressions are few and immutable; re-parsing and
    // re-rendering one per query was ~14% of a selective ranked query's
    // fixed cost. Canonicalize each distinct text once, process-wide.
    static CANONICAL: std::sync::OnceLock<
        std::sync::RwLock<rustc_hash::FxHashMap<String, Option<String>>>,
    > = std::sync::OnceLock::new();
    let cache = CANONICAL.get_or_init(|| std::sync::RwLock::new(rustc_hash::FxHashMap::default()));
    if let Some(canonical) = cache.read().expect("canonical cache").get(stored) {
        return canonical
            .as_ref()
            .is_some_and(|canonical| *canonical == key(query));
    }
    let canonical = parse_routine_expr(stored).ok().map(|stored| key(&stored));
    let matches = canonical
        .as_ref()
        .is_some_and(|canonical| *canonical == key(query));
    let mut write = cache.write().expect("canonical cache");
    if write.len() > 4_096 {
        // A runaway set of distinct expressions (tests churning schemas)
        // must not grow without bound.
        write.clear();
    }
    write.insert(stored.to_string(), canonical);
    matches
}

pub(crate) fn row_columns_expr_pg_type<'a>(
    db: &BicDb,
    expr: &Expr,
    columns: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let columns = columns.into_iter().collect::<Vec<_>>();
    if let Expr::Nested(inner) | Expr::Collate { expr: inner, .. } = expr {
        return row_columns_expr_pg_type(db, inner, columns.iter().copied());
    }
    if let Expr::Case {
        conditions,
        else_result,
        ..
    } = expr
    {
        let mut branch_types = Vec::with_capacity(conditions.len() + 1);
        branch_types.push(
            else_result
                .as_deref()
                .and_then(|result| row_columns_expr_pg_type(db, result, columns.iter().copied())),
        );
        branch_types.extend(conditions.iter().map(|condition| {
            row_columns_expr_pg_type(db, &condition.result, columns.iter().copied())
        }));
        return select_common_pg_type(db, &branch_types, "CASE").ok();
    }
    if let Expr::Array(array) = expr {
        let element_types = array
            .elem
            .iter()
            .map(|element| row_columns_expr_pg_type(db, element, columns.iter().copied()))
            .collect::<Vec<_>>();
        return select_common_pg_type(db, &element_types, "ARRAY")
            .ok()
            .map(|element| format!("{element}[]"));
    }
    if let Expr::Function(function) = expr {
        let name = object_name(&function.name).ok()?.to_ascii_lowercase();
        let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
        if matches!(
            bare_name,
            "coalesce" | "nullif" | "greatest" | "least" | "ifnull"
        ) {
            let arg_types = function_args(function)
                .iter()
                .map(|arg| row_columns_expr_pg_type(db, arg, columns.iter().copied()))
                .collect::<Vec<_>>();
            return select_common_pg_type(db, &arg_types, &bare_name.to_ascii_uppercase()).ok();
        }
    }
    if let Some(pg_type) = projected_expr_pg_type_with_db(db, expr) {
        return Some(pg_type);
    }
    let mut relation_type = None;
    for relation in columns
        .iter()
        .filter_map(|column| column.rsplit_once('.').map(|(relation, _)| relation))
        .collect::<BTreeSet<_>>()
    {
        let Some(schema) = load_schema_shared(db, relation).ok().flatten() else {
            continue;
        };
        let Some(pg_type) = projected_expr_pg_type(expr, Some(&schema)) else {
            continue;
        };
        if relation_type
            .as_ref()
            .is_some_and(|found| found != &pg_type)
        {
            return None;
        }
        relation_type = Some(pg_type);
    }
    if relation_type.is_some() {
        return relation_type;
    }
    let column_name = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(idents) => idents.last()?.value.as_str(),
        _ => return projected_expr_pg_type_with_db(db, expr),
    };
    let mut found = None;
    for column in columns {
        let Some((relation, candidate)) = column.rsplit_once('.') else {
            continue;
        };
        if !candidate.eq_ignore_ascii_case(column_name) {
            continue;
        }
        let pg_type = load_schema_shared(db, relation)
            .ok()
            .flatten()
            .and_then(|schema| {
                schema
                    .column(column_name)
                    .map(|column| column.pg_type.clone())
            })
            .or_else(|| {
                virtual_table_column_types(relation).and_then(|columns| {
                    columns
                        .into_iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(column_name))
                        .and_then(|(_, pg_type)| pg_type)
                })
            });
        let Some(pg_type) = pg_type else {
            continue;
        };
        if found.as_ref().is_some_and(|found| found != &pg_type) {
            return None;
        }
        found = Some(pg_type);
    }
    found.or_else(|| projected_expr_pg_type_with_db(db, expr))
}

fn resolved_text_search_operand_type(
    inferred: Option<String>,
    expr: &Expr,
    value: &SqlValue,
) -> Option<String> {
    if is_explicit_tsvector_expr(expr) {
        Some("tsvector".to_string())
    } else if is_explicit_tsquery_expr(expr) || matches!(value, SqlValue::TsQuery(_)) {
        Some("tsquery".to_string())
    } else {
        inferred
    }
}

fn eval_atat_match_value(
    left: SqlValue,
    right: SqlValue,
    left_type: Option<&str>,
    right_type: Option<&str>,
) -> Result<Option<bool>> {
    if left_type == Some("jsonb") && right_type == Some("jsonpath") {
        return eval_jsonpath_operator_value(left, right, true).and_then(sql_value_truth);
    }
    eval_text_search_match_value(left, right, left_type, right_type).and_then(sql_value_truth)
}

pub(crate) enum RecordLocators {
    Pks(Vec<String>),
    Rowids(Vec<RowId>),
}

const POSTGRES_SYSTEM_COLUMNS: [&str; 6] = ["tableoid", "xmin", "xmax", "cmin", "cmax", "ctid"];

fn is_postgres_system_column(column: &str) -> bool {
    let name = column.rsplit('.').next().unwrap_or(column);
    POSTGRES_SYSTEM_COLUMNS
        .iter()
        .any(|system| name.eq_ignore_ascii_case(system))
}

fn postgres_system_output_columns(table: &str, alias: &str) -> Vec<String> {
    POSTGRES_SYSTEM_COLUMNS
        .iter()
        .map(|column| format!("{alias}.{column}"))
        .chain(
            (alias != table)
                .then(|| {
                    POSTGRES_SYSTEM_COLUMNS
                        .iter()
                        .map(|column| format!("{table}.{column}"))
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .flatten(),
        )
        .collect()
}

impl RecordLocators {
    pub(crate) fn len(&self) -> usize {
        match self {
            RecordLocators::Pks(pks) => pks.len(),
            RecordLocators::Rowids(rowids) => rowids.len(),
        }
    }
}

/// A routine frame's slot layout and values while one of its embedded
/// statements runs: `ids` is the frame template's name -> slot map (the same
/// `VarId` space the routine IR was bound in), `values` the frame's slots.
/// The engine binds routine variables against this instead of snapshotting
/// the string-keyed map per statement.
#[derive(Clone, Debug)]
pub(crate) struct RoutineSlotBinding {
    pub(crate) ids: Arc<FxHashMap<String, VarId>>,
    pub(crate) values: Rc<Vec<SqlValue>>,
}

/// One `bound_row_context` build per column layout, reusable while the
/// variable binding it captured is still current.
#[derive(Debug)]
pub(crate) struct BoundContextEntry {
    pub(crate) scope: BoundExprScope,
    pub(crate) row_lookup: SlotRowLookup,
    pub(crate) vars: BoundContextVars,
}

#[derive(Debug)]
pub(crate) enum BoundContextVars {
    /// Snapshot of the string-keyed map, valid while that map is the binding.
    Snapshot {
        binding: std::sync::Weak<BTreeMap<String, SqlValue>>,
        values: Rc<Vec<SqlValue>>,
    },
    /// The frame's slots themselves; a frame write (`Rc::make_mut`) detaches
    /// this weak handle, so a stale entry can never be served.
    Slots(std::rc::Weak<Vec<SqlValue>>),
}

pub(crate) type SharedBoundContextCache = Rc<RefCell<BTreeMap<Vec<String>, BoundContextEntry>>>;

#[derive(Debug)]
pub struct SqlEngine<'db> {
    pub(crate) db: &'db BicDb,
    pub(crate) tx: Option<&'db Transaction>,
    pub(crate) settings: SqlSettings,
    pub(crate) ctes: BTreeMap<String, CteResult>,
    pub(crate) security_context: Option<SecurityContext>,
    pub(crate) runtime: Option<Arc<dyn SqlSessionRuntime>>,
    pub(crate) session_gucs: Arc<HashMap<String, String>>,
    pub(crate) routine_vars: Arc<BTreeMap<String, SqlValue>>,
    /// The running routine frame's slots (see `RoutineSlotBinding`), when an
    /// embedded statement of a compiled routine is executing.
    pub(crate) routine_slots: Option<RoutineSlotBinding>,
    /// Identity of the routine IR whose embedded statement this engine runs
    /// (see `Session::current_routine_ir`); enables the per-node type memo.
    pub(crate) routine_ir: Option<usize>,
    /// Memoize binary-operator operand types by AST node address in
    /// `eval_slot_row_value`. Only ever set by callers evaluating routine-IR-
    /// owned, subquery-free expressions (see `execute_update_with_ctes`).
    pub(crate) memo_operand_types: bool,
    /// True when the statement being executed is a node of the current
    /// routine IR (see `SqlSession::ir_owned_statement`): per-node memos
    /// keyed by AST address are safe only then.
    pub(crate) ir_owned_statement: bool,
    pub(crate) outer_row: Option<OuterSlotRow>,
    pub(crate) index_lookup_cache: IndexLookupCache,
    pub(crate) rowid_index_lookup_cache: RowIdIndexLookupCache,
    pub(crate) record_lookup_cache: RecordLookupCache,
    pub(crate) bound_var_cache: BoundVarCache,
    /// Memoizes `bound_row_context` results. A `SqlEngine` binds
    /// `routine_vars` once at construction and never reassigns it, so for a given
    /// column layout the scope and context are stable for the engine's lifetime.
    /// The several context builds per embedded statement (filter, projection,
    /// ordering, aggregates) therefore reuse builds instead of re-cloning every
    /// routine variable value when execution alternates among a few row layouts.
    /// Shared by every engine a session builds for one statement (UPDATE …
    /// FROM builds one per candidate row); entries are validated against the
    /// identity of the routine-variable binding they snapshotted.
    pub(crate) bound_context_cache: SharedBoundContextCache,
    pub(crate) cancellation: CancellationToken,
    /// Immutable per-query full-text work ceilings; exhaustion errors with
    /// `query_budget_exceeded` instead of silently widening the scan.
    pub(crate) fts_limits: bicdb_core::FtsQueryLimits,
}

// Nested maps so a cache probe is two zero-allocation hash lookups
// (&str, then &[IndexValue] via Borrow); the old flat BTreeMap keyed by an
// owned (String, Vec<IndexValue>) cloned both per probe and tree-walked
// comparing them — ~3-6% of CPU on the point-read path.
pub(crate) type IndexLookupCache =
    Rc<RefCell<FxHashMap<String, FxHashMap<Vec<IndexValue>, Rc<[String]>>>>>;
pub(crate) type RowIdIndexLookupCache =
    Rc<RefCell<FxHashMap<String, FxHashMap<Vec<IndexValue>, Rc<[RowId]>>>>>;
pub(crate) type RecordLookupCache =
    Rc<RefCell<BTreeMap<RecordLookupCacheKey, Option<Arc<Record>>>>>;
/// Caches the routine-variable name->id map for the current `routine_vars`
/// binding, keyed by the `Arc` identity of `routine_vars`. The map is rebuilt
/// only when the variable binding changes, which lets the several
/// `bound_row_context` calls made per embedded statement (filter, projection,
/// ordering, aggregates) reuse one build instead of re-normalizing every
/// variable name each time.
pub(crate) type BoundVarCache = RefCell<
    Option<(
        Weak<BTreeMap<String, SqlValue>>,
        Arc<FxHashMap<String, VarId>>,
    )>,
>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IndexLookupCacheKey {
    pub(crate) index_name: String,
    pub(crate) prefix: Vec<IndexValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RecordLookupCacheKey {
    pub(crate) collection: String,
    pub(crate) id: String,
    pub(crate) tx_write_len: Option<usize>,
}

fn typed_index_predicate_value(
    schema: &TableSchema,
    field: &IndexField,
    value: IndexValue,
) -> Result<IndexValue> {
    let column = unique_key_column_schemas(schema, &UniqueKey::IndexFields(vec![field.clone()]))
        .into_iter()
        .next()
        .flatten();
    if let Some(column) = column.filter(|column| column.user_type.is_some()) {
        let value = cast_value_to_column_type(sql_value_from_index_value(&value), column)?;
        return column_typed_index_label(column, &value).map(IndexValue::String);
    }
    // CHAR is blank-padded to its declared width on write, so the index holds
    // the padded text. A predicate literal is not padded, which made an
    // indexed CHAR column disagree with an unindexed one: `code = 'ab'`
    // matched the row until the column gained an index, and then silently
    // matched nothing. Put the probe through the same cast the stored value
    // went through.
    if let Some(column) = column.filter(|column| column.pg_type == "bpchar") {
        let value = cast_value_to_column_type(sql_value_from_index_value(&value), column)?;
        return Ok(index_value_from_sql(value)?);
    }
    let Some(pg_type) =
        index_field_pg_type(schema, field).filter(|pg_type| uses_typed_storage(pg_type))
    else {
        return Ok(value);
    };
    let value = cast_value_to_pg_type(sql_value_from_index_value(&value), &pg_type)?;
    pg_typed_index_label(&pg_type, &value).map(IndexValue::String)
}

impl<'db> SqlEngine<'db> {
    /// The query engine always holds a shared database reference; this mirrors
    /// the `SqlSession::db_ref` accessor so the two share call-site forms.
    pub(crate) fn db_ref(&self) -> &BicDb {
        self.db
    }

    pub fn new(db: &'db BicDb) -> Self {
        Self::new_with_settings(db, SqlSettings::default())
    }

    pub fn new_secure(db: &'db BicDb, ctx: SecurityContext) -> Self {
        Self::new_with_settings_and_context(db, SqlSettings::default(), Some(ctx))
    }

    pub(crate) fn new_with_settings(db: &'db BicDb, settings: SqlSettings) -> Self {
        Self {
            db,
            tx: None,
            settings,
            ctes: BTreeMap::new(),
            security_context: None,
            runtime: None,
            session_gucs: Arc::new(HashMap::new()),
            routine_vars: Arc::new(BTreeMap::new()),
            routine_slots: None,
            routine_ir: None,
            memo_operand_types: false,
            ir_owned_statement: false,
            outer_row: None,
            index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            rowid_index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            record_lookup_cache: Rc::new(RefCell::new(BTreeMap::new())),
            bound_var_cache: RefCell::new(None),
            bound_context_cache: Rc::new(RefCell::new(BTreeMap::new())),
            cancellation: CancellationToken::uncancelable(),
            fts_limits: bicdb_core::FtsQueryLimits::UNLIMITED,
        }
    }

    pub(crate) fn new_with_settings_and_context(
        db: &'db BicDb,
        settings: SqlSettings,
        security_context: Option<SecurityContext>,
    ) -> Self {
        let mut session_gucs = HashMap::new();
        bind_trusted_security_settings(&mut session_gucs, security_context.as_ref());
        Self {
            db,
            tx: None,
            settings,
            ctes: BTreeMap::new(),
            security_context,
            runtime: None,
            session_gucs: Arc::new(session_gucs),
            routine_vars: Arc::new(BTreeMap::new()),
            routine_slots: None,
            routine_ir: None,
            memo_operand_types: false,
            ir_owned_statement: false,
            outer_row: None,
            index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            rowid_index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            record_lookup_cache: Rc::new(RefCell::new(BTreeMap::new())),
            bound_var_cache: RefCell::new(None),
            bound_context_cache: Rc::new(RefCell::new(BTreeMap::new())),
            cancellation: CancellationToken::uncancelable(),
            fts_limits: bicdb_core::FtsQueryLimits::UNLIMITED,
        }
    }

    /// Build the short-lived engine used by one statement in a [`SqlSession`]
    /// without first allocating default cache owners that the session then
    /// immediately replaces. Stored procedures create one of these for every
    /// embedded statement, so the ordinary builder chain's throwaway `Rc`s
    /// showed up as allocator traffic even though the transaction caches were
    /// already available to share.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_session_parts(
        db: &'db BicDb,
        tx: Option<&'db Transaction>,
        settings: SqlSettings,
        ctes: BTreeMap<String, CteResult>,
        security_context: Option<SecurityContext>,
        session_gucs: Arc<HashMap<String, String>>,
        routine_vars: Arc<BTreeMap<String, SqlValue>>,
        routine_slots: Option<RoutineSlotBinding>,
        routine_ir: Option<usize>,
        ir_owned_statement: bool,
        bound_context_cache: SharedBoundContextCache,
        index_lookup_cache: IndexLookupCache,
        rowid_index_lookup_cache: RowIdIndexLookupCache,
        runtime: Option<Arc<dyn SqlSessionRuntime>>,
        cancellation: CancellationToken,
        fts_limits: bicdb_core::FtsQueryLimits,
    ) -> Self {
        let session_gucs =
            bind_trusted_security_settings_shared(session_gucs, security_context.as_ref());
        Self {
            db,
            tx,
            settings,
            ctes,
            security_context,
            runtime,
            session_gucs,
            routine_vars,
            routine_slots,
            routine_ir,
            memo_operand_types: false,
            ir_owned_statement,
            outer_row: None,
            index_lookup_cache,
            rowid_index_lookup_cache,
            record_lookup_cache: Rc::new(RefCell::new(BTreeMap::new())),
            bound_var_cache: RefCell::new(None),
            bound_context_cache,
            cancellation,
            fts_limits,
        }
    }

    pub(crate) fn with_ctes_and_context(
        db: &'db BicDb,
        settings: SqlSettings,
        ctes: BTreeMap<String, CteResult>,
        security_context: Option<SecurityContext>,
        session_gucs: Arc<HashMap<String, String>>,
    ) -> Self {
        let session_gucs =
            bind_trusted_security_settings_shared(session_gucs, security_context.as_ref());
        Self {
            db,
            tx: None,
            settings,
            ctes,
            security_context,
            runtime: None,
            session_gucs,
            routine_vars: Arc::new(BTreeMap::new()),
            routine_slots: None,
            routine_ir: None,
            memo_operand_types: false,
            ir_owned_statement: false,
            outer_row: None,
            index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            rowid_index_lookup_cache: Rc::new(RefCell::new(FxHashMap::default())),
            record_lookup_cache: Rc::new(RefCell::new(BTreeMap::new())),
            bound_var_cache: RefCell::new(None),
            bound_context_cache: Rc::new(RefCell::new(BTreeMap::new())),
            cancellation: CancellationToken::uncancelable(),
            fts_limits: bicdb_core::FtsQueryLimits::UNLIMITED,
        }
    }

    pub(crate) fn with_bound_context_cache(mut self, cache: SharedBoundContextCache) -> Self {
        self.bound_context_cache = cache;
        self
    }

    pub(crate) fn with_routine_ir(mut self, routine_ir: Option<usize>) -> Self {
        self.routine_ir = routine_ir;
        self
    }

    pub(crate) fn with_operand_type_memo(mut self, enabled: bool) -> Self {
        self.memo_operand_types = enabled;
        self
    }

    pub(crate) fn with_ir_owned_statement(mut self, owned: bool) -> Self {
        self.ir_owned_statement = owned;
        self
    }

    pub fn with_session_gucs(mut self, session_gucs: Arc<HashMap<String, String>>) -> Self {
        self.session_gucs =
            bind_trusted_security_settings_shared(session_gucs, self.security_context.as_ref());
        self
    }

    #[cfg(test)]
    pub(crate) fn with_routine_vars(mut self, routine_vars: BTreeMap<String, SqlValue>) -> Self {
        self.routine_vars = Arc::new(routine_vars);
        self
    }

    pub(crate) fn with_shared_routine_vars(
        mut self,
        routine_vars: Arc<BTreeMap<String, SqlValue>>,
    ) -> Self {
        self.routine_vars = routine_vars;
        self
    }

    pub(crate) fn with_routine_slots(mut self, slots: Option<RoutineSlotBinding>) -> Self {
        self.routine_slots = slots;
        self
    }

    pub(crate) fn with_optional_runtime(
        mut self,
        runtime: Option<Arc<dyn SqlSessionRuntime>>,
    ) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outer_slot_row(mut self, outer_row: OuterSlotRow) -> Self {
        self.outer_row = Some(outer_row);
        self
    }

    pub(crate) fn with_index_lookup_cache(mut self, cache: IndexLookupCache) -> Self {
        self.index_lookup_cache = cache;
        self
    }

    pub(crate) fn with_rowid_index_lookup_cache(mut self, cache: RowIdIndexLookupCache) -> Self {
        self.rowid_index_lookup_cache = cache;
        self
    }

    pub(crate) fn with_record_lookup_cache(mut self, cache: RecordLookupCache) -> Self {
        self.record_lookup_cache = cache;
        self
    }

    pub fn with_fts_limits(mut self, limits: bicdb_core::FtsQueryLimits) -> Self {
        self.fts_limits = limits;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub(crate) fn with_transaction(mut self, tx: &'db Transaction) -> Self {
        self.tx = Some(tx);
        self
    }

    pub(crate) fn inherit_transaction(&self, engine: SqlEngine<'db>) -> SqlEngine<'db> {
        let mut engine = engine
            .with_index_lookup_cache(self.index_lookup_cache.clone())
            .with_rowid_index_lookup_cache(self.rowid_index_lookup_cache.clone())
            .with_record_lookup_cache(self.record_lookup_cache.clone())
            .with_optional_runtime(self.runtime.clone());
        if engine.outer_row.is_none() {
            engine.outer_row.clone_from(&self.outer_row);
        }
        match self.tx {
            Some(tx) => engine.with_transaction(tx),
            None => engine,
        }
    }

    pub fn execute(&self, sql: &str) -> Result<SqlResult> {
        let mut profile = SqlProfileScope::new(sql);
        let _schema_cache = SqlSchemaCacheScope::new();
        let result = self.execute_inner(sql);
        profile.finish(&result);
        result
    }

    pub(crate) fn execute_inner(&self, sql: &str) -> Result<SqlResult> {
        self.check_cancellation()?;
        if split_sql_statements(sql).is_empty() {
            return Ok(SqlResult::empty(Vec::new()));
        }
        if let Some(result) = self.execute_builtin(sql)? {
            return Ok(result);
        }

        let statements = parse_statements(sql)?;
        let [statement] = statements.as_slice() else {
            return Err(SqlError::Unsupported(
                "expected exactly one SQL statement".to_string(),
            ));
        };

        match statement {
            Statement::Query(query) => {
                // Centralized authorization: every base table the query reads is
                // authorized before any physical path is chosen, so no optimizer
                // fast path can read an unauthorized relation. Per-path gates
                // remain as defense in depth.
                self.authorize_read_relations(query)?;
                self.execute_query(query)
            }
            Statement::ShowVariable { variable, .. } => self.execute_show(variable),
            Statement::Explain {
                analyze, statement, ..
            } => self.execute_explain(statement, *analyze),
            other => Err(SqlError::Unsupported(format!(
                "only SELECT and SHOW are supported, got {other}"
            ))),
        }
    }

    pub(crate) fn check_cancellation(&self) -> Result<()> {
        self.cancellation.check().map_err(SqlError::from)
    }

    pub(crate) fn execute_builtin(&self, sql: &str) -> Result<Option<SqlResult>> {
        let normalized = normalize_sql(sql);
        if let Some(result) =
            active_record_column_definitions_fast_path(self.db_ref(), &normalized)?
        {
            return Ok(Some(result));
        }
        if let Some(result) = active_record_primary_key_fast_path(self.db_ref(), &normalized)? {
            return Ok(Some(result));
        }
        if let Some(result) = active_record_foreign_keys_fast_path(self.db_ref(), &normalized)? {
            return Ok(Some(result));
        }
        if let Some(result) = active_record_index_invalid_fast_path(self.db_ref(), &normalized)? {
            return Ok(Some(result));
        }
        match normalized.as_str() {
            "select 1" => Ok(Some(SqlResult::new(
                vec!["?column?".to_string()],
                vec![vec![SqlValue::Int(1)]],
            ))),
            "select current_database"
            | "select current_database()"
            | "select pg_catalog.current_database()" => Ok(Some(SqlResult::new(
                vec!["current_database".to_string()],
                vec![vec![SqlValue::String(current_database_from_gucs(
                    &self.session_gucs,
                ))]],
            ))),
            "select current_schema"
            | "select current_schema()"
            | "select pg_catalog.current_schema()" => Ok(Some(SqlResult::new(
                vec!["current_schema".to_string()],
                vec![vec![SqlValue::String("public".to_string())]],
            ))),
            "select version()" | "select pg_catalog.version()" => Ok(Some(SqlResult::new(
                vec!["version".to_string()],
                vec![vec![SqlValue::String(sql_version_banner(
                    &self.session_gucs,
                ))]],
            ))),
            "select bicdb_version()" | "select pg_catalog.bicdb_version()" => {
                Ok(Some(SqlResult::new(
                    vec!["bicdb_version".to_string()],
                    vec![vec![SqlValue::String(BICDB_VERSION.to_string())]],
                )))
            }
            "select pg_catalog.pg_client_encoding()" => Ok(Some(SqlResult::new(
                vec!["pg_client_encoding".to_string()],
                vec![vec![SqlValue::String("UTF8".to_string())]],
            ))),
            "select sum(xact_commit + xact_rollback) from pg_stat_database"
            | "select sum(xact_commit + xact_rollback) from pg_catalog.pg_stat_database" => {
                Ok(Some(SqlResult::new(
                    vec!["sum".to_string()],
                    vec![vec![SqlValue::Int(0)]],
                )))
            }
            "select pg_catalog.set_config('search_path', '', false)" => Ok(Some(SqlResult::new(
                vec!["set_config".to_string()],
                vec![vec![SqlValue::String(String::new())]],
            ))),
            "show server_version" => Ok(Some(SqlResult::new(
                vec!["server_version".to_string()],
                vec![vec![SqlValue::String(
                    postgres_compatibility_version_from_gucs(&self.session_gucs).to_string(),
                )]],
            ))),
            "show server_version_num" => Ok(Some(SqlResult::new(
                vec!["server_version_num".to_string()],
                vec![vec![SqlValue::String(
                    self.session_gucs
                        .get("server_version_num")
                        .map(String::as_str)
                        .unwrap_or(POSTGRES_COMPATIBILITY_VERSION_NUM)
                        .to_string(),
                )]],
            ))),
            "show bicdb_version" => Ok(Some(SqlResult::new(
                vec!["bicdb_version".to_string()],
                vec![vec![SqlValue::String(BICDB_VERSION.to_string())]],
            ))),
            "show application_name" => {
                let value = self
                    .session_gucs
                    .get("application_name")
                    .cloned()
                    .unwrap_or_default();
                Ok(Some(SqlResult::new(
                    vec!["application_name".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show client_encoding" => Ok(Some(SqlResult::new(
                vec!["client_encoding".to_string()],
                vec![vec![SqlValue::String("UTF8".to_string())]],
            ))),
            "show client_min_messages" => {
                let value = self
                    .session_gucs
                    .get("client_min_messages")
                    .cloned()
                    .unwrap_or_else(|| "notice".to_string());
                Ok(Some(SqlResult::new(
                    vec!["client_min_messages".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show transaction_isolation" | "show transaction.isolation.level" => {
                Ok(Some(SqlResult::new(
                    vec!["transaction_isolation".to_string()],
                    vec![vec![SqlValue::String("read committed".to_string())]],
                )))
            }
            "show datestyle" => {
                let value = self
                    .session_gucs
                    .get("datestyle")
                    .cloned()
                    .unwrap_or_else(|| "ISO, MDY".to_string());
                Ok(Some(SqlResult::new(
                    vec!["datestyle".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show default_table_access_method" => {
                let value = self
                    .session_gucs
                    .get("default_table_access_method")
                    .cloned()
                    .unwrap_or_else(|| "heap".to_string());
                Ok(Some(SqlResult::new(
                    vec!["default_table_access_method".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show default_tablespace" => {
                let value = self
                    .session_gucs
                    .get("default_tablespace")
                    .cloned()
                    .unwrap_or_default();
                Ok(Some(SqlResult::new(
                    vec!["default_tablespace".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show extra_float_digits" => {
                let value = self
                    .session_gucs
                    .get("extra_float_digits")
                    .cloned()
                    .unwrap_or_else(|| "1".to_string());
                Ok(Some(SqlResult::new(
                    vec!["extra_float_digits".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show restrict_nonsystem_relation_kind" => {
                let value = self
                    .session_gucs
                    .get("restrict_nonsystem_relation_kind")
                    .cloned()
                    .unwrap_or_default();
                Ok(Some(SqlResult::new(
                    vec!["restrict_nonsystem_relation_kind".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show standard_conforming_strings" => Ok(Some(SqlResult::new(
                vec!["standard_conforming_strings".to_string()],
                vec![vec![SqlValue::String("on".to_string())]],
            ))),
            "show intervalstyle" => {
                let value = self
                    .session_gucs
                    .get("intervalstyle")
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string());
                Ok(Some(SqlResult::new(
                    vec!["intervalstyle".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show statement_timeout" => {
                let value = self
                    .session_gucs
                    .get("statement_timeout")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                Ok(Some(SqlResult::new(
                    vec!["statement_timeout".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show lock_timeout" => {
                let value = self
                    .session_gucs
                    .get("lock_timeout")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                Ok(Some(SqlResult::new(
                    vec!["lock_timeout".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show idle_in_transaction_session_timeout" => {
                let value = self
                    .session_gucs
                    .get("idle_in_transaction_session_timeout")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                Ok(Some(SqlResult::new(
                    vec!["idle_in_transaction_session_timeout".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show idle_session_timeout" => {
                let value = self
                    .session_gucs
                    .get("idle_session_timeout")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                Ok(Some(SqlResult::new(
                    vec!["idle_session_timeout".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show transaction_timeout" => {
                let value = self
                    .session_gucs
                    .get("transaction_timeout")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                Ok(Some(SqlResult::new(
                    vec!["transaction_timeout".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show row_security" => {
                let value = self
                    .session_gucs
                    .get("row_security")
                    .cloned()
                    .unwrap_or_else(|| "on".to_string());
                Ok(Some(SqlResult::new(
                    vec!["row_security".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show role" => {
                let value = self
                    .session_gucs
                    .get(CURRENT_ROLE_GUC)
                    .cloned()
                    .unwrap_or_else(|| "none".to_string());
                Ok(Some(SqlResult::new(
                    vec!["role".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show session_authorization" => Ok(Some(SqlResult::new(
                vec!["session_authorization".to_string()],
                vec![vec![SqlValue::String(session_user_from_gucs(
                    &self.session_gucs,
                ))]],
            ))),
            "show search_path" => {
                let value = self
                    .session_gucs
                    .get("search_path")
                    .cloned()
                    .unwrap_or_else(|| "\"$user\", public".to_string());
                Ok(Some(SqlResult::new(
                    vec!["search_path".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show synchronize_seqscans" => {
                let value = self
                    .session_gucs
                    .get("synchronize_seqscans")
                    .cloned()
                    .unwrap_or_else(|| "on".to_string());
                Ok(Some(SqlResult::new(
                    vec!["synchronize_seqscans".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show timezone" => {
                let value = self
                    .session_gucs
                    .get("timezone")
                    .cloned()
                    .unwrap_or_else(|| "UTC".to_string());
                Ok(Some(SqlResult::new(
                    vec!["timezone".to_string()],
                    vec![vec![SqlValue::String(value)]],
                )))
            }
            "show integer_datetimes" => Ok(Some(SqlResult::new(
                vec!["integer_datetimes".to_string()],
                vec![vec![SqlValue::String("on".to_string())]],
            ))),
            "show max_identifier_length" => Ok(Some(SqlResult::new(
                vec!["max_identifier_length".to_string()],
                vec![vec![SqlValue::String("63".to_string())]],
            ))),
            "select table_name from information_schema.tables" => {
                let mut rows = catalog_table_names(self.db_ref())
                    .into_iter()
                    .map(|collection| vec![SqlValue::String(collection)])
                    .collect::<Vec<_>>();
                rows.sort_by(|left, right| left[0].to_cell().cmp(&right[0].to_cell()));
                Ok(Some(SqlResult::new(vec!["table_name".to_string()], rows)))
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn execute_show(&self, variable: &[Ident]) -> Result<SqlResult> {
        let name = variable
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>()
            .join(".");
        match name.to_ascii_lowercase().as_str() {
            "bicdb.vector_search" => {
                let mode = match self.settings.vector_search {
                    VectorSearchMode::Exact => "exact",
                    VectorSearchMode::Ann => "ann",
                };
                return Ok(SqlResult::new(
                    vec![name],
                    vec![vec![SqlValue::String(mode.to_string())]],
                ));
            }
            "bicdb.ef_search" => {
                return Ok(SqlResult::new(
                    vec![name],
                    vec![vec![SqlValue::Int(self.settings.ef_search as i64)]],
                ));
            }
            _ => {}
        }
        let key = name.to_ascii_lowercase();
        if let Some(value) = self.session_gucs.get(&key) {
            return Ok(SqlResult::new(
                vec![name],
                vec![vec![SqlValue::String(value.clone())]],
            ));
        }
        if let Some(value) = default_session_guc(&key) {
            return Ok(SqlResult::new(
                vec![name],
                vec![vec![SqlValue::String(value.to_string())]],
            ));
        }
        self.execute_builtin(&format!("show {name}"))?
            .ok_or_else(|| SqlError::Unsupported(format!("SHOW {name} is not supported")))
    }

    pub(crate) fn execute_query(&self, query: &Query) -> Result<SqlResult> {
        self.check_cancellation()?;
        if let Some(with) = &query.with {
            return self.execute_query_with_ctes(query, with);
        }

        if !query.locks.is_empty() {
            return self.execute_locking_query(query);
        }

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            SetExpr::Query(_) | SetExpr::SetOperation { .. } | SetExpr::Values(_) => {
                return self.execute_set_query(query);
            }
            _ => {
                return Err(SqlError::Unsupported(
                    "only simple SELECT queries are supported".to_string(),
                ));
            }
        };
        validate_window_functions(select, query)?;
        self.validate_money_aggregate_signatures(select, query)?;
        let has_windows = select_has_window_functions(select, query)?;

        if select.from.is_empty() {
            if has_windows {
                return self.execute_window_query_without_from(select, query);
            }
            return self.execute_select_without_from(select);
        }

        if has_windows {
            return self.execute_row_query(select, query);
        }

        if query_group_aggregate_functions(select, query)?
            .iter()
            .any(|function| function.filter.is_some())
        {
            return self.execute_row_query(select, query);
        }

        if select.from.len() != 1 {
            return self.execute_row_query(select, query);
        }
        // Answered from the row count, without reading a row — see
        // `try_count_star_fast_path` for the guards that make that equivalent.
        if let Some(result) = self.try_count_star_fast_path(&select.from[0], select, query)? {
            return Ok(result);
        }
        // Full-text boolean count: `WHERE tsv @@ q` answered by intersecting
        // dense posting blocks — no row fetch, no per-row re-tokenization.
        if let Some(result) = self.try_fts_count_fast_path(&select.from[0], select, query)? {
            return Ok(result);
        }
        // Unranked full-text SELECT: `WHERE tsv @@ q` rows fetched by the
        // candidate primary keys the posting blocks enumerate.
        if let Some(result) = self.try_fts_select_fast_path(&select.from[0], select, query)? {
            return Ok(result);
        }
        if select.distinct.is_some() {
            // DISTINCT over a streamed sort: one row per distinct key, no
            // input materialization. Best-effort like every streaming path.
            match self.try_streaming_distinct(&select.from[0], select, query) {
                Ok(Some(result)) => return Ok(result),
                Err(error) if error.is_resource_limit() || error.is_query_interruption() => {
                    return Err(error);
                }
                Ok(None) | Err(_) => {}
            }
            return self.execute_row_query(select, query);
        }
        let from = &select.from[0];
        if from.joins.is_empty() && !has_group_by(select)? {
            if let Some(result) = self.try_single_table_indexed_extreme_aggregate(from, select)? {
                return Ok(result);
            }
        }
        // Simple aggregates folded over a streamed scan: COUNT/SUM/MIN/MAX and
        // friends with an optional WHERE never materialize the table. Errors
        // fall through to the general path, same best-effort contract as the
        // other streaming attempts (it either answers correctly or raises the
        // same error itself).
        if let Ok(Some(result)) = self.try_streaming_simple_aggregates(from, select, query) {
            return Ok(result);
        }
        // Bound-plan cache fast path (flag-gated; byte-identical to the fused
        // `execute_row_query` path when it returns `Some`, and inert otherwise).
        // When `BICDB_PLAN_CACHE` is off this is a single relaxed atomic load and
        // the original path below is taken unchanged.
        if plan_cache::enabled() || (ir_plan_cache::enabled() && self.ir_owned_statement) {
            if let Some(result) = self.try_cached_point_lookup_select(from, select, query)? {
                return Ok(result);
            }
        }
        if !from.joins.is_empty()
            || has_group_by(select)?
            || select
                .selection
                .as_ref()
                .is_some_and(expr_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(select_item_needs_row_evaluator)
            || select
                .projection
                .iter()
                .any(|item| select_item_is_whole_row_reference(item, &from.relation))
            || query
                .order_by
                .as_ref()
                .is_some_and(order_by_needs_row_evaluator)
            || !matches!(from.relation, TableFactor::Table { args: None, .. })
        {
            // The general row path materializes its whole input. Before taking
            // it, try streaming: the only condition above that the streaming
            // path cannot handle is *not* a row-evaluator selection — it
            // evaluates predicates per row through the same
            // `row_from_record` + `eval_row_truth_typed` machinery this path
            // would use. Everything else (joins, GROUP BY, projections or
            // ORDER BY needing the evaluator, whole-row references, non-plain
            // relations) it re-checks and declines.
            // Best-effort errors mean this path cannot serve the query, so the
            // general path below runs and produces either the right answer or
            // the right error. Planning failures are discarded for exactly
            // that reason — e.g. `WHERE pk = (SELECT ...)` where the planner
            // must evaluate the subquery to a constant to attempt a point
            // lookup. Resource limits and query interruptions from spillable
            // operators are the exception: they must propagate so fallback
            // cannot bypass a hard bound or resume work after cancellation.
            // Ranked full-text top-k: ORDER BY ts_rank(...) DESC LIMIT k
            // scored from index postings — no row fetch, no re-tokenization.
            if let Ok(Some(result)) = self.try_ranked_fts_topk(from, select, query) {
                return Ok(result);
            }
            if let Ok(Some(result)) = self.try_streaming_row_query(from, select, query) {
                return Ok(result);
            }
            // ORDER BY declines the projection streamer above, but the
            // external sort evaluates the same row predicates — a
            // row-evaluator WHERE with a sortable ORDER BY still streams.
            match self.try_streaming_external_sort_query(from, select, query) {
                Ok(Some(result)) => return Ok(result),
                Err(error) if error.is_resource_limit() || error.is_query_interruption() => {
                    return Err(error);
                }
                Ok(None) | Err(_) => {}
            }
            // Grouped aggregates spill through the same sorter and fold
            // adjacent groups during the merge.
            match self.try_streaming_group_by(from, select, query) {
                Ok(Some(result)) => return Ok(result),
                Err(error) if error.is_resource_limit() || error.is_query_interruption() => {
                    return Err(error);
                }
                Ok(None) | Err(_) => {}
            }
            return self.execute_row_query(select, query);
        }

        let TableFactor::Table { name, alias, .. } = &from.relation else {
            return Err(SqlError::Unsupported(
                "only collection table scans are supported".to_string(),
            ));
        };
        let collection = relation_name(name)?;
        let alias_name = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| {
                collection
                    .rsplit('.')
                    .next()
                    .unwrap_or(&collection)
                    .to_string()
            });

        if self.cte(&collection).is_some() {
            return self.execute_row_query(select, query);
        }

        if is_virtual_table(&collection) {
            return self.execute_virtual_table(select, query, &collection);
        }

        if let Some(view) = load_view(self.db_ref(), &collection)? {
            self.require_relation_privilege(&collection, "SELECT")?;
            let mut ctes = self.ctes.clone();
            ctes.insert(
                view_key(&view.name),
                self.materialize_view_with_selection(
                    &view,
                    &alias_name,
                    select.selection.as_ref(),
                )?,
            );
            return self
                .inherit_transaction(SqlEngine::with_ctes_and_context(
                    self.db_ref(),
                    self.settings,
                    ctes,
                    self.security_context.clone(),
                    self.session_gucs.clone(),
                ))
                .with_shared_routine_vars(self.routine_vars.clone())
                .with_cancellation(self.cancellation.clone())
                .execute_row_query(select, query);
        }

        if let Some(sequence) = load_sequence(self.db_ref(), &collection)? {
            let role = current_user_from_gucs(&self.session_gucs);
            if !role_can_use_sequence(self.db_ref(), &role, &sequence, &["SELECT"])? {
                return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                    "permission denied for sequence {}",
                    sequence.name
                ))));
            }
            return self.execute_sequence_relation(select, query, sequence);
        }

        let collection = resolve_session_relation_name(self.db_ref(), &collection)?;
        self.require_relation_privilege(&collection, "SELECT")?;

        if let Some(records) = self.load_ann_records_if_enabled(&collection, select, query)? {
            let schema = load_schema(self.db_ref(), &collection)?;
            let projection = Projection::from_select_items(&select.projection, schema.as_ref())?;
            let mut rows = Vec::with_capacity(records.len());
            for (idx, record) in records.iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                rows.push(projection.row(record)?);
            }
            let column_types = projection.column_types();
            let column_metadata =
                projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
            return Ok(SqlResult::new(projection.columns, rows)
                .with_column_types(column_types)
                .with_column_metadata(column_metadata));
        }

        self.reject_encrypted_predicates(&collection, select.selection.as_ref())?;
        let schema = load_schema(self.db_ref(), &collection)?;
        if let Some(pg_type) = query
            .order_by
            .as_ref()
            .and_then(|order_by| match &order_by.kind {
                OrderByKind::Expressions(expressions) => Some(expressions),
                _ => None,
            })
            .and_then(|expressions| {
                expressions.iter().find_map(|order| {
                    type_without_comparison_operators(
                        projected_expr_pg_type(&order.expr, schema.as_ref()).as_deref(),
                    )
                })
            })
        {
            return Err(SqlError::undefined_function(format!(
                "could not identify an ordering operator for type {pg_type}"
            )));
        }
        if let Some(result) = self.execute_indexed_extreme_aggregate(
            &collection,
            &alias_name,
            select,
            schema.as_ref(),
        )? {
            return Ok(result);
        }
        let plan = self.plan_query(&collection, &alias_name, select, query)?;
        // STREAMING FAST PATH: a full scan whose result needs no global view of
        // its input — no ORDER BY, no aggregates, no DISTINCT — can emit each
        // row and forget it. Without this, `SELECT id FROM t LIMIT 5` on a
        // 2,000,000-row paged table materialized every row (measured 3760 MiB);
        // the same query now runs inside the buffer-pool envelope.
        //
        // Every other shape still materializes, because it genuinely needs to:
        // sorting, grouping and DISTINCT are not one-pass over an unordered
        // input. Those are Phase 5's external-sort and spill work.
        // Best-effort, as at the other call site: any failure falls through to
        // the materializing path below.
        if let Ok(Some(result)) = self.try_streaming_projection(
            &collection,
            &alias_name,
            select,
            query,
            &plan,
            schema.as_ref(),
        ) {
            return Ok(result);
        }
        // EXTERNAL SORT: full-table ORDER BY over normalizable keys streams
        // the input into bounded sorted runs and merges — only the OUTPUT
        // materializes. Fidelity failures (an order key the normalizer cannot
        // encode, a value the spill codec declines) abandon the attempt and
        // the materializing path below answers. Resource limits and query
        // interruptions propagate so fallback cannot bypass configured bounds
        // or resume work after cancellation.
        match self.try_streaming_external_sort(
            &collection,
            &alias_name,
            select,
            query,
            &plan,
            schema.as_ref(),
        ) {
            Ok(Some(result)) => return Ok(result),
            Err(error) if error.is_resource_limit() || error.is_query_interruption() => {
                return Err(error);
            }
            Ok(None) | Err(_) => {}
        }
        let mut records = self.load_records_for_plan(&collection, &plan)?;
        if let Some(selection) = &select.selection {
            let predicate_env = self.from_relation_columns(&select.from).unwrap_or_default();
            let mut filtered = Vec::new();
            for (idx, record) in records.into_iter().enumerate() {
                if idx % 1024 == 0 {
                    self.check_cancellation()?;
                }
                let row = row_from_record(&collection, &collection, schema.as_ref(), &record)?;
                if self
                    .eval_row_truth_typed(&row, selection, &predicate_env)?
                    .unwrap_or(false)
                {
                    filtered.push(record);
                }
            }
            records = filtered;
        }
        self.check_cancellation()?;

        if has_aggregates(&select.projection) {
            self.check_cancellation()?;
            // LIMIT/OFFSET apply to the aggregate's own result row. Returning
            // straight from `execute_aggregates` skipped `apply_limit`
            // entirely, so `SELECT COUNT(*) FROM t OFFSET 1` returned the
            // count where PostgreSQL returns no rows.
            let mut result = execute_aggregates(&select.projection, &records, schema.as_ref())?;
            apply_limit(&mut result.rows, query)?;
            return Ok(result);
        }

        self.check_cancellation()?;
        apply_order_by_cancellable(
            &mut records,
            query.order_by.as_ref(),
            schema.as_ref(),
            &self.cancellation,
        )?;
        self.check_cancellation()?;
        apply_limit(&mut records, query)?;

        let projection = Projection::from_select_items(&select.projection, schema.as_ref())?;
        let mut rows = Vec::with_capacity(records.len());
        for (idx, record) in records.iter().enumerate() {
            if idx % 1024 == 0 {
                self.check_cancellation()?;
            }
            rows.push(projection.row(record)?);
        }
        let column_types = projection.column_types();
        let column_metadata =
            projection.column_metadata(self.db_ref(), &collection, schema.as_ref());
        Ok(SqlResult::new(projection.columns, rows)
            .with_column_types(column_types)
            .with_column_metadata(column_metadata))
    }
}

pub(crate) fn aggregate_filter_from_expr(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Function(function) if is_aggregate_function(function) => function.filter.as_deref(),
        Expr::Cast { expr, .. } | Expr::Nested(expr) | Expr::Collate { expr, .. } => {
            aggregate_filter_from_expr(expr)
        }
        _ => None,
    }
}

pub(crate) fn scalar_subquery_value(result: SqlResult) -> Result<SqlValue> {
    if result.columns.len() != 1 {
        return Err(SqlError::InvalidSql(
            "scalar subquery must return one column".to_string(),
        ));
    }
    match result.rows.as_slice() {
        [] => Ok(SqlValue::Null),
        [row] => Ok(row.first().cloned().unwrap_or(SqlValue::Null)),
        _ => Err(SqlError::InvalidSql(
            "scalar subquery returned more than one row".to_string(),
        )),
    }
}

pub(crate) fn subquery_values(result: SqlResult) -> Result<Vec<SqlValue>> {
    if result.columns.len() != 1 {
        return Err(SqlError::InvalidSql(
            "IN subquery must return one column".to_string(),
        ));
    }
    Ok(result
        .rows
        .into_iter()
        .map(|row| row.into_iter().next().unwrap_or(SqlValue::Null))
        .collect())
}

pub(crate) fn extend_outer_slot_context(
    existing: Option<&OuterSlotRow>,
    columns: &[String],
    row: &[SqlValue],
) -> OuterSlotRow {
    let Some(existing) = existing else {
        return OuterSlotRow::from_slot(columns, row);
    };
    let values = merge_slot_rows(&existing.values, existing.columns(), row, columns);
    let columns = merge_row_set_columns(existing.columns().to_vec(), columns.to_vec());
    OuterSlotRow::new(columns, values)
}

pub(crate) fn extend_outer_slot_context_from_sql_row(
    existing: Option<&OuterSlotRow>,
    row: &SqlRow,
) -> OuterSlotRow {
    let row = OuterSlotRow::from_sql_row(row);
    extend_outer_slot_context(existing, row.columns(), &row.values)
}

impl<'db> SqlEngine<'db> {
    pub(crate) fn materialize_view_with_selection(
        &self,
        view: &ViewSchema,
        alias: &str,
        selection: Option<&Expr>,
    ) -> Result<CteResult> {
        if self.cte(&view.name).is_some() {
            return Err(SqlError::Unsupported(format!(
                "recursive view \"{}\" is not supported",
                view.name
            )));
        }
        if is_postgres_foreign_keys_catalog_view(view) {
            return materialize_postgres_foreign_keys_view(self.db_ref(), view, alias, selection);
        }
        if is_postgres_constraints_catalog_view(view) {
            return materialize_postgres_constraints_view(self.db_ref(), view, alias, selection);
        }
        if is_postgres_partitioned_tables_catalog_view(view) {
            return materialize_postgres_partitioned_tables_view(
                self.db_ref(),
                view,
                alias,
                selection,
            );
        }
        if is_postgres_partitions_catalog_view(view) {
            return materialize_postgres_partitions_view(self.db_ref(), view, alias, selection);
        }
        if is_postgres_indexes_catalog_view(view) {
            return materialize_postgres_indexes_view(self.db_ref(), view, alias, selection);
        }
        if is_postgres_sequences_catalog_view(view) {
            return materialize_postgres_sequences_view(self.db_ref(), view, alias, selection);
        }
        if is_postgres_triggers_catalog_view(view) {
            return materialize_postgres_triggers_view(self.db_ref(), view, alias, selection);
        }
        let query = parse_query(&view.query_sql)?;
        // A view with NO recorded owner falls back to INVOKER semantics rather
        // than to a definer identity.
        //
        // `owner` is `Option`, and views persisted before ownership tracking
        // carry `None`. Everywhere else that convention reads "treated as
        // owned by the bootstrap role", which is FAIL-CLOSED — only a
        // superuser may alter or drop such a view. Here the same convention
        // was fail-OPEN: `unwrap_or_else(current_role_name)` resolves to the
        // bootstrap role, so an ownerless view executed its body with
        // SUPERUSER read authority over every underlying table. Anyone holding
        // SELECT on the view — a grant that survives an upgrade — read through
        // it as bootstrap.
        //
        // Invoker semantics is the safe fallback rather than a hard refusal:
        // it can never grant more than the caller already has, and a legacy
        // view keeps working for callers who could read the sources
        // themselves. Restore definer behaviour by naming an owner with
        // `ALTER VIEW ... OWNER TO`.
        let definer_owner = match &view.owner {
            Some(owner) if !view.security_invoker => Some(owner.clone()),
            _ => None,
        };
        let result = match definer_owner {
            // PostgreSQL default (definer) semantics: RLS on the underlying
            // tables is decided as the view owner. Policy expressions still
            // evaluate with the session's identity and GUCs.
            Some(owner) => self
                .engine_with_rls_check_as(&owner)
                .execute_query(&query)?,
            None => self.execute_query(&query)?,
        };
        Ok(CteResult::new(
            view.name.clone(),
            view.columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            result.rows,
        ))
    }

    // A sub-engine identical to `self` except that RLS bypass/policy-selection
    // decisions are made as `check_as` (definer-view semantics). Lookup caches
    // hold pre-RLS raw records, so sharing them across identities is safe.
    pub(crate) fn engine_with_rls_check_as(&self, check_as: &str) -> SqlEngine<'db> {
        let mut session_gucs = (*self.session_gucs).clone();
        session_gucs.insert(RLS_CHECK_AS_GUC.to_string(), check_as.to_string());
        let session_gucs = Arc::new(session_gucs);
        SqlEngine {
            db: self.db,
            tx: self.tx,
            settings: self.settings,
            ctes: self.ctes.clone(),
            routine_ir: self.routine_ir,
            memo_operand_types: self.memo_operand_types,
            ir_owned_statement: self.ir_owned_statement,
            security_context: self.security_context.clone(),
            runtime: self.runtime.clone(),
            session_gucs,
            routine_vars: self.routine_vars.clone(),
            routine_slots: self.routine_slots.clone(),
            outer_row: self.outer_row.clone(),
            index_lookup_cache: self.index_lookup_cache.clone(),
            rowid_index_lookup_cache: self.rowid_index_lookup_cache.clone(),
            record_lookup_cache: self.record_lookup_cache.clone(),
            bound_var_cache: RefCell::new(None),
            bound_context_cache: Rc::new(RefCell::new(BTreeMap::new())),
            cancellation: self.cancellation.clone(),
            fts_limits: self.fts_limits,
        }
    }
}

pub(crate) fn is_postgres_foreign_keys_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_foreign_keys") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_constraint")
        && lower.contains("constrained_columns")
        && lower.contains("referenced_columns")
}

pub(crate) fn materialize_postgres_foreign_keys_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let filters = PostgresForeignKeyViewFilters::from_selection(selection, alias, &view.name)?;
    cte_result_from_virtual_rows(view, postgres_foreign_key_view_rows_filtered(db, &filters)?)
}

pub(crate) fn is_postgres_constraints_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_constraints") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_constraint")
        && lower.contains("column_names")
        && lower.contains("table_identifier")
        && lower.contains("pg_get_constraintdef")
}

pub(crate) fn materialize_postgres_constraints_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let filters = PostgresConstraintsViewFilters::from_selection(selection, alias, &view.name)?;
    cte_result_from_virtual_rows(view, postgres_constraints_view_rows_filtered(db, &filters)?)
}

pub(crate) fn cte_result_from_virtual_rows(
    view: &ViewSchema,
    rows: Vec<BTreeMap<String, SqlValue>>,
) -> Result<CteResult> {
    let columns = view
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let rows = rows
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| virtual_cell(&row, column))
                .collect()
        })
        .collect();
    Ok(CteResult::new(view.name.clone(), columns, rows))
}

pub(crate) fn is_postgres_partitioned_tables_catalog_view(view: &ViewSchema) -> bool {
    if !view
        .name
        .eq_ignore_ascii_case("postgres_partitioned_tables")
    {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_partitioned_table")
        && lower.contains("partstrat")
        && lower.contains("key_columns")
}

pub(crate) fn materialize_postgres_partitioned_tables_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let identifiers =
        view_string_filters_from_selection(selection, alias, &view.name, &["identifier"])?;
    let names = view_string_filters_from_selection(selection, alias, &view.name, &["name"])?;
    cte_result_from_virtual_rows(
        view,
        postgres_partitioned_table_view_rows_filtered(
            db,
            view,
            identifiers.as_ref(),
            names.as_ref(),
        )?,
    )
}

pub(crate) fn is_postgres_partitions_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_partitions") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_inherits")
        && lower.contains("parent_identifier")
        && lower.contains("relispartition")
}

pub(crate) fn materialize_postgres_partitions_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let identifiers =
        view_string_filters_from_selection(selection, alias, &view.name, &["identifier"])?;
    let parent_identifiers =
        view_string_filters_from_selection(selection, alias, &view.name, &["parent_identifier"])?;
    let schemas = view_string_filters_from_selection(selection, alias, &view.name, &["schema"])?;
    let names = view_string_filters_from_selection(selection, alias, &view.name, &["name"])?;
    cte_result_from_virtual_rows(
        view,
        postgres_partitions_view_rows_filtered(
            db,
            view,
            identifiers.as_ref(),
            parent_identifiers.as_ref(),
            schemas.as_ref(),
            names.as_ref(),
        )?,
    )
}

pub(crate) fn is_postgres_indexes_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_indexes") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_index")
        && lower.contains("pg_indexes")
        && lower.contains("valid_index")
        && lower.contains("ondisk_size_bytes")
}

pub(crate) fn materialize_postgres_indexes_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let filters = PostgresIndexesViewFilters::from_selection(selection, alias, &view.name)?;
    cte_result_from_virtual_rows(view, postgres_indexes_view_rows_filtered(db, &filters)?)
}

pub(crate) fn is_postgres_sequences_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_sequences") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_sequence") && lower.contains("seq_name") && lower.contains("last_value")
}

pub(crate) fn materialize_postgres_sequences_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let seq_names =
        view_string_filters_from_selection(selection, alias, &view.name, &["seq_name"])?;
    let table_names =
        view_string_filters_from_selection(selection, alias, &view.name, &["table_name"])?;
    let column_names =
        view_string_filters_from_selection(selection, alias, &view.name, &["col_name"])?;
    cte_result_from_virtual_rows(
        view,
        postgres_sequences_view_rows_filtered(
            db,
            seq_names.as_ref(),
            table_names.as_ref(),
            column_names.as_ref(),
        )?,
    )
}

pub(crate) fn is_postgres_triggers_catalog_view(view: &ViewSchema) -> bool {
    if !view.name.eq_ignore_ascii_case("postgres_triggers") {
        return false;
    }
    let lower = view.query_sql.to_ascii_lowercase();
    lower.contains("pg_trigger")
        && lower.contains("trigger_name")
        && lower.contains("table_name")
        && lower.contains("function_name")
}

pub(crate) fn materialize_postgres_triggers_view(
    db: &BicDb,
    view: &ViewSchema,
    alias: &str,
    selection: Option<&Expr>,
) -> Result<CteResult> {
    let filters = PostgresTriggersViewFilters::from_selection(selection, alias, &view.name)?;
    cte_result_from_virtual_rows(view, postgres_triggers_view_rows_filtered(db, &filters)?)
}

/// Gate for content-hash locator strategy plans; ON by default (confirmed
/// +1.4% across a balanced battery with identical semantics), disable with
/// `BICDB_LOCATOR_PLANS=0` for A/B.
/// Budget exhaustion, cancellation, and deadline expiry abort the query
/// outright — every other error keeps the legacy behavior of declining the
/// plan and letting another route try. Without this distinction, a budget
/// refusal would silently fall through to an exhaustive corpus scan, which
/// is precisely what budgets exist to prevent.

#[derive(Clone, Copy)]
pub(crate) enum SpatialJoinOp {
    Intersects,
    DWithin(f64),
}

/// Resolves a bare or qualified column reference against a row set's
/// column names; ambiguous names decline (returning None keeps the generic
/// join path in charge).
pub(crate) fn row_set_column_index(columns: &[String], expr: &Expr) -> Option<usize> {
    let (qualified, bare) = match expr {
        Expr::Identifier(identifier) => (identifier.value.clone(), identifier.value.clone()),
        Expr::CompoundIdentifier(parts) => (
            parts
                .iter()
                .map(|part| part.value.clone())
                .collect::<Vec<_>>()
                .join("."),
            parts.last()?.value.clone(),
        ),
        _ => return None,
    };
    let mut found = None;
    for (index, column) in columns.iter().enumerate() {
        let matches = column.eq_ignore_ascii_case(&qualified)
            || column.eq_ignore_ascii_case(&bare)
            || column.rsplit('.').next().is_some_and(|tail| {
                tail.eq_ignore_ascii_case(&bare)
                    && qualified.contains('.')
                    && column
                        .to_ascii_lowercase()
                        .ends_with(&qualified.to_ascii_lowercase())
            });
        if matches {
            if found.is_some() {
                return None;
            }
            found = Some(index);
        }
    }
    found
}

/// A slot value as a geometry: native geometry values directly, text as
/// WKT or GeoJSON. Unparseable or NULL values simply never match (SQL
/// three-valued logic).
pub(crate) fn slot_geometry(value: Option<&SqlValue>) -> Option<Geometry> {
    match value? {
        SqlValue::Geometry(geometry) => Some(geometry.clone()),
        SqlValue::String(text) => {
            let trimmed = text.trim_start();
            if trimmed.starts_with('{') {
                Geometry::from_geojson_str(text).ok()
            } else {
                Geometry::from_wkt(text).ok()
            }
        }
        _ => None,
    }
}

pub(crate) fn fts_budget_abort(error: &bicdb_core::BicDbError) -> bool {
    matches!(
        error,
        bicdb_core::BicDbError::QueryBudgetExceeded { .. }
            | bicdb_core::BicDbError::QueryCanceled
            | bicdb_core::BicDbError::QueryTimedOut
    )
}

pub(crate) fn locator_plans_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_LOCATOR_PLANS")
            .map(|value| !matches!(value.as_str(), "0" | "off" | "false" | "no"))
            .unwrap_or(true)
    })
}

/// Located (not evaluated) index-analysis results for one statement's WHERE
/// clause against one table: which expressions bind the pk columns, and per
/// catalog index the prefix/range/filter expressions. Execution evaluates
/// these per call; deriving them is the repeated AST-walk cost this caches.
#[derive(Clone)]
pub(crate) struct LocatorStrategy {
    #[allow(dead_code)] // pk path currently delegates; kept for the next slice
    pub(crate) pk_exprs: Vec<Option<Expr>>,
    pub(crate) indexes: Vec<LocatorIndexPlan>,
    /// Whether the collection carries any index that is not a B-tree: only
    /// then can the array/jsonb/full-text candidates answer, so a table with
    /// B-trees alone skips those three catalog walks per execution.
    pub(crate) has_non_btree_indexes: bool,
}

#[derive(Clone)]
pub(crate) struct LocatorIndexPlan {
    pub(crate) name: String,
    pub(crate) fields: Vec<IndexField>,
    pub(crate) prefix_exprs: Vec<Expr>,
    pub(crate) range_bounds: Vec<(BinaryOperator, Expr)>,
    pub(crate) filters: Vec<(usize, Expr)>,
    /// The catalog entry, for the probes that merge this transaction's
    /// pending writes into the index answer.
    pub(crate) definition: IndexDefinition,
}

/// Locator strategies by routine IR node (see `SqlEngine::ir_plan_node_key`):
/// a routine-owned statement's WHERE never changes, so its strategy needs no
/// hash or content verify per execution — only the index-catalog length,
/// which the IR key's generation word does not cover.
type LocatorStrategyNodeCache =
    FxHashMap<(usize, u64, usize, usize), (usize, std::rc::Rc<LocatorStrategy>)>;

thread_local! {
    static LOCATOR_STRATEGY_NODES: RefCell<LocatorStrategyNodeCache> =
        RefCell::new(FxHashMap::default());
}

const LOCATOR_STRATEGY_NODES_MAX: usize = 4096;

pub(crate) fn locator_strategy_node_get(
    key: (usize, u64, usize, usize),
    index_len: usize,
) -> Option<std::rc::Rc<LocatorStrategy>> {
    LOCATOR_STRATEGY_NODES.with(|cache| {
        cache
            .borrow()
            .get(&key)
            .filter(|(cached_len, _)| *cached_len == index_len)
            .map(|(_, strategy)| strategy.clone())
    })
}

pub(crate) fn locator_strategy_node_set(
    key: (usize, u64, usize, usize),
    index_len: usize,
    strategy: std::rc::Rc<LocatorStrategy>,
) {
    LOCATOR_STRATEGY_NODES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= LOCATOR_STRATEGY_NODES_MAX {
            cache.clear();
        }
        cache.insert(key, (index_len, strategy));
    });
}

struct LocatorStrategyEntry {
    schema_generation: u64,
    index_len: usize,
    selection: Expr,
    table: String,
    alias: String,
    strategy: std::rc::Rc<LocatorStrategy>,
}

thread_local! {
    static LOCATOR_STRATEGIES: RefCell<FxHashMap<(usize, u64), LocatorStrategyEntry>> =
        RefCell::new(FxHashMap::default());
}

fn locator_strategy_memo(
    db: &BicDb,
    selection: &Expr,
    table: &str,
    alias: &str,
    build: impl FnOnce() -> Result<LocatorStrategy>,
) -> Result<std::rc::Rc<LocatorStrategy>> {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    selection.hash(&mut hasher);
    table.hash(&mut hasher);
    alias.hash(&mut hasher);
    let key = (db as *const BicDb as usize, hasher.finish());
    let schema_generation = db.collection_generation(SCHEMA_COLLECTION);
    let index_len = db.index_catalog_len();
    let hit = LOCATOR_STRATEGIES.with(|memo| {
        memo.borrow().get(&key).and_then(|entry| {
            // Full-content verify: hash collisions and recycled addresses can
            // never serve a wrong plan.
            (entry.schema_generation == schema_generation
                && entry.index_len == index_len
                && entry.table == table
                && entry.alias == alias
                && entry.selection == *selection)
                .then(|| entry.strategy.clone())
        })
    });
    if let Some(strategy) = hit {
        return Ok(strategy);
    }
    let strategy = std::rc::Rc::new(build()?);
    LOCATOR_STRATEGIES.with(|memo| {
        let mut memo = memo.borrow_mut();
        if memo.len() >= 1024 {
            memo.clear();
        }
        memo.insert(
            key,
            LocatorStrategyEntry {
                schema_generation,
                index_len,
                selection: selection.clone(),
                table: table.to_string(),
                alias: alias.to_string(),
                strategy: strategy.clone(),
            },
        );
    });
    Ok(strategy)
}
