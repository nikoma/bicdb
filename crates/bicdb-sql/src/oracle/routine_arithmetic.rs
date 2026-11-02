//! Logical DATE provenance and typed operations for the version-3 compiler.
use super::*;

fn returns_date(routine: &RoutineSchema) -> Result<bool> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let [Statement::CreateFunction(function)] = statements.as_slice() else {
        return Err(SqlError::InvalidSql(
            "typed routine requires a function definition".into(),
        ));
    };
    Ok(matches!(
        &function.return_type,
        Some(
            FunctionReturnType::DataType(DataType::Date)
                | FunctionReturnType::SetOf(DataType::Date)
        )
    ))
}

pub(crate) fn prepare_physical_metadata(routine: &mut RoutineSchema) -> Result<()> {
    let params = parse_routine_params(&routine.args)?;
    if params.len() != routine.arg_types.len() {
        return Err(SqlError::InvalidSql(
            "routine argument metadata does not match its declaration".into(),
        ));
    }
    for (param, physical) in params.iter().zip(&mut routine.arg_types) {
        if param.declared_date {
            *physical = raw_routine_type_schema("timestamp(0)")?;
        }
    }
    if returns_date(routine)? {
        let physical = raw_routine_type_schema("timestamp(0)")?;
        routine.return_type = physical.pg_type.clone();
        routine.return_type_modifier = physical.type_modifier.clone();
        routine.return_type_declaration = Some(physical.formatted());
    }
    Ok(())
}

pub(crate) fn prepare_declared_types(
    routine: &RoutineSchema,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
) -> Result<bool> {
    for param in params {
        if param.declared_date {
            param.oracle_date = true;
            param.type_schema = Some(raw_routine_type_schema("timestamp")?);
        }
    }
    for declaration in declarations {
        if let RoutineDecl::Variable {
            declared_date,
            oracle_date,
            pg_type,
            type_modifier,
            ..
        } = declaration
        {
            if *declared_date {
                *oracle_date = true;
                *pg_type = Some("timestamp".into());
                *type_modifier = None;
            } else if pg_type.is_none() {
                return Err(SqlError::Unsupported(
                    "Oracle routine declaration type is not supported".into(),
                ));
            }
        }
    }
    returns_date(routine)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Date,
    Timestamp,
    TemporalUnknown,
    Number,
    Text,
    Interval,
    Null,
    Other,
    Unknown,
}

fn pg_kind(kind: &str) -> Kind {
    match kind {
        "date" => Kind::Date,
        "timestamp" => Kind::TemporalUnknown,
        "numeric" | "int2" | "int4" | "int8" => Kind::Number,
        "text" | "varchar" | "bpchar" | "char" | "name" => Kind::Text,
        "interval" => Kind::Interval,
        _ => Kind::Other,
    }
}

impl Kind {
    fn temporal(self) -> bool {
        matches!(self, Self::Date | Self::Timestamp | Self::TemporalUnknown)
    }
    fn day_operand(self) -> bool {
        matches!(self, Self::Number | Self::Text | Self::Null)
    }
}

#[derive(Clone, Default)]
struct Environment {
    v4_semantics: bool,
    variables: BTreeMap<String, Kind>,
    records: BTreeMap<String, BTreeMap<String, Kind>>,
}

struct Resolve<'a> {
    db: &'a BicDb,
    env: &'a Environment,
    queries: Vec<Query>,
    sql_depth: usize,
}

impl Resolve<'_> {
    fn query_kind(&self, expr: &Expr) -> Option<Kind> {
        let mut probe = self.queries.last()?.clone();
        let SetExpr::Select(select) = probe.body.as_mut() else {
            return None;
        };
        select.projection = vec![SelectItem::UnnamedExpr(expr.clone())];
        probe.locks.clear();
        infer_query_result_types(self.db, &probe.to_string())
            .ok()
            .flatten()?
            .first()?
            .as_deref()
            .map(pg_kind)
    }

    fn kind(&self, expr: &Expr) -> Kind {
        match expr {
            Expr::Nested(expr) => self.kind(expr),
            Expr::Identifier(ident) => self
                .query_kind(expr)
                .or_else(|| {
                    self.env
                        .variables
                        .get(&normalize_object_name(&ident.value))
                        .copied()
                })
                .unwrap_or(Kind::Unknown),
            Expr::CompoundIdentifier(parts) => self
                .query_kind(expr)
                .or_else(|| {
                    let [record, field] = parts.as_slice() else {
                        return None;
                    };
                    self.env
                        .records
                        .get(&normalize_object_name(&record.value))?
                        .get(&field.value.to_ascii_lowercase())
                        .copied()
                })
                .unwrap_or(Kind::Unknown),
            Expr::Value(value) => match &value.value {
                Value::Null => Kind::Null,
                Value::Number(..) => Kind::Number,
                Value::SingleQuotedString(_) => Kind::Text,
                Value::Placeholder(name) => self
                    .env
                    .variables
                    .get(&normalize_object_name(name))
                    .copied()
                    .unwrap_or(Kind::Unknown),
                _ => Kind::Other,
            },
            Expr::Cast {
                data_type: DataType::Date,
                ..
            } => Kind::Date,
            Expr::Cast {
                data_type:
                    DataType::Timestamp(
                        _,
                        sqlparser::ast::TimezoneInfo::None
                        | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                    ),
                ..
            } => Kind::Timestamp,
            Expr::Cast { data_type, .. } => routine_type_schema(data_type)
                .ok()
                .map_or(Kind::Unknown, |kind| pg_kind(&kind.pg_type)),
            Expr::TypedString(value) => match &value.data_type {
                DataType::Date => Kind::Date,
                DataType::Timestamp(
                    _,
                    sqlparser::ast::TimezoneInfo::None
                    | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                ) => Kind::Timestamp,
                _ => Kind::Other,
            },
            Expr::UnaryOp { expr, .. } => self.kind(expr),
            Expr::Function(function) => {
                let name = object_name_lowercase(&function.name).ok();
                match name.as_deref() {
                    Some(
                        "bicdb_oracle_date_coerce"
                        | "bicdb_oracle_date_add_days"
                        | "bicdb_oracle_to_date"
                        | "bicdb_oracle_trunc_date"
                        | "bicdb_oracle_plsql_trunc_date"
                        | "bicdb_oracle_add_months",
                    ) => return Kind::Date,
                    Some(
                        "bicdb_oracle_date_subtract"
                        | "bicdb_oracle_number_divide"
                        | "bicdb_oracle_number_trunc"
                        | "bicdb_oracle_plsql_number_trunc",
                    ) => return Kind::Number,
                    Some("trunc") => {
                        return function_args(function)
                            .first()
                            .map_or(Kind::Unknown, |arg| {
                                let kind = self.kind(arg);
                                if kind.temporal() {
                                    Kind::Date
                                } else {
                                    kind
                                }
                            })
                    }
                    Some("coalesce" | "nullif") => {
                        let kinds = function_args(function)
                            .iter()
                            .map(|arg| self.kind(arg))
                            .filter(|kind| *kind != Kind::Null)
                            .collect::<Vec<_>>();
                        if let Some(kind) = kinds
                            .first()
                            .filter(|kind| kinds.iter().all(|candidate| candidate == *kind))
                        {
                            return *kind;
                        }
                    }
                    _ => {}
                }
                if let Ok(name) = object_name(&function.name) {
                    if let Ok(Some(routine)) = load_routine(self.db, RoutineKind::Function, &name) {
                        if typed_preview_routine_language(&routine.language) {
                            if returns_date(&routine).unwrap_or(false) {
                                return Kind::Date;
                            }
                            if routine.return_type == "timestamp" {
                                return Kind::Timestamp;
                            }
                        }
                        return pg_kind(&routine.return_type);
                    }
                }
                projected_expr_pg_type(expr, None)
                    .as_deref()
                    .map_or(Kind::Unknown, pg_kind)
            }
            Expr::BinaryOp { left, op, right } => {
                let left = self.kind(left);
                let right = self.kind(right);
                if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) {
                    if left.temporal() && right.day_operand()
                        || *op == BinaryOperator::Plus && right.temporal() && left.day_operand()
                    {
                        return Kind::Date;
                    }
                    if *op == BinaryOperator::Minus && left == Kind::Date && right == Kind::Date {
                        return Kind::Number;
                    }
                    if *op == BinaryOperator::Minus
                        && matches!(left, Kind::Date | Kind::Timestamp)
                        && matches!(right, Kind::Date | Kind::Timestamp)
                    {
                        return Kind::Interval;
                    }
                }
                if left == Kind::Number && right.day_operand() {
                    Kind::Number
                } else {
                    projected_expr_pg_type(expr, None)
                        .as_deref()
                        .map_or(Kind::Unknown, pg_kind)
                }
            }
            _ => projected_expr_pg_type(expr, None)
                .as_deref()
                .map_or(Kind::Unknown, pg_kind),
        }
    }

    fn rewrite(&self, expr: &mut Expr) -> Result<()> {
        date_trunc::lower_calendar_call(expr)?;
        if self.env.v4_semantics && self.sql_depth == 0 && self.queries.is_empty() {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name)
                    .is_ok_and(|name| name == "bicdb_oracle_trunc_date")
                {
                    function.name =
                        ObjectName::from(vec![Ident::new("bicdb_oracle_plsql_trunc_date")]);
                }
            }
        }
        if let Expr::Cast {
            expr: input,
            data_type: DataType::Date,
            ..
        } = expr
        {
            *expr = expression(&format!("bicdb_oracle_date_coerce({input})"))?;
            return Ok(());
        }
        if let Expr::TypedString(value) = expr {
            if matches!(value.data_type, DataType::Timestamp(..)) {
                if let Value::SingleQuotedString(text) = &value.value.value {
                    if text.split_once('.').is_some_and(|(_, fraction)| {
                        fraction
                            .chars()
                            .take_while(|character| character.is_ascii_digit())
                            .count()
                            > 6
                    }) {
                        return Err(SqlError::Unsupported("Oracle timestamps with more than six fractional digits are not supported".into()));
                    }
                }
            }
            if matches!(value.data_type, DataType::Date) {
                *expr = expression(&format!("bicdb_oracle_date_coerce({})", value.value))?;
                return Ok(());
            }
        }
        if let Expr::Function(function) = expr {
            if object_name_lowercase(&function.name).is_ok_and(|name| name == "trunc")
                && function_args(function)
                    .first()
                    .is_some_and(|arg| self.kind(arg).temporal())
            {
                function.name = ObjectName::from(vec![Ident::new(
                    if self.env.v4_semantics && self.sql_depth == 0 && self.queries.is_empty() {
                        "bicdb_oracle_plsql_trunc_date"
                    } else {
                        "bicdb_oracle_trunc_date"
                    },
                )]);
            }
        }
        if self.env.v4_semantics {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|name| name == "trunc") {
                    let args = function_args(function);
                    if matches!(
                        args.first().map(|arg| self.kind(arg)),
                        Some(Kind::Number | Kind::Text)
                    ) {
                        *expr = date_trunc::numeric_trunc_expr(&args)?;
                        if self.sql_depth == 0 && self.queries.is_empty() {
                            if let Expr::Function(function) = expr {
                                function.name = ObjectName::from(vec![Ident::new(
                                    "bicdb_oracle_plsql_number_trunc",
                                )]);
                            }
                        }
                    }
                }
            }
        }
        let Expr::BinaryOp { left, op, right } = expr else {
            return Ok(());
        };
        let left_kind = self.kind(left);
        let right_kind = self.kind(right);
        let numeric = |expr: &Expr| {
            format!(
                "CAST({} AS NUMERIC)",
                date_trunc::exact_numeric_operand_sql(expr)
            )
        };
        if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) {
            let direction = if *op == BinaryOperator::Minus { -1 } else { 1 };
            if left_kind.temporal() && right_kind.day_operand() {
                *expr = expression(&format!(
                    "bicdb_oracle_date_add_days({left},{},{direction})",
                    numeric(right)
                ))?;
                return Ok(());
            }
            if *op == BinaryOperator::Plus && right_kind.temporal() && left_kind.day_operand() {
                *expr = expression(&format!(
                    "bicdb_oracle_date_add_days({right},{},1)",
                    numeric(left)
                ))?;
                return Ok(());
            }
            if *op == BinaryOperator::Minus && left_kind == Kind::Date && right_kind == Kind::Date {
                *expr = expression(&format!("bicdb_oracle_date_subtract({left},{right})"))?;
                return Ok(());
            }
            if *op == BinaryOperator::Minus
                && matches!(left_kind, Kind::Date | Kind::Timestamp)
                && matches!(right_kind, Kind::Date | Kind::Timestamp)
            {
                *expr = expression(&format!(
                    "CAST({left} AS TIMESTAMP)-CAST({right} AS TIMESTAMP)"
                ))?;
                return Ok(());
            }
            if left_kind.temporal() || right_kind.temporal() {
                return Err(SqlError::Unsupported(
                    "Oracle routine datetime arithmetic requires resolved logical operand types"
                        .into(),
                ));
            }
        }
        if left_kind == Kind::Number && right_kind.day_operand()
            || right_kind == Kind::Number && left_kind.day_operand()
        {
            if *op == BinaryOperator::Divide {
                *expr = expression(&format!(
                    "bicdb_oracle_number_divide({},{})",
                    numeric(left),
                    numeric(right)
                ))?;
            } else if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                *expr = expression(&format!("{} {op} {}", numeric(left), numeric(right)))?;
            }
        }
        Ok(())
    }
}

impl sqlparser::ast::VisitorMut for Resolve<'_> {
    type Break = SqlError;
    fn pre_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<SqlError> {
        self.sql_depth += 1;
        ControlFlow::Continue(())
    }
    fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<SqlError> {
        self.sql_depth -= 1;
        ControlFlow::Continue(())
    }
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        self.queries.push(query.clone());
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
        let previous = self.queries.pop().expect("query scope");
        if let Some(parent) = self.queries.last_mut() {
            struct Refresh<'a> {
                previous: &'a Query,
                resolved: &'a Query,
            }
            impl sqlparser::ast::VisitorMut for Refresh<'_> {
                type Break = ();
                fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
                    if query == self.previous {
                        *query = self.resolved.clone();
                    }
                    ControlFlow::Continue(())
                }
            }
            let _ = sqlparser::ast::VisitMut::visit(
                parent,
                &mut Refresh {
                    previous: &previous,
                    resolved: query,
                },
            );
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<SqlError> {
        match self.rewrite(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

fn visit<T: sqlparser::ast::VisitMut>(db: &BicDb, env: &Environment, node: &mut T) -> Result<()> {
    match node.visit(&mut Resolve {
        db,
        env,
        queries: Vec::new(),
        sql_depth: 0,
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

fn record_types(db: &BicDb, query: &Query) -> BTreeMap<String, Kind> {
    infer_query_result_columns(db, &query.to_string())
        .ok()
        .flatten()
        .unwrap_or_default()
        .into_iter()
        .map(|(name, kind)| {
            (
                name.to_ascii_lowercase(),
                kind.as_deref().map_or(Kind::Unknown, pg_kind),
            )
        })
        .collect()
}

fn statements(
    db: &BicDb,
    env: &Environment,
    cursors: &BTreeMap<String, Query>,
    body: &mut [RoutineStmt],
) -> Result<()> {
    for statement in body {
        match statement {
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    visit(db, env, &mut index.expr)?;
                }
                visit(db, env, &mut expr.expr)?;
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                visit(db, env, &mut condition.expr)?;
                statements(db, env, cursors, then_body)?;
                statements(db, env, cursors, else_body)?;
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                visit(db, env, &mut expression.expr)?;
                for arm in arms {
                    statements(db, env, cursors, arm)?;
                }
                if let Some(body) = else_body {
                    statements(db, env, cursors, body)?;
                }
            }
            RoutineStmt::ForLoop {
                iterator,
                lower,
                upper,
                body,
            } => {
                visit(db, env, &mut lower.expr)?;
                visit(db, env, &mut upper.expr)?;
                let mut scope = env.clone();
                scope
                    .variables
                    .insert(normalize_object_name(iterator), Kind::Number);
                statements(db, &scope, cursors, body)?;
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                visit(db, env, &mut array.expr)?;
                statements(db, env, cursors, body)?;
            }
            RoutineStmt::WhileLoop { condition, body } => {
                visit(db, env, &mut condition.expr)?;
                statements(db, env, cursors, body)?;
            }
            RoutineStmt::QueryForLoop {
                target,
                query,
                body,
            } => {
                visit(db, env, query)?;
                let mut scope = env.clone();
                scope
                    .records
                    .insert(normalize_object_name(target), record_types(db, query));
                statements(db, &scope, cursors, body)?;
            }
            RoutineStmt::CursorForLoop {
                target,
                cursor,
                body,
            } => {
                let mut scope = env.clone();
                if let Some(query) = cursors.get(cursor) {
                    scope
                        .records
                        .insert(normalize_object_name(target), record_types(db, query));
                }
                statements(db, &scope, cursors, body)?;
            }
            RoutineStmt::Loop { body } => statements(db, env, cursors, body)?,
            RoutineStmt::SelectInto { query, .. }
            | RoutineStmt::Perform { query }
            | RoutineStmt::ReturnQuery(query) => visit(db, env, query)?,
            RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                visit(db, env, statement)?
            }
            RoutineStmt::Return(Some(expr)) | RoutineStmt::DynamicExecute(expr) => {
                visit(db, env, &mut expr.expr)?
            }
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    visit(db, env, &mut arg.expr)?;
                }
                if let Some(detail) = detail {
                    visit(db, env, &mut detail.expr)?;
                }
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::ExitLoop
            | RoutineStmt::Null
            | RoutineStmt::OpenCursor { .. }
            | RoutineStmt::FetchCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::Return(None) => {}
        }
    }
    Ok(())
}

pub(crate) fn lower(
    db: &BicDb,
    v4_semantics: bool,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    body: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    let mut env = Environment {
        v4_semantics,
        ..Environment::default()
    };
    for param in params.iter() {
        let kind = if param.oracle_date {
            Kind::Date
        } else {
            param.type_schema.as_ref().map_or(Kind::Unknown, |schema| {
                if schema.pg_type == "timestamp" {
                    Kind::Timestamp
                } else {
                    pg_kind(&schema.pg_type)
                }
            })
        };
        env.variables.insert(format!("${}", param.index + 1), kind);
        if let Some(name) = &param.name {
            env.variables.insert(normalize_object_name(name), kind);
        }
    }
    for declaration in declarations.iter() {
        match declaration {
            RoutineDecl::Variable {
                name,
                pg_type,
                oracle_date,
                ..
            } => {
                let kind = if *oracle_date {
                    Kind::Date
                } else if pg_type.as_deref() == Some("timestamp") {
                    Kind::Timestamp
                } else {
                    pg_type.as_deref().map_or(Kind::Unknown, pg_kind)
                };
                env.variables.insert(normalize_object_name(name), kind);
            }
            RoutineDecl::Alias { name, position } => {
                if let Some(kind) = env.variables.get(&format!("${position}")).copied() {
                    env.variables.insert(normalize_object_name(name), kind);
                }
            }
            _ => {}
        }
    }
    for param in params {
        if let Some(expr) = &mut param.default_expr {
            visit(db, &env, expr)?;
        }
    }
    let mut cursors = BTreeMap::new();
    for declaration in declarations {
        match declaration {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => visit(db, &env, &mut expr.expr)?,
            RoutineDecl::Cursor { name, query } => {
                visit(db, &env, query)?;
                cursors.insert(normalize_object_name(name), query.clone());
            }
            _ => {}
        }
    }
    statements(db, &env, &cursors, body)?;
    for handler in handlers {
        statements(db, &env, &cursors, &mut handler.statements)?;
    }
    Ok(())
}
