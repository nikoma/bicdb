//! Logical DATE provenance and typed operations for the version-3 compiler.
use super::*;

pub(super) fn returns_date(routine: &RoutineSchema) -> Result<bool> {
    if routine.kind == RoutineKind::Procedure {
        return Ok(false);
    }
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
    let _lobs = oracle::lobs::Scope::new(
        oracle::preview_routine_language_version(&routine.language).is_some_and(|v| v >= 29),
    );
    let _national = national::Scope::new(
        preview_routine_language_version(&routine.language).is_some_and(|v| v >= 17),
    );
    let params = parse_routine_params(&routine.args)?;
    if params.len() != routine.arg_types.len() {
        return Err(SqlError::InvalidSql(
            "routine argument metadata does not match its declaration".into(),
        ));
    }
    for (param, physical) in params.iter().zip(&mut routine.arg_types) {
        if param.declared_date {
            *physical = raw_routine_type_schema("timestamp(0)")?;
        } else if param.type_schema.as_ref().is_some_and(|schema| {
            national::is_modifier(schema.type_modifier.as_ref())
                || lobs::is_modifier(schema.type_modifier.as_ref())
        }) {
            *physical = RoutineTypeSchema {
                pg_type: if param
                    .type_schema
                    .as_ref()
                    .is_some_and(|schema| lobs::is_modifier(schema.type_modifier.as_ref()))
                {
                    "bytea"
                } else {
                    "varchar"
                }
                .into(),
                type_modifier: None,
            };
        }
    }
    if lobs::returns_blob(routine)? {
        routine.return_type = "bytea".into();
        routine.return_type_modifier = None;
        routine.return_type_declaration = Some("bytea".into());
    }
    if national::returns_national(routine)? {
        routine.return_type = "varchar".into();
        routine.return_type_modifier = None;
        routine.return_type_declaration = Some("character varying".into());
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
    prepare_local_declarations(declarations)?;
    returns_date(routine)
}

pub(crate) fn prepare_local_declarations(declarations: &mut [RoutineDecl]) -> Result<()> {
    for declaration in declarations {
        if let RoutineDecl::Cursor { params, .. } = declaration {
            crate::routines::parameterized_cursors::prepare_parameters(params)?;
        }
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
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Date,
    Timestamp(Option<u8>),
    TemporalUnknown,
    Number,
    Text,
    Raw,
    Blob,
    NationalText,
    FixedNationalText,
    Interval,
    Null,
    Other,
    Unknown,
}

fn temporal_precision(modifier: Option<&PgTypeModifier>) -> Option<u8> {
    Some(match modifier {
        Some(PgTypeModifier::Temporal { precision })
        | Some(PgTypeModifier::OracleTemporalV1 { precision, .. }) => *precision,
        _ => 6,
    })
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

fn schema_kind(schema: &RoutineTypeSchema) -> Kind {
    if lobs::is_modifier(schema.type_modifier.as_ref()) {
        Kind::Blob
    } else if let Some(PgTypeModifier::OracleNationalV1 { fixed, .. }) = &schema.type_modifier {
        if *fixed {
            Kind::FixedNationalText
        } else {
            Kind::NationalText
        }
    } else {
        pg_kind(&schema.pg_type)
    }
}

impl Kind {
    fn is_national(self) -> bool {
        matches!(self, Self::NationalText | Self::FixedNationalText)
    }
    fn common(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, kind) | (kind, Self::Null) => kind,
            (Self::Timestamp(a), Self::Timestamp(b)) => {
                Self::Timestamp(a.zip(b).map(|(a, b)| a.max(b)))
            }
            (Self::Text, Self::NationalText | Self::FixedNationalText)
            | (Self::NationalText | Self::FixedNationalText, Self::Text)
            | (Self::NationalText, Self::FixedNationalText)
            | (Self::FixedNationalText, Self::NationalText) => Self::NationalText,
            (a, b) if a == b => a,
            _ => Self::Unknown,
        }
    }
    fn temporal(self) -> bool {
        matches!(
            self,
            Self::Date | Self::Timestamp(_) | Self::TemporalUnknown
        )
    }
    fn day_operand(self) -> bool {
        matches!(
            self,
            Self::Number | Self::Text | Self::NationalText | Self::FixedNationalText | Self::Null
        )
    }
}

#[derive(Clone, Default)]
struct Environment {
    blob_return: bool,
    v4_semantics: bool,
    concatenation: bool,
    number_arithmetic: bool,
    decode: bool,
    raw_conversion: bool,
    character_conversion: bool,
    nvl: bool,
    nvl2: bool,
    national_literals: bool,
    sql_builtins_only: bool,
    defer_bind_types: bool,
    text_patterns: bool,
    statement_provenance: bool,
    number_formats: bool,
    collection_namespace: Option<String>,
    collections: BTreeMap<String, super::collections::CollectionBinding>,
    variables: BTreeMap<String, Kind>,
    records: BTreeMap<String, BTreeMap<String, Kind>>,
}

struct Resolve<'a> {
    db: &'a BicDb,
    env: &'a Environment,
    queries: Vec<Query>,
    selects: Vec<(usize, sqlparser::ast::Select)>,
    sql_depth: usize,
    statement_from: Vec<Option<String>>,
}

impl Resolve<'_> {
    fn variable_kind(&self, kind: Kind) -> Kind {
        if matches!(kind, Kind::Timestamp(_)) && (self.sql_depth > 0 || !self.queries.is_empty()) {
            Kind::Timestamp(Some(9))
        } else {
            kind
        }
    }
    fn query_kind(&self, expr: &Expr) -> Option<Kind> {
        // A set operation's SELECT operands have independent FROM scopes.
        // Keep the enclosing WITH clause while probing the operand being visited.
        let context = self.queries.last().map(|query| {
            if !matches!(query.body.as_ref(), SetExpr::Select(_)) {
                if let Some((depth, select)) = self.selects.last() {
                    if *depth == self.queries.len() {
                        let mut operand = query.clone();
                        operand.body = Box::new(SetExpr::Select(Box::new(select.clone())));
                        operand.order_by = None;
                        operand.limit_clause = None;
                        operand.fetch = None;
                        operand.locks.clear();
                        return std::borrow::Cow::Owned(operand);
                    }
                }
            }
            std::borrow::Cow::Borrowed(query)
        });
        if let Some(source) = context
            .as_deref()
            .and_then(|query| temporal_columns::logical_in_query(self.db, query, expr))
        {
            return Some(match source.source {
                temporal_columns::Source::Date => Kind::Date,
                temporal_columns::Source::Timestamp => Kind::Timestamp(source.precision),
                temporal_columns::Source::TimestampWithTimeZone => Kind::Other,
            });
        }
        let sql = if let Some(query) = context {
            let mut probe = query.into_owned();
            let SetExpr::Select(select) = probe.body.as_mut() else {
                return None;
            };
            select.projection = vec![SelectItem::UnnamedExpr(expr.clone())];
            probe.locks.clear();
            probe.to_string()
        } else if let Some(Some(from)) = self.statement_from.last() {
            format!("SELECT {expr} FROM {from}")
        } else {
            return None;
        };
        if self.env.statement_provenance && self.queries.is_empty() {
            if let Ok(statements) = parse_statements(&sql) {
                if let Some(Statement::Query(query)) = statements.first() {
                    if let Some(logical) = temporal_columns::logical_in_query(self.db, query, expr)
                    {
                        return Some(match logical.source {
                            temporal_columns::Source::Date => Kind::Date,
                            temporal_columns::Source::Timestamp => {
                                Kind::Timestamp(logical.precision)
                            }
                            temporal_columns::Source::TimestampWithTimeZone => Kind::Other,
                        });
                    }
                }
            }
        }
        infer_query_result_types(self.db, &sql)
            .ok()
            .flatten()?
            .first()?
            .as_deref()
            .map(pg_kind)
    }

    fn kind(&self, expr: &Expr) -> Kind {
        match expr {
            Expr::Nested(expr) => self.kind(expr),
            Expr::Subquery(_) => self.query_kind(expr).unwrap_or(Kind::Unknown),
            Expr::Substring { expr, .. } | Expr::Overlay { expr, .. } | Expr::Trim { expr, .. } => {
                // These are dedicated AST nodes, not Function calls. Their
                // character result type must retain procedural input types
                // when SQL metadata cannot resolve a host variable.
                match self.kind(expr) {
                    kind @ (Kind::Text | Kind::NationalText | Kind::FixedNationalText) => kind,
                    Kind::Null => Kind::Text,
                    _ => Kind::Unknown,
                }
            }
            Expr::Identifier(ident) => self
                .query_kind(expr)
                .or_else(|| {
                    self.env
                        .variables
                        .get(&normalize_object_name(&ident.value))
                        .copied()
                        .map(|kind| self.variable_kind(kind))
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
                        .map(|kind| self.variable_kind(kind))
                })
                .unwrap_or(Kind::Unknown),
            Expr::Value(value) => match &value.value {
                Value::Null => Kind::Null,
                Value::Number(..) => Kind::Number,
                Value::SingleQuotedString(_) => Kind::Text,
                Value::NationalStringLiteral(_) if self.env.national_literals => {
                    Kind::FixedNationalText
                }
                Value::Placeholder(name) => self
                    .env
                    .variables
                    .get(&normalize_object_name(name))
                    .copied()
                    .map(|kind| self.variable_kind(kind))
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
                        precision,
                        sqlparser::ast::TimezoneInfo::None
                        | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                    ),
                ..
            } => Kind::Timestamp(Some(precision.unwrap_or(6) as u8)),
            Expr::Cast { data_type, .. } => routine_type_schema(data_type)
                .ok()
                .map_or(Kind::Unknown, |kind| schema_kind(&kind)),
            Expr::TypedString(value) => match &value.data_type {
                DataType::Date => Kind::Date,
                DataType::Timestamp(
                    precision,
                    sqlparser::ast::TimezoneInfo::None
                    | sqlparser::ast::TimezoneInfo::WithoutTimeZone,
                ) => Kind::Timestamp(Some(precision.unwrap_or(9) as u8)),
                _ => Kind::Other,
            },
            Expr::UnaryOp { expr, .. } => self.kind(expr),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                let mut kinds = conditions
                    .iter()
                    .map(|arm| self.kind(&arm.result))
                    .chain(else_result.iter().map(|value| self.kind(value)))
                    .filter(|kind| *kind != Kind::Null);
                let first = kinds.next().unwrap_or(Kind::Null);
                kinds.fold(first, Kind::common)
            }
            Expr::Function(function) => {
                let name = object_name_lowercase(&function.name).ok();
                match name.as_deref() {
                    Some("bicdb_oracle_empty_blob" | "bicdb_oracle_blob_from_raw")
                        if lobs::enabled() =>
                    {
                        return Kind::Blob
                    }
                    // SYS_GUID lowers to the existing random-byte intrinsic.
                    // Do not infer RAW from a text value's bytea-looking prefix.
                    Some(
                        "gen_random_bytes" | "public.gen_random_bytes" | "bicdb_oracle_hextoraw",
                    ) => return Kind::Raw,
                    Some(
                        "bicdb_oracle_current_sqlcode"
                        | "bicdb_oracle_sql_blob_getlength"
                        | "bicdb_oracle_sql_blob_istemporary"
                        | "pg_catalog.bicdb_oracle_sql_blob_getlength"
                        | "pg_catalog.bicdb_oracle_sql_blob_istemporary"
                        | "bicdb_oracle_blob_getlength"
                        | "bicdb_oracle_blob_istemporary"
                        | "bicdb_oracle_national_length"
                        | "length"
                        | "lengthb"
                        | "lengthc"
                        | "length2"
                        | "length4",
                    ) => return Kind::Number,
                    Some("bicdb_oracle_concat") => {
                        return if function_args(function)
                            .iter()
                            .take(2)
                            .any(|arg| self.kind(arg).is_national())
                        {
                            Kind::NationalText
                        } else {
                            Kind::Text
                        }
                    }
                    Some("bicdb_oracle_national_value") => return Kind::NationalText,
                    Some("bicdb_oracle_national_literal") => return Kind::FixedNationalText,
                    Some("concat" | "pg_catalog.concat" | "concat_ws" | "pg_catalog.concat_ws") => {
                        return Kind::Text
                    }
                    Some("bicdb_oracle_current_sqlerrm") => return Kind::Text,
                    Some(
                        "chr"
                        | "pg_catalog.chr"
                        | "bicdb_oracle_chr"
                        | "bicdb_oracle_default_to_char",
                    ) => return Kind::Text,
                    Some(
                        "substr" | "substring" | "trim" | "btrim" | "ltrim" | "rtrim" | "reverse"
                        | "lower" | "upper",
                    ) if self.env.concatenation => {
                        if function_args(function).first().is_some_and(|arg| {
                            matches!(
                                self.kind(arg),
                                Kind::Text | Kind::NationalText | Kind::FixedNationalText
                            )
                        }) {
                            return self.kind(&function_args(function)[0]);
                        }
                    }
                    Some(
                        "bicdb_oracle_date_coerce"
                        | "bicdb_oracle_decode_date"
                        | "bicdb_oracle_date_add_days"
                        | "bicdb_oracle_to_date"
                        | "bicdb_oracle_trunc_date"
                        | "bicdb_oracle_plsql_trunc_date"
                        | "bicdb_oracle_add_months",
                    ) => return Kind::Date,
                    Some("bicdb_oracle_decode_timestamp") => return Kind::Timestamp(None),
                    Some(name @ ("bicdb_oracle_nvl_timestamp" | "bicdb_oracle_nvl2_timestamp")) => {
                        return Kind::Timestamp(
                            function_args(function)
                                .get(if name == "bicdb_oracle_nvl2_timestamp" {
                                    3
                                } else {
                                    2
                                })
                                .and_then(|value| match value {
                                    Expr::Value(value) => match &value.value {
                                        Value::Number(n, _) => n.parse().ok(),
                                        _ => None,
                                    },
                                    _ => None,
                                }),
                        )
                    }
                    Some("bicdb_oracle_nvl_date" | "bicdb_oracle_nvl2_date") => return Kind::Date,
                    Some("bicdb_oracle_nvl_number" | "bicdb_oracle_nvl2_number") => {
                        return Kind::Number
                    }
                    Some(
                        "bicdb_oracle_nvl_text"
                        | "bicdb_oracle_nvl2_text"
                        | "bicdb_oracle_nvl_convert_text",
                    ) => return Kind::Text,
                    Some(
                        "bicdb_oracle_decode_number"
                        | "bicdb_oracle_date_subtract"
                        | "bicdb_oracle_number_divide"
                        | "bicdb_oracle_number_arithmetic"
                        | "bicdb_oracle_number_trunc"
                        | "bicdb_oracle_number_round"
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
                        if let Some(kind) = kinds.into_iter().reduce(Kind::common) {
                            return kind;
                        }
                    }
                    Some("min" | "max") => {
                        // These aggregates preserve the operand's logical type,
                        // including Oracle DATE provenance which native output
                        // metadata represents with a timestamp transport type.
                        return function_args(function)
                            .first()
                            .map_or(Kind::Unknown, |arg| self.kind(arg));
                    }
                    _ => {}
                }
                if let Ok(name) = object_name(&function.name) {
                    if let Ok(Some(routine)) = load_routine(self.db, RoutineKind::Function, &name) {
                        if lobs::enabled() && lobs::returns_blob(&routine).unwrap_or(false) {
                            return Kind::Blob;
                        }
                        if let Some(schema) = national::return_type(&routine).ok().flatten() {
                            return schema_kind(&schema);
                        }
                        if typed_preview_routine_language(&routine.language) {
                            if returns_date(&routine).unwrap_or(false) {
                                return Kind::Date;
                            }
                            if routine.return_type == "timestamp" {
                                return Kind::Timestamp(
                                    if self.sql_depth > 0 || !self.queries.is_empty() {
                                        Some(9)
                                    } else {
                                        temporal_precision(routine.return_type_modifier.as_ref())
                                    },
                                );
                            }
                        }
                        return pg_kind(&routine.return_type);
                    }
                }
                // Aggregate and scalar function arguments can reference input
                // columns. Projection-only inference has no FROM scope; use
                // the query's metadata without executing predicates or rows.
                projected_expr_pg_type(expr, None)
                    .as_deref()
                    .map(pg_kind)
                    .or_else(|| self.query_kind(expr))
                    .unwrap_or(Kind::Unknown)
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
                        && matches!(left, Kind::Date | Kind::Timestamp(_))
                        && matches!(right, Kind::Date | Kind::Timestamp(_))
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
        if self.env.national_literals {
            if let Expr::Value(value) = expr {
                if let Value::NationalStringLiteral(text) = &value.value {
                    // Runtime payloads use the ordinary text codec. The
                    // wrapper retains fixed national identity and normalizes
                    // the empty literal to NULL without changing native SQL.
                    *expr = expression(&format!(
                        "bicdb_oracle_national_literal('{}')",
                        text.replace('\'', "''")
                    ))?;
                    return Ok(());
                }
            }
        }

        if self.env.character_conversion {
            let convert = |value: &mut Expr| -> Result<()> {
                if self.kind(value) == Kind::Raw {
                    *value = expression(&format!(
                        "bicdb_oracle_default_to_char({},'raw','sql')",
                        render_sql(value)
                    ))?;
                }
                Ok(())
            };
            match expr {
                Expr::Cast {
                    expr: value,
                    data_type,
                    ..
                } if routine_type_schema(data_type).ok().is_some_and(|s| {
                    matches!(
                        schema_kind(&s),
                        Kind::Text | Kind::NationalText | Kind::FixedNationalText
                    )
                }) =>
                {
                    convert(value)?
                }
                Expr::Function(function)
                    if object_name_lowercase(&function.name).is_ok_and(|name| {
                        matches!(
                            &*name,
                            "lower"
                                | "upper"
                                | "length"
                                | "lengthb"
                                | "lengthc"
                                | "length2"
                                | "length4"
                                | "substr"
                                | "substring"
                                | "instr"
                                | "ascii"
                                | "lpad"
                                | "rpad"
                                | "concat"
                                | "pg_catalog.concat"
                                | "concat_ws"
                                | "pg_catalog.concat_ws"
                        )
                    }) =>
                {
                    if let FunctionArguments::List(args) = &mut function.args {
                        let name = object_name_lowercase(&function.name)?;
                        let raw_length = matches!(
                            &*name,
                            "lengthb" | "lengthc" | "length2" | "length4"
                        ) && matches!(args.args.first(), Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(value))) if self.kind(value) == Kind::Raw);
                        let length_name = if name == "lengthb" {
                            "octet_length"
                        } else {
                            "length"
                        };
                        let all = matches!(
                            &*name,
                            "concat" | "pg_catalog.concat" | "concat_ws" | "pg_catalog.concat_ws"
                        );
                        for (index, arg) in args.args.iter_mut().enumerate() {
                            let character = all
                                || index == 0
                                || (index == 1 && name == "instr")
                                || (index == 2 && matches!(&*name, "lpad" | "rpad"));
                            if character {
                                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) = arg {
                                    convert(value)?;
                                }
                            }
                        }
                        if raw_length {
                            function.name = ObjectName::from(vec![Ident::new(length_name)]);
                        }
                    }
                }
                Expr::Substring { expr: value, .. } | Expr::Trim { expr: value, .. } => {
                    convert(value)?
                }
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::StringConcat,
                    right,
                } => {
                    convert(left)?;
                    convert(right)?;
                }
                _ => {}
            }
        }
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
                fn fixed_literal(expr: &Expr) -> bool {
                    match expr {
                        Expr::Nested(expr) => fixed_literal(expr),
                        Expr::Value(v) => matches!(v.value, Value::SingleQuotedString(_)),
                        _ => false,
                    }
                }
                let left_fixed = self.kind(left) == Kind::FixedNationalText;
                let right_fixed = self.kind(right) == Kind::FixedNationalText;
                if (left_fixed && (right_fixed || fixed_literal(right)))
                    || (right_fixed && fixed_literal(left))
                {
                    *expr = expression(&format!(
                        "rtrim({},' ') {op} rtrim({},' ')",
                        super::render_sql(left.as_ref()),
                        super::render_sql(right.as_ref())
                    ))?;
                    return Ok(());
                }
            }
        }
        if let Expr::Substring { expr: operand, .. } | Expr::Overlay { expr: operand, .. } = expr {
            if self.kind(operand).is_national() {
                return Err(unsupported(
                    "national-character substring positions require a UTF16 execution contract",
                ));
            }
        }
        if let Expr::Function(function) = expr {
            if object_name_lowercase(&function.name).is_ok_and(|name| {
                matches!(
                    name.as_ref(),
                    "substr" | "substring" | "reverse" | "lpad" | "rpad"
                )
            }) && function_args(function)
                .iter()
                .any(|arg| self.kind(arg).is_national())
            {
                return Err(unsupported(
                    "national-character positional functions require a UTF16 execution contract",
                ));
            }
        }
        if let Expr::TypedString(value) = expr {
            if self.env.nvl && matches!(value.data_type, DataType::Timestamp(..)) {
                if let Value::SingleQuotedString(text) = &value.value.value {
                    if text.split_once('.').is_some_and(|(_, fraction)| {
                        let digits = fraction
                            .chars()
                            .take_while(|ch| ch.is_ascii_digit())
                            .collect::<String>();
                        digits.len() > 6 && digits[6..].bytes().any(|byte| byte != b'0')
                    }) {
                        return Err(SqlError::Unsupported("Oracle timestamps with more than six fractional digits are not supported".into()));
                    }
                }
            }
        }
        if let Expr::Function(function) = expr {
            let name = object_name_lowercase(&function.name).ok();
            if let Some(name @ ("length" | "lengthb" | "lengthc" | "length2" | "length4")) =
                name.as_deref()
            {
                let args = function_args(function);
                if let [arg] = args.as_slice() {
                    if self.kind(arg).is_national() {
                        let mode = match name {
                            "lengthb" => "bytes",
                            "lengthc" => "complete",
                            "length4" => "characters",
                            _ => "utf16",
                        };
                        *expr = expression(&format!(
                            "bicdb_oracle_national_length({},'{mode}')",
                            super::render_sql(arg)
                        ))?;
                        return Ok(());
                    }
                }
            }
        }
        if self.env.raw_conversion {
            super::raw_values::lower_call(
                expr,
                |value| self.kind(value),
                self.env.defer_bind_types,
                self.sql_depth > 0 || !self.queries.is_empty(),
            )?;
        }
        if self.env.character_conversion {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|name| name == "chr") {
                    function.name = ObjectName::from(vec![Ident::new("bicdb_oracle_chr")]);
                }
            }
        }
        if self.env.nvl2 {
            let national_result = if let Expr::Function(function) = expr {
                if super::nvl2::is_call(function) {
                    let args = function_args(function);
                    args.get(1).is_some_and(|arg| self.kind(arg).is_national())
                        || (args.get(1).is_some_and(|arg| self.kind(arg) == Kind::Null)
                            && args.get(2).is_some_and(|arg| self.kind(arg).is_national()))
                } else {
                    false
                }
            } else {
                false
            };
            super::nvl2::lower_call(
                expr,
                |value| self.kind(value),
                self.sql_depth > 0 || !self.queries.is_empty(),
                self.env.defer_bind_types,
            )?;
            if national_result {
                *expr = expression(&format!(
                    "bicdb_oracle_national_value({})",
                    super::render_sql(expr)
                ))?;
            }
        }
        if self.env.nvl {
            let national_result = if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|name| name == "nvl") {
                    let args = function_args(function);
                    args.first().is_some_and(|arg| self.kind(arg).is_national())
                        || (args.first().is_some_and(|arg| self.kind(arg) == Kind::Null)
                            && args.get(1).is_some_and(|arg| self.kind(arg).is_national()))
                } else {
                    false
                }
            } else {
                false
            };
            super::nvl::lower_call(
                expr,
                |value| self.kind(value),
                self.sql_depth > 0 || !self.queries.is_empty(),
                self.env.defer_bind_types,
            )?;
            if national_result {
                *expr = expression(&format!(
                    "bicdb_oracle_national_value({})",
                    super::render_sql(expr)
                ))?;
            }
        }
        if self.env.decode {
            let national_result = if let Expr::Function(function) = expr {
                object_name_lowercase(&function.name).is_ok_and(|name| name == "decode")
                    && function_args(function)
                        .get(2)
                        .is_some_and(|arg| self.kind(arg).is_national())
            } else {
                false
            };
            super::decode::lower_call(
                expr,
                |arg| self.kind(arg),
                self.sql_depth > 0 || !self.queries.is_empty(),
                self.env.defer_bind_types,
            )?;
            if national_result {
                *expr = expression(&format!(
                    "bicdb_oracle_national_value({})",
                    super::render_sql(expr)
                ))?;
            }
        }
        if self.env.concatenation || self.env.character_conversion {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|name| name == "to_char") {
                    let args = function_args(function);
                    if let [arg] = args.as_slice() {
                        let kind=match self.kind(arg) {
                            Kind::Number=>"number",Kind::Date=>"date",Kind::Text|Kind::NationalText|Kind::FixedNationalText|Kind::Null=>"text",
                            Kind::Unknown if self.env.sql_builtins_only && self.env.defer_bind_types => return Ok(()),
                            _=>return Err(SqlError::Unsupported("default Oracle TO_CHAR requires resolved character, NUMBER or logical DATE input".into())),
                        };
                        let context = if self.sql_depth > 0 || !self.queries.is_empty() {
                            "sql"
                        } else {
                            "plsql"
                        };
                        *expr = expression(&format!(
                            "bicdb_oracle_default_to_char({arg},'{kind}','{context}')"
                        ))?;
                        return Ok(());
                    }
                }
            }
        }
        // Standalone DML has a target row scope but no enclosing Query.
        // Its proven temporal operands need the same day-arithmetic rules as
        // SELECT expressions. Keep ordinary SQL queries on their existing path.
        let dml_scope = self.env.statement_provenance
            && self.queries.is_empty()
            && self.statement_from.last().is_some_and(Option::is_some);
        if self.env.sql_builtins_only && !dml_scope {
            return Ok(());
        }

        if self.env.concatenation {
            if let Expr::BinaryOp {
                left,
                op: BinaryOperator::StringConcat,
                right,
            } = expr
            {
                let kind = |operand: &Expr| -> Result<&str> {
                    match self.kind(operand) {
                        Kind::Text | Kind::NationalText | Kind::FixedNationalText | Kind::Null => {
                            Ok("text")
                        }
                        Kind::Number => Ok("number"),
                        Kind::Date => Ok("date"),
                        resolved => {
                            let label = match operand {
                                Expr::Identifier(name) => {
                                    format!("identifier {}", ident_value(name))
                                }
                                Expr::Function(function) => format!("function {}", function.name),
                                _ => "expression".into(),
                            };
                            Err(SqlError::Unsupported(format!("Oracle concatenation requires resolved character, NUMBER or logical DATE operands; use an explicit conversion for other types ({label}: {resolved:?})")))
                        }
                    }
                };
                let context = if self.sql_depth > 0 || !self.queries.is_empty() {
                    "sql"
                } else {
                    "plsql"
                };
                *expr = expression(&format!(
                    "bicdb_oracle_concat({},{},'{}','{}','{context}')",
                    super::render_sql(left.as_ref()),
                    super::render_sql(right.as_ref()),
                    kind(left)?,
                    kind(right)?
                ))?;
                return Ok(());
            }
        }
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
                        Some(
                            Kind::Number
                                | Kind::Text
                                | Kind::NationalText
                                | Kind::FixedNationalText
                        )
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
                && matches!(left_kind, Kind::Date | Kind::Timestamp(_))
                && matches!(right_kind, Kind::Date | Kind::Timestamp(_))
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
        // The standalone DML pass extends temporal provenance only. Preserve
        // existing numeric predicates and increment shapes for indexed lookup
        // and read-committed write repair.
        if self.env.sql_builtins_only {
            return Ok(());
        }
        if left_kind == Kind::Number && right_kind.day_operand()
            || right_kind == Kind::Number && left_kind.day_operand()
        {
            if self.env.number_arithmetic
                && matches!(
                    op,
                    BinaryOperator::Plus
                        | BinaryOperator::Minus
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                )
            {
                let context = if self.sql_depth == 0 && self.queries.is_empty() {
                    "plsql"
                } else {
                    "sql"
                };
                *expr = expression(&format!(
                    "bicdb_oracle_number_arithmetic({},{},'{op}','{context}')",
                    numeric(left),
                    numeric(right)
                ))?;
            } else if *op == BinaryOperator::Divide {
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

/// Resolve standalone SQL builtins without applying routine arithmetic rules.
pub(super) fn lower_sql_builtins(
    db: &BicDb,
    statements: &mut Vec<Statement>,
    oids: Option<&[i32]>,
) -> Result<()> {
    let mut env = Environment {
        decode: true,
        character_conversion: true,
        raw_conversion: true,
        nvl: true,
        nvl2: true,
        national_literals: true,
        statement_provenance: true,
        number_formats: true,
        sql_builtins_only: true,
        text_patterns: true,
        defer_bind_types: oids.is_none(),
        ..Environment::default()
    };
    if let Some(oids) = oids {
        // Unspecified PostgreSQL bind types default to text. Explicit Parse
        // OIDs take precedence; delaying the first pass avoids freezing a
        // numeric/date bind's DECODE rules before those OIDs are available.
        let _ = visit_expressions_mut(statements, |expr| {
            if let Expr::Value(value) = expr {
                if let Value::Placeholder(name) = &value.value {
                    let kind = name
                        .strip_prefix('$')
                        .and_then(|n| n.parse::<usize>().ok())
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|n| oids.get(n))
                        .and_then(|oid| pg_type_spec_by_oid(*oid))
                        .map_or(Kind::Text, |spec| {
                            if spec.name == "timestamp" {
                                Kind::Timestamp(Some(6))
                            } else if spec.name == "bytea" && env.raw_conversion {
                                // A binary protocol input is RAW, not a persistent
                                // LOB locator. Keep earlier routine contracts intact.
                                Kind::Raw
                            } else {
                                pg_kind(spec.name)
                            }
                        });
                    env.variables.insert(normalize_object_name(name), kind);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    visit(db, &env, statements)
}

impl sqlparser::ast::VisitorMut for Resolve<'_> {
    // AST visitors carry their break value through every recursive frame.
    // Keep the large structured SQL diagnostic out of that hot stack path.
    type Break = Box<SqlError>;
    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Box<SqlError>> {
        self.sql_depth += 1;
        self.statement_from.push(if self.env.decode {
            match statement {
                Statement::Update(update) => Some(update.table.to_string()),
                Statement::Delete(delete) if self.env.statement_provenance => {
                    let from = match &delete.from {
                        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
                    };
                    Some(
                        from.iter()
                            .chain(delete.using.iter().flatten())
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                }
                Statement::CreateIndex(index) => Some(index.table_name.to_string()),
                _ => None,
            }
        } else {
            None
        });
        ControlFlow::Continue(())
    }
    fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<Box<SqlError>> {
        self.sql_depth -= 1;
        self.statement_from.pop();
        ControlFlow::Continue(())
    }
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Box<SqlError>> {
        self.queries.push(query.clone());
        ControlFlow::Continue(())
    }
    fn pre_visit_select(
        &mut self,
        select: &mut sqlparser::ast::Select,
    ) -> ControlFlow<Box<SqlError>> {
        self.selects.push((self.queries.len(), select.clone()));
        ControlFlow::Continue(())
    }
    fn post_visit_select(&mut self, _: &mut sqlparser::ast::Select) -> ControlFlow<Box<SqlError>> {
        let (depth, _) = self.selects.pop().expect("SELECT scope");
        debug_assert_eq!(depth, self.queries.len());
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Box<SqlError>> {
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
            let depth = self.queries.len();
            for (owner, select) in &mut self.selects {
                if *owner == depth {
                    let _ = sqlparser::ast::VisitMut::visit(
                        select,
                        &mut Refresh {
                            previous: &previous,
                            resolved: query,
                        },
                    );
                }
            }
        }
        ControlFlow::Continue(())
    }
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Box<SqlError>> {
        if lobs::enabled() {
            if let Expr::Function(function) = expr {
                if let Some(operation) = lobs::package_operation(function) {
                    if matches!(operation.as_str(), "GETLENGTH" | "ISTEMPORARY") {
                        let lower = (|| -> Result<Expr> {
                            let params = parse_routine_params(&["lob_loc bytea".into()])?;
                            let inputs = procedure_actual_arguments(function, &params, false)?;
                            let mut input = inputs[0]
                                .cloned()
                                .ok_or_else(|| unsupported("missing LOB scalar argument"))?;
                            blob_input(self.db, self.env, &mut input)?;
                            expression(&format!(
                                "bicdb_oracle_blob_{}({input})",
                                operation.to_ascii_lowercase()
                            ))
                        })();
                        match lower {
                            Ok(lowered) => *expr = lowered,
                            Err(error) => return ControlFlow::Break(Box::new(error)),
                        }
                    }
                }
            }
        }
        if self.env.number_formats && super::number_formats::input(expr).is_some() {
            let input = super::number_formats::input(expr).expect("checked input");
            let kind = match self.kind(input) {
                Kind::Number | Kind::Null => "number",
                Kind::Text => "text",
                Kind::Date | Kind::Timestamp(_) => "date",
                Kind::Unknown if self.env.sql_builtins_only && self.env.defer_bind_types => "defer",
                _ => "unsupported",
            };
            if kind != "defer" {
                if let Err(error) = super::number_formats::lower_expression(expr, kind) {
                    return ControlFlow::Break(Box::new(error));
                }
            }
        }

        if let Err(error) = super::session_context::lower_expression(expr) {
            return ControlFlow::Break(Box::new(error));
        }
        if self.env.text_patterns {
            let conversion = super::text_patterns::convert_text_arguments(expr, |value| {
                let kind = match self.kind(value) {
                    Kind::Text | Kind::Null => return Ok(value.clone()),
                    Kind::Number => "number",
                    Kind::Date => "date",
                    Kind::Raw => "raw",
                    Kind::Unknown if self.env.sql_builtins_only && self.env.defer_bind_types => return Ok(value.clone()),
                    Kind::NationalText | Kind::FixedNationalText => return Err(unsupported("national-character text patterns require a UTF16 execution contract")),
                    _ => return Err(unsupported("text patterns require resolved character, NUMBER or logical DATE input; use an explicit conversion")),
                };
                let context = if self.sql_depth > 0 || !self.queries.is_empty() {
                    "sql"
                } else {
                    "plsql"
                };
                expression(&format!(
                    "bicdb_oracle_default_to_char({},'{kind}','{context}')",
                    render_sql(value)
                ))
            });
            if let Err(error) = conversion {
                return ControlFlow::Break(Box::new(error));
            }
            if let Err(error) = super::text_patterns::lower_expression(expr) {
                return ControlFlow::Break(Box::new(error));
            }
        }
        if let Some(namespace) = self.env.collection_namespace.as_deref() {
            if self.sql_depth == 0 && self.queries.is_empty() {
                if let Err(error) =
                    super::collections::method_expression(expr, &self.env.collections)
                {
                    return ControlFlow::Break(Box::new(error));
                }
            }
            if let Err(error) =
                super::collections::constructor_expression(self.db, expr, Some(namespace))
            {
                return ControlFlow::Break(Box::new(error));
            }
        }
        if super::collections::multisets::enabled() {
            if let Err(error) = super::collections::multisets::lower_expression(
                self.db,
                expr,
                &self.env.collections,
                self.env.collection_namespace.as_deref(),
                self.sql_depth > 0 || !self.queries.is_empty(),
            ) {
                return ControlFlow::Break(Box::new(error));
            }
        }
        match self.rewrite(expr) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Box::new(error)),
        }
    }
}

fn visit<T: sqlparser::ast::VisitMut>(db: &BicDb, env: &Environment, node: &mut T) -> Result<()> {
    match node.visit(&mut Resolve {
        db,
        env,
        queries: Vec::new(),
        selects: Vec::new(),
        sql_depth: 0,
        statement_from: Vec::new(),
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

fn record_types(db: &BicDb, env: &Environment, query: &Query) -> BTreeMap<String, Kind> {
    let mut fields: BTreeMap<_, _> = infer_query_result_columns(db, &query.to_string())
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
        .collect();
    if let SetExpr::Select(select) = query.body.as_ref() {
        let resolve = Resolve {
            db,
            env,
            queries: vec![query.clone()],
            selects: Vec::new(),
            sql_depth: 1,
            statement_from: Vec::new(),
        };
        for (name, kind) in &mut fields {
            let expr = Expr::Identifier(Ident::new(name.as_str()));
            if let Some(source) = temporal_columns::logical_in_query(db, query, &expr) {
                *kind = match source.source {
                    temporal_columns::Source::Date => Kind::Date,
                    temporal_columns::Source::Timestamp => Kind::Timestamp(source.precision),
                    temporal_columns::Source::TimestampWithTimeZone => Kind::Other,
                };
            }
        }
        for item in &select.projection {
            let (expr, name) = match item {
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.clone()),
                SelectItem::UnnamedExpr(expr) => (expr, select_expr_column_name(expr)),
                _ => continue,
            };
            let kind = resolve.kind(expr);
            if kind != Kind::Unknown {
                fields.insert(name.to_ascii_lowercase(), kind);
            }
        }
    }
    fields
}

fn prepare_cursor(
    db: &BicDb,
    env: &Environment,
    name: &str,
    params: &mut [RoutineParam],
    query: &mut Query,
) -> Result<BTreeMap<String, Kind>> {
    let mut scope = env.clone();
    let mut qualified = BTreeMap::new();
    for param in params {
        if let Some(expr) = &mut param.default_expr {
            visit(db, env, expr)?;
        }
        let kind = if param.oracle_date {
            Kind::Date
        } else {
            param
                .type_schema
                .as_ref()
                .map_or(Kind::Unknown, schema_kind)
        };
        let name = normalize_object_name(param.name.as_ref().expect("validated formal"));
        scope.variables.insert(name.clone(), kind);
        qualified.insert(name, kind);
    }
    scope.records.insert(normalize_object_name(name), qualified);
    visit(db, &scope, query)?;
    Ok(record_types(db, &scope, query))
}

/// Select the conversion from compiler provenance, never from a runtime
/// string's spelling. A PostgreSQL bytea-looking text is still character data.
fn blob_input(db: &BicDb, env: &Environment, value: &mut Expr) -> Result<()> {
    let resolve = Resolve {
        db,
        env,
        queries: Vec::new(),
        selects: Vec::new(),
        sql_depth: 0,
        statement_from: Vec::new(),
    };
    match resolve.kind(value) {
        Kind::Blob | Kind::Null => Ok(()),
        Kind::Raw => {
            *value = expression(&format!("bicdb_oracle_blob_from_raw({value})"))?;
            Ok(())
        }
        _ => Err(unsupported(
            "BLOB assignment requires a qualified binary conversion",
        )),
    }
}

fn statements(
    db: &BicDb,
    env: &Environment,
    cursors: &BTreeMap<String, BTreeMap<String, Kind>>,
    body: &mut [RoutineStmt],
) -> Result<()> {
    for statement in body {
        if env.collection_namespace.is_some() {
            super::collections::method_statement(statement, &env.collections)?;
        }
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                let mut local_env = env.clone();
                let mut local_cursors = cursors.clone();
                if env.collection_namespace.is_some() {
                    super::collections::declaration_bindings(
                        db,
                        &mut local_env.collections,
                        declarations,
                    )?;
                }
                for declaration in declarations.iter() {
                    match declaration {
                        RoutineDecl::Variable {
                            name,
                            pg_type,
                            oracle_date,
                            type_modifier,
                            ..
                        } => {
                            let kind = if *oracle_date {
                                Kind::Date
                            } else if pg_type.as_deref() == Some("timestamp") {
                                Kind::Timestamp(temporal_precision(type_modifier.as_ref()))
                            } else {
                                if national::is_modifier(type_modifier.as_ref())
                                    || lobs::is_modifier(type_modifier.as_ref())
                                {
                                    schema_kind(&RoutineTypeSchema {
                                        pg_type: pg_type.clone().unwrap_or_default(),
                                        type_modifier: type_modifier.clone(),
                                    })
                                } else {
                                    pg_type.as_deref().map_or(Kind::Unknown, pg_kind)
                                }
                            };
                            local_env
                                .variables
                                .insert(normalize_object_name(name), kind);
                        }
                        RoutineDecl::Alias { name, position } => {
                            if let Some(kind) = env.variables.get(&format!("${position}")) {
                                local_env
                                    .variables
                                    .insert(normalize_object_name(name), *kind);
                            }
                        }
                        _ => {}
                    }
                }
                for declaration in declarations {
                    match declaration {
                        RoutineDecl::Variable {
                            name,
                            default_expr: Some(expr),
                            ..
                        } => {
                            visit(db, &local_env, &mut expr.expr)?;
                            if local_env.variables.get(&normalize_object_name(name))
                                == Some(&Kind::Blob)
                            {
                                blob_input(db, &local_env, &mut expr.expr)?;
                            }
                        }
                        RoutineDecl::Cursor {
                            name,
                            params,
                            query,
                        } => {
                            let fields = prepare_cursor(db, &local_env, name, params, query)?;
                            local_cursors.insert(normalize_object_name(name), fields);
                        }
                        _ => {}
                    }
                }
                statements(db, &local_env, &local_cursors, body)?;
                for handler in handlers {
                    statements(db, &local_env, &local_cursors, &mut handler.statements)?;
                }
            }
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    visit(db, env, &mut index.expr)?;
                }
                visit(db, env, &mut expr.expr)?;
                if let RoutineAssignmentTarget::Variable(name) = target {
                    if env.variables.get(&normalize_object_name(name)) == Some(&Kind::Blob) {
                        blob_input(db, env, &mut expr.expr)?;
                    }
                }
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
                scope.collections.remove(&normalize_object_name(iterator));
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
                scope.collections.remove(&normalize_object_name(target));
                scope
                    .records
                    .insert(normalize_object_name(target), record_types(db, env, query));
                statements(db, &scope, cursors, body)?;
            }
            RoutineStmt::CursorForLoop {
                target,
                cursor,
                arguments,
                body,
            } => {
                for arg in arguments {
                    visit(db, env, &mut arg.expr.expr)?;
                }
                let mut scope = env.clone();
                scope.collections.remove(&normalize_object_name(target));
                if let Some(fields) = cursors.get(cursor) {
                    scope
                        .records
                        .insert(normalize_object_name(target), fields.clone());
                }
                statements(db, &scope, cursors, body)?;
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    visit(db, env, &mut arg.expr.expr)?;
                }
            }
            RoutineStmt::Loop { body } => statements(db, env, cursors, body)?,
            RoutineStmt::SelectInto { query, .. }
            | RoutineStmt::Perform { query }
            | RoutineStmt::ReturnQuery(query) => visit(db, env, query)?,
            RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                visit(db, env, statement)?
            }
            RoutineStmt::PostgresControl(_) => {
                return Err(SqlError::Unsupported(
                    "PostgreSQL procedural controls are not Oracle preview statements".into(),
                ))
            }
            RoutineStmt::OracleDynamicExecute(dynamic) => {
                visit(db, env, &mut dynamic.command.expr)?;
                for bind in &mut dynamic.using {
                    visit(db, env, &mut bind.expr)?;
                    let resolve = Resolve {
                        db,
                        env,
                        queries: Vec::new(),
                        selects: Vec::new(),
                        sql_depth: 0,
                        statement_from: Vec::new(),
                    };
                    if resolve.kind(&bind.expr).is_national() {
                        bind.expr = expression(&format!(
                            "bicdb_oracle_national_value({})",
                            super::render_sql(&bind.expr)
                        ))?;
                    }
                }
            }
            RoutineStmt::Return(Some(expr)) => {
                visit(db, env, &mut expr.expr)?;
                if env.blob_return {
                    blob_input(db, env, &mut expr.expr)?;
                }
            }
            RoutineStmt::DynamicExecute(expr) => {
                visit(db, env, &mut expr.expr)?;
            }
            RoutineStmt::OracleBuiltin { builtin, arguments } => {
                if matches!(builtin, super::errors::Builtin::LobAppend) {
                    let Expr::Identifier(destination) = &arguments[0].expr else {
                        return Err(unsupported(
                            "LOB APPEND destination requires a declared BLOB variable",
                        ));
                    };
                    if env
                        .variables
                        .get(&normalize_object_name(&ident_value(destination)))
                        != Some(&Kind::Blob)
                    {
                        return Err(unsupported(
                            "LOB APPEND destination requires a declared BLOB variable",
                        ));
                    }
                    visit(db, env, &mut arguments[1].expr)?;
                    blob_input(db, env, &mut arguments[1].expr)?;
                    continue;
                }
                // Output destinations are lvalues, never converted input
                // expressions. Other supplied operations have independent
                // signatures rather than application-error's three arguments.
                if !matches!(
                    builtin,
                    super::errors::Builtin::Output(super::output::Operation::GetLine)
                ) {
                    for arg in arguments.iter_mut() {
                        visit(db, env, &mut arg.expr)?;
                    }
                }
                let message_index = match builtin {
                    super::errors::Builtin::RaiseApplicationError => Some(1),
                    super::errors::Builtin::Output(
                        super::output::Operation::Put | super::output::Operation::PutLine,
                    ) => Some(0),
                    _ => None,
                };
                let Some(message_index) = message_index else {
                    continue;
                };
                let resolve = Resolve {
                    db,
                    env,
                    queries: Vec::new(),
                    selects: Vec::new(),
                    sql_depth: 0,
                    statement_from: Vec::new(),
                };
                let message = &mut arguments[message_index].expr;
                let kind=match resolve.kind(message) {
                    Kind::Date=>Some("date"),Kind::Number=>Some("number"),
                    Kind::Text|Kind::NationalText|Kind::FixedNationalText|Kind::Null=>None,
                    _=>return Err(SqlError::Unsupported(match builtin {
                        super::errors::Builtin::RaiseApplicationError => "RAISE_APPLICATION_ERROR msg requires a resolved character, NUMBER or logical DATE operand",
                        _ => "Oracle character procedure input requires a resolved character, NUMBER or logical DATE operand",
                    }.into())),
                };
                if let Some(kind) = kind {
                    *message = expression(&format!(
                        "bicdb_oracle_default_to_char({message},'{kind}','plsql')"
                    ))?;
                }
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
            | RoutineStmt::Rethrow
            | RoutineStmt::ExitLoop
            | RoutineStmt::Null
            | RoutineStmt::FetchCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::ReturnNext
            | RoutineStmt::Return(None) => {}
        }
    }
    Ok(())
}

pub(crate) fn lower(
    db: &BicDb,
    blob_return: bool,
    collection_namespace: Option<&str>,
    statement_provenance: bool,
    text_patterns: bool,
    v4_semantics: bool,
    concatenation: bool,
    number_arithmetic: bool,
    decode: bool,
    raw_conversion: bool,
    character_conversion: bool,
    nvl: bool,
    nvl2: bool,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    body: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    let mut env = Environment {
        blob_return,
        collection_namespace: collection_namespace.map(str::to_string),
        number_formats: super::number_formats::enabled(),
        statement_provenance,
        text_patterns,
        v4_semantics,
        concatenation,
        number_arithmetic,
        decode,
        character_conversion,
        raw_conversion,
        nvl,
        nvl2,
        // Contract 27 introduces national literal provenance with NVL2;
        // previous persisted compiler contracts retain their existing rules.
        national_literals: nvl2,
        ..Environment::default()
    };
    for param in params.iter() {
        let kind = if param.oracle_date {
            Kind::Date
        } else {
            param.type_schema.as_ref().map_or(Kind::Unknown, |schema| {
                if schema.pg_type == "timestamp" {
                    Kind::Timestamp(temporal_precision(schema.type_modifier.as_ref()))
                } else {
                    schema_kind(schema)
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
                type_modifier,
                ..
            } => {
                let kind = if *oracle_date {
                    Kind::Date
                } else if pg_type.as_deref() == Some("timestamp") {
                    Kind::Timestamp(temporal_precision(type_modifier.as_ref()))
                } else {
                    if national::is_modifier(type_modifier.as_ref())
                        || lobs::is_modifier(type_modifier.as_ref())
                    {
                        schema_kind(&RoutineTypeSchema {
                            pg_type: pg_type.clone().unwrap_or_default(),
                            type_modifier: type_modifier.clone(),
                        })
                    } else {
                        pg_type.as_deref().map_or(Kind::Unknown, pg_kind)
                    }
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
    if collection_namespace.is_some() {
        for param in params.iter() {
            if let Some(kind) = &param.type_schema {
                if let Some(collection) = super::collections::schema_for_type(db, &kind.pg_type)? {
                    let binding = super::collections::CollectionBinding {
                        type_name: kind.pg_type.clone(),
                        schema: collection,
                    };
                    env.collections
                        .insert(format!("${}", param.index + 1), binding.clone());
                    if let Some(name) = &param.name {
                        env.collections.insert(normalize_object_name(name), binding);
                    }
                }
            }
        }
        super::collections::declaration_bindings(db, &mut env.collections, declarations)?;
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
                name,
                default_expr: Some(expr),
                ..
            } => {
                visit(db, &env, &mut expr.expr)?;
                if env.variables.get(&normalize_object_name(name)) == Some(&Kind::Blob) {
                    blob_input(db, &env, &mut expr.expr)?;
                }
            }
            RoutineDecl::Cursor {
                name,
                params,
                query,
            } => {
                let fields = prepare_cursor(db, &env, name, params, query)?;
                cursors.insert(normalize_object_name(name), fields);
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
