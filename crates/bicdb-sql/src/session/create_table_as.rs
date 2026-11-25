//! `CREATE TABLE ... AS query` and `SELECT ... INTO table`.
//!
//! The query runs once, through the session's ordinary query path, so the
//! creator needs SELECT on every source and row-level security of a source
//! applies. The new table takes PostgreSQL's names and types for the query's
//! output columns (type modifiers included); its rows go through the normal
//! record validation and durable write path, all inside one statement
//! transaction with the table's creation.
use super::*;
use sqlparser::ast::SelectInto;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Word};

/// One `CREATE TABLE AS` / `SELECT INTO` request.
pub(crate) struct CreateTableAs {
    /// The table definition without its query: the name, TEMPORARY and any
    /// other clauses the ordinary CREATE TABLE path handles.
    pub(crate) definition: CreateTable,
    /// `CREATE TABLE t(a, b) AS ...`: names replacing the leading output names.
    pub(crate) column_names: Vec<String>,
    pub(crate) query: Query,
    /// False for `WITH NO DATA`.
    pub(crate) with_data: bool,
}

impl CreateTableAs {
    pub(crate) fn from_parsed(create_table: &CreateTable) -> Result<Self> {
        let Some(query) = create_table.query.as_deref() else {
            return Err(SqlError::InvalidSql(
                "CREATE TABLE AS requires a query".to_string(),
            ));
        };
        if !create_table.columns.is_empty() || !create_table.constraints.is_empty() {
            // PostgreSQL's grammar accepts only a list of column names here.
            return Err(SqlError::InvalidSql(
                "syntax error at or near \"AS\"".to_string(),
            ));
        }
        let mut definition = create_table.clone();
        definition.query = None;
        Ok(Self {
            definition,
            column_names: Vec::new(),
            query: query.clone(),
            with_data: true,
        })
    }
}

fn ctas_error(sqlstate: &'static str, message: String) -> SqlError {
    SqlError::data_exception(sqlstate, message, None)
}

/// PostgreSQL's spelling of a result column's type, for the new column.
fn result_column_type_text(pg_type: &str, typmod: i32) -> String {
    let pg_type = pg_type.trim();
    if pg_type.is_empty() || pg_type.eq_ignore_ascii_case("unknown") {
        // An untyped literal resolves to text, as in PostgreSQL.
        return "text".to_string();
    }
    if pg_type.eq_ignore_ascii_case("bpchar") && typmod < 0 {
        return "bpchar".to_string();
    }
    if let Some(oid) = pg_type_oid_by_name(pg_type) {
        if let Some(text) = pg_format_type(oid, typmod.max(-1)) {
            return text;
        }
    }
    pg_type.to_string()
}

fn parse_column_data_type(text: &str) -> Result<DataType> {
    let dialect = PostgreSqlDialect {};
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(text)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let data_type = parser
        .parse_data_type()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(SqlError::Unsupported(format!(
            "CREATE TABLE AS cannot create a column of type {text}"
        )));
    }
    Ok(data_type)
}

/// Type modifiers of explicit casts in the select list (`x::varchar(5)`):
/// PostgreSQL keeps them on the output column.
fn cast_type_modifiers(query: &Query, columns: usize) -> Vec<Option<i32>> {
    let mut modifiers = vec![None; columns];
    let mut body = query.body.as_ref();
    loop {
        match body {
            SetExpr::Query(inner) if inner.with.is_none() => body = inner.body.as_ref(),
            SetExpr::SetOperation { left, .. } => body = left.as_ref(),
            _ => break,
        }
    }
    let SetExpr::Select(select) = body else {
        return modifiers;
    };
    if matches!(query.body.as_ref(), SetExpr::SetOperation { .. })
        || select.projection.len() != columns
    {
        return modifiers;
    }
    for (slot, item) in modifiers.iter_mut().zip(&select.projection) {
        let mut expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return vec![None; columns],
        };
        while let Expr::Nested(inner) = expr {
            expr = inner;
        }
        if let Expr::Cast { data_type, .. } = expr {
            if let Ok(Some(modifier)) = pg_type_modifier_from_data_type(data_type) {
                *slot = i32::try_from(modifier.catalog_value()).ok();
            }
        }
    }
    modifiers
}

/// A query that reads no rows but keeps the output columns: `WITH NO DATA`
/// never runs the query in PostgreSQL.
fn without_rows(query: &Query) -> Query {
    let mut query = query.clone();
    query.limit_clause = Some(LimitClause::LimitOffset {
        limit: Some(Expr::Value(Value::Number("0".to_string(), false).into())),
        offset: None,
        limit_by: Vec::new(),
    });
    query.fetch = None;
    query.locks.clear();
    query
}

/// The leftmost `SELECT` of a set operation carries PostgreSQL's top-level INTO.
fn top_level_into(query: &mut Query) -> Option<SelectInto> {
    let mut body = query.body.as_mut();
    loop {
        match body {
            SetExpr::Select(select) => return select.into.take(),
            SetExpr::SetOperation { left, .. } => body = left.as_mut(),
            _ => return None,
        }
    }
}

struct NestedIntoFinder(bool);

impl sqlparser::ast::Visitor for NestedIntoFinder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        fn has_into(body: &SetExpr) -> bool {
            match body {
                SetExpr::Select(select) => select.into.is_some(),
                SetExpr::SetOperation { left, right, .. } => has_into(left) || has_into(right),
                SetExpr::Query(query) => has_into(&query.body),
                _ => false,
            }
        }
        if has_into(&query.body) {
            self.0 = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn query_has_into(query: &Query) -> bool {
    use sqlparser::ast::Visit;
    let mut finder = NestedIntoFinder(false);
    let _ = query.visit(&mut finder);
    finder.0
}

impl SqlSession<'_> {
    /// `SELECT ... INTO [TEMP] [TABLE] name ...` at statement level. `None`
    /// when the query has no INTO.
    pub(crate) fn execute_select_into(&mut self, query: &Query) -> Result<Option<SqlResult>> {
        if !query_has_into(query) {
            return Ok(None);
        }
        let mut query = query.clone();
        let Some(into) = top_level_into(&mut query) else {
            return Err(SqlError::InvalidSql(
                "SELECT ... INTO is not allowed here".to_string(),
            ));
        };
        if query_has_into(&query) {
            return Err(SqlError::InvalidSql(
                "SELECT ... INTO is not allowed here".to_string(),
            ));
        }
        if into.unlogged {
            return Err(SqlError::Unsupported(
                "SELECT ... INTO UNLOGGED is not supported".to_string(),
            ));
        }
        let sql = format!(
            "CREATE {}TABLE {} ()",
            if into.temporary { "TEMPORARY " } else { "" },
            into.name
        );
        let statements = sqlparser::parser::Parser::parse_sql(&PostgreSqlDialect {}, &sql)
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
        let [Statement::CreateTable(definition)] = statements.as_slice() else {
            return Err(SqlError::InvalidSql(format!(
                "invalid SELECT INTO target {}",
                into.name
            )));
        };
        self.execute_create_table_as(CreateTableAs {
            definition: definition.clone(),
            column_names: Vec::new(),
            query,
            with_data: true,
        })
        .map(Some)
    }

    /// Relation statements sqlparser does not read: CREATE TABLE AS with a
    /// column-name list or a data clause.
    pub(crate) fn execute_raw_relation_forms(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if let Some(result) = self.execute_raw_create_table_as(sql)? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    /// The forms sqlparser does not read: a column-name list
    /// (`CREATE TABLE t(a, b) AS ...`) and a trailing `WITH [NO] DATA`.
    pub(crate) fn execute_raw_create_table_as(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(raw) = raw_create_table_as(sql) else {
            return Ok(None);
        };
        let statements = parse_statements(&raw.sql)?;
        let [Statement::CreateTable(create_table)] = statements.as_slice() else {
            return Ok(None);
        };
        if create_table.query.is_none() {
            return Ok(None);
        }
        let mut request = CreateTableAs::from_parsed(create_table)?;
        request.column_names = raw.column_names;
        request.with_data = raw.with_data;
        self.execute_create_table_as(request).map(Some)
    }

    pub(crate) fn execute_create_table_as(&mut self, request: CreateTableAs) -> Result<SqlResult> {
        self.require_transaction_write()?;
        let table = relation_name(&request.definition.name)?;
        let schema_name = relation_schema_name(&request.definition.name);
        let relation_exists = load_schema(self.db_ref(), &table)?.is_some()
            || load_sequence(self.db_ref(), &table)?.is_some()
            || load_view(self.db_ref(), &table)?.is_some()
            || load_user_type(self.db_ref(), &schema_name, &table)?.is_some()
            || user_collection_names(self.db_ref())
                .iter()
                .any(|name| name == &table);
        let short = table.rsplit('.').next().unwrap_or(&table).to_string();
        if relation_exists {
            if request.definition.if_not_exists {
                let mut result = SqlResult::command("CREATE TABLE AS");
                result.notices.push(SqlNotice {
                    sqlstate: "42P07".to_string(),
                    message: format!("relation \"{short}\" already exists, skipping"),
                    fields: Vec::new(),
                });
                return Ok(result);
            }
            return Err(ctas_error(
                "42P07",
                format!("relation \"{short}\" already exists"),
            ));
        }
        self.with_statement_transaction(|session| session.create_table_as_inner(&request, &table))
    }

    fn create_table_as_inner(&mut self, request: &CreateTableAs, table: &str) -> Result<SqlResult> {
        let source = if request.with_data {
            self.execute_query(&request.query)?
        } else {
            self.execute_query(&without_rows(&request.query))?
        };
        if request.column_names.len() > source.columns.len() {
            return Err(ctas_error(
                "42601",
                "too many column names were specified".to_string(),
            ));
        }
        let mut names = source.columns.clone();
        if let Some(figured) = postgres_output_names(&request.query) {
            if figured.len() == names.len() {
                names = figured;
            }
        }
        for (name, alias) in names.iter_mut().zip(&request.column_names) {
            *name = alias.clone();
        }
        for (index, name) in names.iter().enumerate() {
            if names[..index].contains(name) {
                return Err(ctas_error(
                    "42701",
                    format!("column \"{name}\" specified more than once"),
                ));
            }
        }
        let cast_modifiers = cast_type_modifiers(&request.query, names.len());
        let mut columns = Vec::with_capacity(names.len());
        for (index, name) in names.iter().enumerate() {
            let pg_type = source
                .column_types
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| "text".to_string());
            let typmod = source
                .column_metadata
                .get(index)
                .map(|metadata| metadata.type_modifier)
                .filter(|modifier| *modifier >= 0)
                .or(cast_modifiers.get(index).copied().flatten())
                .unwrap_or(-1);
            columns.push(ColumnDef {
                name: Ident::with_quote('"', name.clone()),
                data_type: parse_column_data_type(&result_column_type_text(&pg_type, typmod))?,
                options: Vec::new(),
            });
        }
        let mut definition = request.definition.clone();
        definition.columns = columns;
        definition.query = None;
        self.execute_create_table_definition(&definition, &[], false)?;
        if !request.with_data {
            return Ok(SqlResult::command("CREATE TABLE AS"));
        }
        let target = resolve_session_relation_name_if_exists(self.db_ref(), table);
        let schema = load_schema_shared(self.db_ref(), &target)?;
        let count = source.rows.len();
        let records =
            self.records_from_insert_result(&target, schema.as_deref(), &names, source.rows)?;
        if let Some(schema) = schema.as_deref() {
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                &target,
                schema,
                &records,
                false,
                Some(&self.sql_engine()),
            )?;
        }
        self.insert_session_new_records(&target, schema.as_deref(), records)?;
        let mut result = SqlResult::command(format!("SELECT {count}"));
        result.notices = source.notices;
        Ok(result)
    }
}

pub(crate) struct RawCreateTableAs {
    /// `CREATE ... TABLE name AS query`, without the column list and the
    /// `WITH [NO] DATA` suffix.
    pub(crate) sql: String,
    pub(crate) column_names: Vec<String>,
    pub(crate) with_data: bool,
}

fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    let line_start = *line_starts.get((location.line as usize).checked_sub(1)?)?;
    let column = (location.column as usize).checked_sub(1)?;
    let rest = sql.get(line_start..)?;
    Some(
        line_start
            + rest
                .char_indices()
                .nth(column)
                .map(|(offset, _)| offset)
                .unwrap_or(rest.len()),
    )
}

fn keyword_is(token: &TokenWithSpan, keyword: &str) -> bool {
    matches!(&token.token, Token::Word(Word { value, quote_style: None, .. })
        if value.eq_ignore_ascii_case(keyword))
}

/// Recognizes `CREATE [TEMP|TEMPORARY|LOCAL TEMP|GLOBAL TEMP] TABLE [IF NOT
/// EXISTS] name [(col, ...)] AS query [WITH [NO] DATA]` when it uses a form
/// sqlparser cannot read (a column-name list or the data clause).
pub(crate) fn raw_create_table_as(sql: &str) -> Option<RawCreateTableAs> {
    let head = sql.trim_start();
    if head.len() < 6 || !head[..6].eq_ignore_ascii_case("create") {
        return None;
    }
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("TABLE") || !upper.contains("AS") {
        return None;
    }
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .ok()?;
    let solid: Vec<&TokenWithSpan> = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let mut index = 0;
    let next_is =
        |index: usize, keyword: &str| solid.get(index).is_some_and(|t| keyword_is(t, keyword));
    if !next_is(index, "CREATE") {
        return None;
    }
    index += 1;
    if next_is(index, "GLOBAL") || next_is(index, "LOCAL") {
        index += 1;
    }
    if next_is(index, "TEMP") || next_is(index, "TEMPORARY") {
        index += 1;
    }
    if !next_is(index, "TABLE") {
        return None;
    }
    index += 1;
    if next_is(index, "IF") {
        if !(next_is(index + 1, "NOT") && next_is(index + 2, "EXISTS")) {
            return None;
        }
        index += 3;
    }
    // name: ident [. ident [. ident]]
    if !matches!(solid.get(index)?.token, Token::Word(_)) {
        return None;
    }
    index += 1;
    while matches!(solid.get(index).map(|t| &t.token), Some(Token::Period)) {
        if !matches!(solid.get(index + 1)?.token, Token::Word(_)) {
            return None;
        }
        index += 2;
    }
    let name_end_token = solid.get(index - 1)?;
    let mut column_names = Vec::new();
    if matches!(solid.get(index)?.token, Token::LParen) {
        index += 1;
        loop {
            let Token::Word(word) = &solid.get(index)?.token else {
                return None;
            };
            column_names.push(if word.quote_style.is_some() {
                word.value.clone()
            } else {
                word.value.to_lowercase()
            });
            index += 1;
            match solid.get(index)?.token {
                Token::Comma => index += 1,
                Token::RParen => {
                    index += 1;
                    break;
                }
                _ => return None,
            }
        }
    }
    if !next_is(index, "AS") {
        return None;
    }
    let as_token = solid.get(index)?;
    // `AS EXECUTE name` is a different statement form.
    if next_is(index + 1, "EXECUTE") {
        return None;
    }
    let mut end = solid.len();
    while end > index + 1 && matches!(solid[end - 1].token, Token::SemiColon) {
        end -= 1;
    }
    let mut with_data = true;
    let mut data_clause = None;
    if end >= index + 3 && keyword_is(solid[end - 1], "DATA") {
        if keyword_is(solid[end - 2], "WITH") {
            data_clause = Some(end - 2);
        } else if end >= index + 4
            && keyword_is(solid[end - 2], "NO")
            && keyword_is(solid[end - 3], "WITH")
        {
            with_data = false;
            data_clause = Some(end - 3);
        }
    }
    if column_names.is_empty() && data_clause.is_none() {
        return None;
    }
    let mut line_starts = vec![0];
    line_starts.extend(sql.match_indices('\n').map(|(offset, _)| offset + 1));
    let name_end = byte_offset(sql, &line_starts, name_end_token.span.end)?;
    let query_start = byte_offset(sql, &line_starts, as_token.span.end)?;
    let query_end = match data_clause {
        Some(position) => byte_offset(sql, &line_starts, solid[position].span.start)?,
        None => byte_offset(sql, &line_starts, solid[end - 1].span.end)?,
    };
    if query_start >= query_end {
        return None;
    }
    let rewritten = format!(
        "{} AS {}",
        sql.get(..name_end)?,
        sql.get(query_start..query_end)?.trim()
    );
    Some(RawCreateTableAs {
        sql: rewritten,
        column_names,
        with_data,
    })
}

/// PostgreSQL's names for a select list's output columns (FigureColname),
/// when the list has no wildcards.
fn postgres_output_names(query: &Query) -> Option<Vec<String>> {
    let mut body = query.body.as_ref();
    loop {
        match body {
            SetExpr::Query(inner) => body = inner.body.as_ref(),
            SetExpr::SetOperation { left, .. } => body = left.as_ref(),
            _ => break,
        }
    }
    let SetExpr::Select(select) = body else {
        return None;
    };
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(ident_value(alias)),
            SelectItem::UnnamedExpr(expr) => Some(
                figure_column_name(expr)
                    .map(|(name, _)| name)
                    .unwrap_or_else(|| "?column?".to_string()),
            ),
            _ => None,
        })
        .collect()
}

/// (name, strength): strength 2 for a real name, 1 for a fallback word.
fn figure_column_name(expr: &Expr) -> Option<(String, u8)> {
    match expr {
        Expr::Identifier(ident) => Some((ident_value(ident), 2)),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| (ident_value(part), 2)),
        Expr::Nested(inner) | Expr::Collate { expr: inner, .. } => figure_column_name(inner),
        Expr::Cast {
            expr, data_type, ..
        } => match figure_column_name(expr) {
            Some((name, 2)) => Some((name, 2)),
            _ => Some((cast_type_column_name(data_type), 1)),
        },
        Expr::TypedString(typed) => Some((cast_type_column_name(&typed.data_type), 1)),
        Expr::Function(function) => {
            let name = function.name.0.last()?.as_ident()?;
            Some((ident_value(name), 2))
        }
        Expr::Case { else_result, .. } => match else_result.as_deref().and_then(figure_column_name)
        {
            Some((name, 2)) => Some((name, 2)),
            _ => Some(("case".to_string(), 1)),
        },
        Expr::Exists { .. } => Some(("exists".to_string(), 2)),
        Expr::Subquery(query) => postgres_output_names(query)
            .and_then(|names| names.into_iter().next())
            .map(|name| (name, 2)),
        Expr::Array(_) => Some(("array".to_string(), 2)),
        Expr::Tuple(_) => Some(("row".to_string(), 2)),
        Expr::Interval(_) => Some(("interval".to_string(), 1)),
        Expr::Extract { .. } => Some(("extract".to_string(), 2)),
        Expr::Substring { .. } => Some(("substring".to_string(), 2)),
        Expr::Position { .. } => Some(("position".to_string(), 2)),
        Expr::Trim { .. } => Some(("btrim".to_string(), 2)),
        Expr::Ceil { .. } => Some(("ceil".to_string(), 2)),
        Expr::Floor { .. } => Some(("floor".to_string(), 2)),
        Expr::AtTimeZone { .. } => Some(("timezone".to_string(), 2)),
        _ => None,
    }
}

/// The type name PostgreSQL gives a column that is a cast of an unnamed value.
fn cast_type_column_name(data_type: &DataType) -> String {
    let text = data_type.to_string().to_ascii_lowercase();
    let base = text
        .split(['(', '['])
        .next()
        .unwrap_or(&text)
        .trim()
        .to_string();
    match base.as_str() {
        "int" | "integer" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "real" | "float4" => "float4",
        "double precision" | "float8" | "float" => "float8",
        "boolean" | "bool" => "bool",
        "character varying" | "varchar" => "varchar",
        "character" | "char" | "bpchar" => "bpchar",
        "decimal" | "numeric" | "dec" => "numeric",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "time with time zone" | "timetz" => "timetz",
        other if other.starts_with("timestamp") => {
            if other.contains("with time zone") {
                "timestamptz"
            } else {
                "timestamp"
            }
        }
        other if other.starts_with("time") && !other.starts_with("timestamp") => {
            if other.contains("with time zone") {
                "timetz"
            } else {
                "time"
            }
        }
        other if other.starts_with("interval") => "interval",
        other => {
            return other
                .rsplit('.')
                .next()
                .unwrap_or(other)
                .trim_matches('"')
                .to_string()
        }
    }
    .to_string()
}
