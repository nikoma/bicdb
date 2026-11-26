//! `CREATE CAST` / `DROP CAST` and their use.
//!
//! A user cast is recorded in its own catalog collection and listed in
//! `pg_cast`. Before execution, a statement's explicit casts
//! (`CAST(x AS t)`, `x::t`) whose source type a user cast converts to `t`
//! are rewritten to the cast's function call (`WITH FUNCTION`), to a
//! conversion through text (`WITH INOUT`), or left as written (`WITHOUT
//! FUNCTION`, a binary-coercible cast the executor already performs).
//! `AS IMPLICIT` casts also convert arguments of user function calls whose
//! single candidate needs them, and `AS ASSIGNMENT` / `AS IMPLICIT` casts
//! convert INSERT values to the column type, as PostgreSQL's coercion rules
//! apply them.

use crate::*;
use sqlparser::ast::{
    CastKind, DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SetExpr, Statement,
    VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

pub(crate) const CAST_COLLECTION: &str = "__bicdb_pg_casts";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct UserCast {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) source_display: String,
    pub(crate) target_display: String,
    /// `WITH FUNCTION name`; None for WITHOUT FUNCTION / WITH INOUT.
    pub(crate) function: Option<String>,
    /// 'f' function, 'b' binary (WITHOUT FUNCTION), 'i' WITH INOUT.
    pub(crate) method: char,
    /// 'e' explicit, 'a' assignment, 'i' implicit.
    pub(crate) context: char,
    pub(crate) owner: String,
}

fn key(source: &str, target: &str) -> String {
    format!("{source}->{target}")
}

thread_local! {
    static CASTS: std::cell::RefCell<Option<(u64, u64, Arc<Vec<UserCast>>)>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn user_casts(db: &BicDb) -> Result<Arc<Vec<UserCast>>> {
    let database = db.instance_id();
    let generation = db.collection_generation(CAST_COLLECTION);
    if let Some(list) = CASTS.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, list)| {
            (*id == database && *cached == generation).then(|| list.clone())
        })
    }) {
        return Ok(list);
    }
    let records = match db.scan_collection(CAST_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let list = Arc::new(
        records
            .into_iter()
            .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
            .collect::<Result<Vec<UserCast>>>()?,
    );
    CASTS.with(|cache| *cache.borrow_mut() = Some((database, generation, list.clone())));
    Ok(list)
}

fn find_cast<'a>(casts: &'a [UserCast], source: &str, target: &str) -> Option<&'a UserCast> {
    casts
        .iter()
        .find(|cast| cast.source == source && cast.target == target)
}

pub(crate) fn save_user_cast(db: &mut BicDb, cast: &UserCast) -> Result<()> {
    db.create_collection(CAST_COLLECTION)?;
    db.insert(
        CAST_COLLECTION,
        Record::new(key(&cast.source, &cast.target)).with_metadata(serde_json::to_value(cast)?),
    )?;
    Ok(())
}

pub(crate) fn delete_user_cast(db: &mut BicDb, source: &str, target: &str) -> Result<()> {
    match db.delete(CAST_COLLECTION, &key(source, target)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Canonical type name of a type as written, and its display name.
fn type_names(db: &BicDb, text: &str) -> Result<(String, String)> {
    let data_type = crate::pg_operators::parse_sql_data_type(text.trim())?;
    let pg_type = canonical_data_type(db, &data_type)?;
    let display = if pg_type_oid_by_name(&pg_type).is_some() {
        common_type_display(&pg_type)
    } else {
        pg_type.clone()
    };
    Ok((pg_type, display))
}

fn canonical_data_type(db: &BicDb, data_type: &DataType) -> Result<String> {
    if let Some(user_type) = user_type_column_from_data_type(db, data_type)? {
        return Ok(user_type.formatted_name());
    }
    let (pg_type, _) = pg_type_from_data_type(data_type)?;
    Ok(crate::pg_operators::canonical_type(&pg_type))
}

fn syntax(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42601", message.into(), None)
}

/// `(source AS target)` at the start of `text`: the two type texts and the
/// remaining text.
fn cast_pair(text: &str) -> Result<(String, String, &str)> {
    let text = text.trim_start();
    if !text.starts_with('(') {
        return Err(syntax("CAST signature must be (source AS target)"));
    }
    let mut depth = 0usize;
    let mut close = None;
    for (index, ch) in text.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close.ok_or_else(|| syntax("unterminated cast signature"))?;
    let inner = &text[1..close];
    let at = find_top_level_keyword(inner, "AS")
        .ok_or_else(|| syntax("CAST signature must be (source AS target)"))?;
    Ok((
        inner[..at].trim().to_string(),
        inner[at + 2..].trim().to_string(),
        &text[close + 1..],
    ))
}

fn starts_with_words(text: &str, words: &[&str]) -> Option<usize> {
    let mut rest = text;
    let mut consumed = 0usize;
    for word in words {
        let trimmed = rest.trim_start();
        consumed += rest.len() - trimmed.len();
        if trimmed.len() < word.len() || !trimmed[..word.len()].eq_ignore_ascii_case(word) {
            return None;
        }
        if trimmed[word.len()..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
        {
            return None;
        }
        consumed += word.len();
        rest = &trimmed[word.len()..];
    }
    Some(consumed)
}

impl SqlSession<'_> {
    fn require_cast_privilege(&self, source: &str, target: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        for pg_type in [source, target] {
            let (schema, name) = pg_type.rsplit_once('.').unwrap_or(("public", pg_type));
            if let Some(user_type) = load_user_type(self.db_ref(), schema, name)? {
                if self.current_user_holds_role(&user_type.owner)? {
                    return Ok(());
                }
            }
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of type {source} or type {target}"
        ))))
    }

    /// `CREATE CAST (source AS target) WITH FUNCTION f[(args)] | WITHOUT
    /// FUNCTION | WITH INOUT [AS ASSIGNMENT | AS IMPLICIT]`.
    pub(crate) fn execute_raw_create_cast(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let text = sql.trim().trim_end_matches(';').trim();
        let Some(prefix) = starts_with_words(text, &["create", "cast"]) else {
            return Ok(None);
        };
        let (source_text, target_text, mut rest) = cast_pair(&text[prefix..])?;
        let (source, source_display) = type_names(self.db_ref(), &source_text)?;
        let (target, target_display) = type_names(self.db_ref(), &target_text)?;
        let (method, function) = if let Some(used) = starts_with_words(rest, &["with", "function"])
        {
            rest = &rest[used..];
            let trimmed = rest.trim_start();
            let end = trimmed
                .find(|ch: char| ch == '(' || ch.is_whitespace())
                .unwrap_or(trimmed.len());
            let name = normalize_object_name(&trimmed[..end]);
            rest = &trimmed[end..];
            if rest.trim_start().starts_with('(') {
                let rest_trimmed = rest.trim_start();
                let close = rest_trimmed
                    .find(')')
                    .ok_or_else(|| syntax("unterminated function signature"))?;
                rest = &rest_trimmed[close + 1..];
            }
            ('f', Some(name))
        } else if let Some(used) = starts_with_words(rest, &["without", "function"]) {
            rest = &rest[used..];
            ('b', None)
        } else if let Some(used) = starts_with_words(rest, &["with", "inout"]) {
            rest = &rest[used..];
            ('i', None)
        } else {
            return Err(syntax(
                "CREATE CAST requires WITH FUNCTION, WITHOUT FUNCTION or WITH INOUT",
            ));
        };
        let context = if starts_with_words(rest, &["as", "assignment"]).is_some() {
            'a'
        } else if starts_with_words(rest, &["as", "implicit"]).is_some() {
            'i'
        } else if rest.trim().is_empty() {
            'e'
        } else {
            return Err(syntax(format!("unexpected text after CREATE CAST: {rest}")));
        };
        if source == target {
            return Err(SqlError::data_exception_public(
                "42P17",
                "source data type and target data type are the same",
                None,
            ));
        }
        if let Some(function) = &function {
            let candidates = routines_named(self.db_ref(), RoutineKind::Function, function)?;
            if candidates.is_empty() {
                return Err(SqlError::undefined_function(format!(
                    "function {function}({source_display}) does not exist"
                )));
            }
        }
        self.require_cast_privilege(&source, &target)?;
        let exists = find_cast(&user_casts(self.db_ref())?, &source, &target).is_some()
            || crate::pg_casts::pg_cast_spec(&source, &target).is_some();
        if exists {
            return Err(SqlError::data_exception_public(
                "42710",
                format!("cast from type {source_display} to type {target_display} already exists"),
                None,
            ));
        }
        let cast = UserCast {
            source,
            target,
            source_display,
            target_display,
            function,
            method,
            context,
            owner: current_user_from_gucs(&self.session_gucs),
        };
        self.record_cast_undo(&cast.source, &cast.target)?;
        save_user_cast(self.db_mut()?, &cast)?;
        Ok(Some(SqlResult::command("CREATE CAST")))
    }

    /// `DROP CAST [IF EXISTS] (source AS target) [CASCADE | RESTRICT]`.
    pub(crate) fn execute_raw_drop_cast(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let text = sql.trim().trim_end_matches(';').trim();
        let Some(mut prefix) = starts_with_words(text, &["drop", "cast"]) else {
            return Ok(None);
        };
        let if_exists = match starts_with_words(&text[prefix..], &["if", "exists"]) {
            Some(used) => {
                prefix += used;
                true
            }
            None => false,
        };
        let (source_text, target_text, rest) = cast_pair(&text[prefix..])?;
        let rest = rest.trim();
        if !(rest.is_empty()
            || rest.eq_ignore_ascii_case("cascade")
            || rest.eq_ignore_ascii_case("restrict"))
        {
            return Err(syntax(format!("unexpected text after DROP CAST: {rest}")));
        }
        let (source, source_display) = type_names(self.db_ref(), &source_text)?;
        let (target, target_display) = type_names(self.db_ref(), &target_text)?;
        let casts = user_casts(self.db_ref())?;
        let Some(cast) = find_cast(&casts, &source, &target).cloned() else {
            if if_exists {
                return Ok(Some(SqlResult::command("DROP CAST")));
            }
            return Err(SqlError::data_exception_public(
                "42704",
                format!("cast from type {source_display} to type {target_display} does not exist"),
                None,
            ));
        };
        if !self.current_user_is_superuser()? && !self.current_user_holds_role(&cast.owner)? {
            self.require_cast_privilege(&source, &target)?;
        }
        self.record_cast_undo(&source, &target)?;
        delete_user_cast(self.db_mut()?, &source, &target)?;
        Ok(Some(SqlResult::command("DROP CAST")))
    }

    fn record_cast_undo(&mut self, source: &str, target: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = find_cast(&user_casts(self.db_ref())?, source, target).cloned();
            self.ddl_undo
                .push(crate::session::DdlUndo::RestoreUserCast {
                    source: source.to_string(),
                    target: target.to_string(),
                    previous: previous.map(Box::new),
                });
        }
        Ok(())
    }
}

/// `pg_cast` rows of the user casts.
pub(crate) fn user_cast_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let type_oid = |pg_type: &str| -> i64 {
        pg_type_oid_by_name(pg_type)
            .map(i64::from)
            .or_else(|| {
                crate::pg_user_type_oid_by_name(db, pg_type)
                    .ok()
                    .flatten()
                    .map(i64::from)
            })
            .unwrap_or(0)
    };
    let mut rows = Vec::new();
    for cast in user_casts(db)?.iter() {
        let function_oid = cast
            .function
            .as_ref()
            .and_then(|name| {
                routines_named(db, RoutineKind::Function, name)
                    .ok()?
                    .first()
                    .map(catalog_routine_oid)
            })
            .unwrap_or(0);
        rows.push(virtual_row([
            (
                "oid",
                SqlValue::Int(stable_name_hash_wide(
                    2_000_000_000,
                    &format!("pg_cast:user:{}", key(&cast.source, &cast.target)),
                    100_000_000,
                )),
            ),
            ("castsource", SqlValue::Int(type_oid(&cast.source))),
            ("casttarget", SqlValue::Int(type_oid(&cast.target))),
            ("castfunc", SqlValue::Int(function_oid)),
            ("castcontext", SqlValue::String(cast.context.to_string())),
            ("castmethod", SqlValue::String(cast.method.to_string())),
        ]));
    }
    Ok(rows)
}

/// The conversion of `expr` (of type `source`) by `cast`.
fn convert(cast: &UserCast, expr: Expr, target: DataType) -> Expr {
    match (&cast.function, cast.method) {
        (Some(function), 'f') => Expr::Function(Function {
            name: ObjectName::from(vec![Ident::new(function.clone())]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))],
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
        }),
        (_, 'i') => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(expr),
                data_type: DataType::Text,
                array: false,
                format: None,
            }),
            data_type: target,
            array: false,
            format: None,
        },
        _ => Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(expr),
            data_type: target,
            array: false,
            format: None,
        },
    }
}

struct CastBinder<'s, 'db> {
    engine: &'s SqlEngine<'db>,
    casts: Arc<Vec<UserCast>>,
    scopes: Vec<Vec<RelationColumns>>,
    changed: bool,
    error: Option<SqlError>,
}

impl CastBinder<'_, '_> {
    fn env(&self) -> Vec<RelationColumns> {
        self.scopes.iter().rev().flatten().cloned().collect()
    }

    fn source_type(&self, expr: &Expr) -> Option<String> {
        self.engine
            .infer_env_expr_type(expr, &self.env())
            .map(|pg_type| crate::pg_operators::canonical_type(&pg_type))
    }

    fn relation_env(&self, from: &[TableWithJoins]) -> Vec<RelationColumns> {
        let mut env = Vec::new();
        for table in from {
            for factor in std::iter::once(&table.relation)
                .chain(table.joins.iter().map(|join| &join.relation))
            {
                let mut columns = Vec::new();
                if self
                    .engine
                    .collect_relation_columns(factor, &mut columns)
                    .is_some()
                {
                    env.extend(columns);
                }
            }
        }
        env
    }

    fn bind_cast(&mut self, expr: &mut Expr) -> Result<()> {
        let Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            data_type,
            array: false,
            format: None,
        } = expr
        else {
            return Ok(());
        };
        let target = canonical_data_type(self.engine.db_ref(), data_type)?;
        let Some(source) = self.source_type(inner) else {
            return Ok(());
        };
        let Some(cast) = find_cast(&self.casts, &source, &target).cloned() else {
            return Ok(());
        };
        if cast.method == 'b' {
            return Ok(());
        }
        let inner = std::mem::replace(inner.as_mut(), Expr::value(sqlparser::ast::Value::Null));
        *expr = convert(&cast, inner, data_type.clone());
        self.changed = true;
        Ok(())
    }

    /// Implicit casts of user function arguments: a call whose only
    /// candidate of that arity declares a type an implicit user cast reaches.
    fn bind_call(&mut self, expr: &mut Expr) -> Result<()> {
        let Expr::Function(function) = expr else {
            return Ok(());
        };
        let FunctionArguments::List(list) = &mut function.args else {
            return Ok(());
        };
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let candidates = routines_named(self.engine.db_ref(), RoutineKind::Function, &name)?;
        let candidates = candidates
            .iter()
            .filter(|routine| {
                parse_routine_params(&routine.args)
                    .map(|params| {
                        params
                            .iter()
                            .filter(|param| param.mode != RoutineArgMode::Out)
                            .count()
                            == list.args.len()
                    })
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        let [candidate] = candidates.as_slice() else {
            return Ok(());
        };
        let declared = candidate
            .arg_types
            .iter()
            .map(|kind| crate::pg_operators::canonical_type(&kind.pg_type))
            .collect::<Vec<_>>();
        let mut converted = Vec::new();
        for (index, arg) in list.args.iter().enumerate() {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = arg else {
                return Ok(());
            };
            let Some(want) = declared.get(index) else {
                return Ok(());
            };
            let Some(have) = self.source_type(arg) else {
                continue;
            };
            if &have == want {
                continue;
            }
            if let Some(cast) = find_cast(&self.casts, &have, want) {
                if cast.context == 'i' {
                    let data_type = crate::pg_operators::parse_sql_data_type(want)?;
                    converted.push((index, cast.clone(), data_type));
                }
            }
        }
        for (index, cast, data_type) in converted {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = &mut list.args[index] {
                let value = std::mem::replace(arg, Expr::value(sqlparser::ast::Value::Null));
                *arg = convert(&cast, value, data_type);
                self.changed = true;
            }
        }
        Ok(())
    }

    /// Assignment casts of INSERT ... VALUES to the column types.
    fn bind_insert(&mut self, insert: &mut sqlparser::ast::Insert) -> Result<()> {
        let sqlparser::ast::TableObject::TableName(name) = &insert.table else {
            return Ok(());
        };
        let table =
            resolve_session_relation_name_if_exists(self.engine.db_ref(), &relation_name(name)?);
        let Some(schema) = load_schema(self.engine.db_ref(), &table)? else {
            return Ok(());
        };
        let columns = if insert.columns.is_empty() {
            schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| column.clone())
                .collect::<Vec<_>>()
        } else {
            insert
                .columns
                .iter()
                .filter_map(|column| {
                    let parts = object_name_parts(column);
                    schema.column(parts.last()?).cloned()
                })
                .collect()
        };
        let Some(source) = insert.source.as_mut() else {
            return Ok(());
        };
        let SetExpr::Values(values) = source.body.as_mut() else {
            return Ok(());
        };
        for row in &mut values.rows {
            for (index, value) in row.iter_mut().enumerate() {
                let Some(column) = columns.get(index) else {
                    continue;
                };
                let want = match &column.user_type {
                    Some(user_type) => user_type.formatted_name(),
                    None => crate::pg_operators::canonical_type(&column.pg_type),
                };
                let Some(have) = self.source_type(value) else {
                    continue;
                };
                if have == want {
                    continue;
                }
                if let Some(cast) = find_cast(&self.casts, &have, &want).cloned() {
                    if matches!(cast.context, 'a' | 'i') && cast.method != 'b' {
                        let data_type = crate::pg_operators::parse_sql_data_type(&want)?;
                        let inner =
                            std::mem::replace(value, Expr::value(sqlparser::ast::Value::Null));
                        *value = convert(&cast, inner, data_type);
                        self.changed = true;
                    }
                }
            }
        }
        Ok(())
    }
}

impl VisitorMut for CastBinder<'_, '_> {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        if let Statement::Insert(insert) = statement {
            if let Err(error) = self.bind_insert(insert) {
                self.error = Some(error);
                return ControlFlow::Break(());
            }
        }
        self.scopes.push(Vec::new());
        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        let env = self.relation_env(&select.from);
        self.scopes.push(env);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &mut Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let result = self.bind_cast(expr).and_then(|()| self.bind_call(expr));
        if let Err(error) = result {
            self.error = Some(error);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Apply the user casts of `statement` (see the module documentation).
pub(crate) fn bind_statement_casts(
    engine: &SqlEngine<'_>,
    statement: &Statement,
) -> Result<Option<Statement>> {
    let casts = user_casts(engine.db_ref())?;
    if casts.is_empty()
        || matches!(
            statement,
            Statement::CreateFunction(_) | Statement::CreateProcedure { .. }
        )
    {
        return Ok(None);
    }
    let mut bound = statement.clone();
    let mut binder = CastBinder {
        engine,
        casts,
        scopes: Vec::new(),
        changed: false,
        error: None,
    };
    let _ = bound.visit(&mut binder);
    if let Some(error) = binder.error {
        return Err(error);
    }
    Ok(binder.changed.then_some(bound))
}
