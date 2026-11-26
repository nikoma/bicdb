//! `CREATE AGGREGATE` / `DROP AGGREGATE` and calls of user-defined
//! aggregates.
//!
//! An aggregate is recorded as a routine of its name and argument types
//! (`prokind = 'a'`, `prosrc = aggregate_dummy`, as PostgreSQL shows it), so
//! name resolution, `regproc`, ownership and EXECUTE privileges are those of
//! any function; its definition keeps the CREATE AGGREGATE statement.
//!
//! A call `agg(x ORDER BY y) FILTER (WHERE c) OVER (...)` is rewritten before
//! execution into `agg('<marker>', array_agg(x ORDER BY y) FILTER (WHERE c)
//! OVER (...))`: the built-in array_agg supplies grouping, ordering,
//! filtering, DISTINCT and window frames exactly as for any aggregate, and
//! the marked call folds the collected inputs through the state transition
//! function, then the final function, with PostgreSQL's strictness rules.

use crate::*;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

/// `prosrc` of an aggregate's routine record.
pub(crate) const AGGREGATE_SYMBOL: &str = "aggregate_dummy";

/// First argument of the rewritten call that folds collected inputs.
pub(crate) const FOLD_MARKER: &str = "bicdb$aggregate$fold";

#[derive(Clone, Debug)]
pub(crate) struct AggregateDef {
    pub(crate) name: String,
    pub(crate) args: Vec<RoutineTypeSchema>,
    pub(crate) sfunc: String,
    pub(crate) stype: RoutineTypeSchema,
    pub(crate) finalfunc: Option<String>,
    pub(crate) combinefunc: Option<String>,
    pub(crate) initcond: Option<String>,
    pub(crate) sortop: Option<String>,
}

pub(crate) fn is_aggregate_routine(routine: &RoutineSchema) -> bool {
    routine.language == "internal" && routine.internal_symbol.as_deref() == Some(AGGREGATE_SYMBOL)
}

fn syntax(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42601", message.into(), None)
}

/// Index of the `)` matching the `(` at `open`, outside quotes.
fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut index = open;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// Top-level comma-separated parts of `text`.
fn split_commas(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let (mut depth, mut start, mut index) = (0usize, 0usize, 0usize);
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(text[start..index].trim().to_string());
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    let last = text[start..].trim();
    if !last.is_empty() {
        parts.push(last.to_string());
    }
    parts
}

fn type_schema(text: &str) -> Result<RoutineTypeSchema> {
    let data_type = crate::pg_operators::parse_sql_data_type(text.trim())?;
    routine_type_schema(&data_type)
}

/// A function name as written (unquoted names fold to lower case).
fn function_name(text: &str) -> String {
    let text = text.trim();
    match text
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        Some(quoted) => quoted.to_string(),
        None => text.to_ascii_lowercase(),
    }
}

/// Argument types of an aggregate signature `(type, name type, ...)`.
fn signature_types(text: &str) -> Result<Vec<RoutineTypeSchema>> {
    let text = text.trim();
    if text == "*" {
        return Err(SqlError::Unsupported(
            "aggregates over * are not supported".into(),
        ));
    }
    if find_top_level_keyword(text, "ORDER").is_some() {
        return Err(SqlError::Unsupported(
            "ordered-set aggregates are not supported".into(),
        ));
    }
    split_commas(text)
        .iter()
        .map(|arg| {
            let words = split_sql_words(arg);
            let words = match words.first() {
                Some(mode) if mode.eq_ignore_ascii_case("in") => &words[1..],
                Some(mode) if mode.eq_ignore_ascii_case("variadic") => {
                    return Err(SqlError::Unsupported(
                        "VARIADIC aggregates are not supported".into(),
                    ));
                }
                _ => &words[..],
            };
            // `name type` or `type` (a type may span several words).
            match type_schema(&words.join(" ")) {
                Ok(schema) => Ok(schema),
                Err(_) if words.len() > 1 => type_schema(&words[1..].join(" ")),
                Err(error) => Err(error),
            }
        })
        .collect()
}

/// `CREATE [OR REPLACE] AGGREGATE name (args) (options)`.
pub(crate) fn parse_create_aggregate(sql: &str) -> Result<Option<(bool, AggregateDef)>> {
    let text = sql.trim().trim_end_matches(';').trim();
    let words = split_sql_words(text);
    let lower = words
        .iter()
        .take(4)
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let (or_replace, keyword_count) = match lower.as_slice() {
        [create, aggregate, ..] if create == "create" && aggregate == "aggregate" => (false, 2),
        [create, or, replace, aggregate, ..]
            if create == "create"
                && or == "or"
                && replace == "replace"
                && aggregate == "aggregate" =>
        {
            (true, 4)
        }
        _ => return Ok(None),
    };
    let mut rest = text;
    for _ in 0..keyword_count {
        rest = rest.trim_start();
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        rest = &rest[end..];
    }
    let rest = rest.trim_start();
    let open = rest
        .find('(')
        .ok_or_else(|| syntax("CREATE AGGREGATE requires an argument list"))?;
    let name = function_name(&rest[..open]);
    let close = matching_paren(rest, open).ok_or_else(|| syntax("unterminated argument list"))?;
    let args = signature_types(&rest[open + 1..close])?;
    let options_text = rest[close + 1..].trim();
    let options_open = options_text
        .find('(')
        .ok_or_else(|| syntax("CREATE AGGREGATE requires its definition options"))?;
    let options_close = matching_paren(options_text, options_open)
        .ok_or_else(|| syntax("unterminated aggregate definition"))?;
    let mut def = AggregateDef {
        name,
        args,
        sfunc: String::new(),
        stype: RoutineTypeSchema {
            pg_type: String::new(),
            type_modifier: None,
        },
        finalfunc: None,
        combinefunc: None,
        initcond: None,
        sortop: None,
    };
    let mut have_stype = false;
    for option in split_commas(&options_text[options_open + 1..options_close]) {
        let (key, value) = option
            .split_once('=')
            .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim()))
            .unwrap_or_else(|| (option.trim().to_ascii_lowercase(), ""));
        let unquoted = || {
            value
                .strip_prefix('\'')
                .and_then(|rest| rest.strip_suffix('\''))
                .map(|inner| inner.replace("''", "'"))
                .unwrap_or_else(|| value.to_string())
        };
        match key.as_str() {
            "sfunc" | "sfunc1" => def.sfunc = function_name(value),
            "stype" | "stype1" => {
                def.stype = type_schema(value)?;
                have_stype = true;
            }
            "finalfunc" => def.finalfunc = Some(function_name(value)),
            "combinefunc" => def.combinefunc = Some(function_name(value)),
            "initcond" | "initcond1" => def.initcond = Some(unquoted()),
            "sortop" => def.sortop = Some(value.to_string()),
            "basetype" => def.args = vec![type_schema(value)?],
            // Planner and storage hints, and the moving-aggregate variants
            // (used by PostgreSQL only as a window-frame optimisation with the
            // same results), do not change what the aggregate computes.
            "sspace" | "finalfunc_modify" | "parallel" | "serialfunc" | "deserialfunc"
            | "msfunc" | "minvfunc" | "mstype" | "msspace" | "mfinalfunc" | "mfinalfunc_modify"
            | "minitcond" => {}
            "finalfunc_extra" | "mfinalfunc_extra" | "hypothetical" => {
                return Err(SqlError::Unsupported(format!(
                    "aggregate option {key} is not supported"
                )));
            }
            other => {
                return Err(syntax(format!(
                    "aggregate attribute \"{other}\" not recognized"
                )));
            }
        }
    }
    if def.sfunc.is_empty() {
        return Err(SqlError::data_exception_public(
            "42P13",
            "aggregate sfunc must be specified",
            None,
        ));
    }
    if !have_stype {
        return Err(SqlError::data_exception_public(
            "42P13",
            "aggregate stype must be specified",
            None,
        ));
    }
    Ok(Some((or_replace, def)))
}

/// `DROP AGGREGATE [IF EXISTS] name (args) [, ...] [CASCADE | RESTRICT]`.
pub(crate) fn parse_drop_aggregate(
    sql: &str,
) -> Result<Option<(bool, Vec<(String, Vec<RoutineTypeSchema>)>)>> {
    let text = sql.trim().trim_end_matches(';').trim();
    let words = split_sql_words(text);
    if !(words.len() >= 3
        && words[0].eq_ignore_ascii_case("drop")
        && words[1].eq_ignore_ascii_case("aggregate"))
    {
        return Ok(None);
    }
    let mut rest = text["drop".len()..].trim_start();
    rest = rest["aggregate".len()..].trim_start();
    let mut if_exists = false;
    if rest.to_ascii_lowercase().starts_with("if exists") {
        if_exists = true;
        rest = rest["if exists".len()..].trim_start();
    }
    let lower = rest.to_ascii_lowercase();
    let rest = if lower.ends_with(" cascade") || lower.ends_with(" restrict") {
        rest[..rest.rfind(' ').expect("space found")].trim_end()
    } else {
        rest
    };
    let mut targets = Vec::new();
    for item in split_commas(rest) {
        let open = item
            .find('(')
            .ok_or_else(|| syntax("DROP AGGREGATE requires an argument list"))?;
        let close =
            matching_paren(&item, open).ok_or_else(|| syntax("unterminated argument list"))?;
        targets.push((
            function_name(&item[..open]),
            signature_types(&item[open + 1..close])?,
        ));
    }
    Ok(Some((if_exists, targets)))
}

fn type_display(schema: &RoutineTypeSchema) -> String {
    common_type_display(&schema.pg_type)
}

/// The aggregate definition a routine record keeps.
pub(crate) fn aggregate_def(routine: &RoutineSchema) -> Result<AggregateDef> {
    parse_create_aggregate(&routine.definition)?
        .map(|(_, def)| def)
        .ok_or_else(|| {
            SqlError::InvalidSql(format!("aggregate {} has no definition", routine.name))
        })
}

/// The aggregate called `name` with `arity` arguments.
pub(crate) fn find_aggregate(
    db: &BicDb,
    name: &str,
    arity: usize,
) -> Result<Option<(RoutineSchema, AggregateDef)>> {
    let mut found = None;
    for routine in routines_named(db, RoutineKind::Function, name)? {
        if !is_aggregate_routine(&routine) {
            continue;
        }
        let def = aggregate_def(&routine)?;
        if def.args.len() != arity {
            continue;
        }
        if found.is_some() {
            return Err(SqlError::Unsupported(format!(
                "aggregate {name} is overloaded with {arity} argument(s)"
            )));
        }
        found = Some((routine, def));
    }
    Ok(found)
}

/// The routine record that registers `def`.
pub(crate) fn aggregate_routine(
    def: &AggregateDef,
    definition: &str,
    return_type: RoutineTypeSchema,
    owner: String,
    schema: String,
) -> RoutineSchema {
    RoutineSchema {
        oracle_invalid: None,
        name: def.name.clone(),
        schema,
        kind: RoutineKind::Function,
        args: def.args.iter().map(|arg| arg.formatted()).collect(),
        arg_types: def.args.clone(),
        return_type: return_type.pg_type.clone(),
        return_type_modifier: return_type.type_modifier.clone(),
        return_type_declaration: Some(return_type.formatted()),
        returns_set: false,
        language: "internal".to_string(),
        definition: definition.trim().trim_end_matches(';').trim().to_string(),
        internal_symbol: Some(AGGREGATE_SYMBOL.to_string()),
        owner: Some(owner),
        security_definer: false,
        overload: None,
        catalog_oid: None,
    }
}

pub(crate) fn signature_text(name: &str, args: &[RoutineTypeSchema]) -> String {
    format!(
        "{name}({})",
        args.iter().map(type_display).collect::<Vec<_>>().join(", ")
    )
}

/// Whether a function is declared STRICT (RETURNS NULL ON NULL INPUT).
pub(crate) fn routine_is_strict(routine: &RoutineSchema) -> bool {
    use sqlparser::ast::FunctionCalledOnNull;
    match crate::table_queries::parse(&routine.definition) {
        Ok(statements) => statements.iter().any(|statement| {
            matches!(statement, Statement::CreateFunction(create)
                if matches!(create.called_on_null,
                    Some(FunctionCalledOnNull::Strict | FunctionCalledOnNull::ReturnsNullOnNullInput)))
        }),
        Err(_) => false,
    }
}

/// `pg_aggregate` rows.
pub(crate) fn pg_aggregate_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = Vec::new();
    for routine in list_routines(db)? {
        if !is_aggregate_routine(&routine) {
            continue;
        }
        let def = aggregate_def(&routine)?;
        let function_oid = |name: &Option<String>| -> SqlValue {
            name.as_ref()
                .and_then(|name| {
                    routines_named(db, RoutineKind::Function, name)
                        .ok()?
                        .first()
                        .map(catalog_routine_oid)
                })
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Int(0))
        };
        rows.push(virtual_row([
            ("aggfnoid", SqlValue::Int(catalog_routine_oid(&routine))),
            ("aggkind", SqlValue::String("n".into())),
            ("aggnumdirectargs", SqlValue::Int(0)),
            ("aggtransfn", function_oid(&Some(def.sfunc.clone()))),
            ("aggfinalfn", function_oid(&def.finalfunc)),
            ("aggcombinefn", function_oid(&def.combinefunc)),
            ("aggserialfn", SqlValue::Int(0)),
            ("aggdeserialfn", SqlValue::Int(0)),
            ("aggmtransfn", SqlValue::Int(0)),
            ("aggminvtransfn", SqlValue::Int(0)),
            ("aggmfinalfn", SqlValue::Int(0)),
            ("aggfinalextra", SqlValue::Bool(false)),
            ("aggmfinalextra", SqlValue::Bool(false)),
            ("aggfinalmodify", SqlValue::String("r".into())),
            ("aggmfinalmodify", SqlValue::String("r".into())),
            ("aggsortop", SqlValue::Int(0)),
            (
                "aggtranstype",
                SqlValue::Int(pg_type_oid(&def.stype.pg_type)),
            ),
            ("aggtransspace", SqlValue::Int(0)),
            ("aggmtranstype", SqlValue::Int(0)),
            ("aggmtransspace", SqlValue::Int(0)),
            (
                "agginitval",
                def.initcond
                    .clone()
                    .map(SqlValue::String)
                    .unwrap_or(SqlValue::Null),
            ),
            ("aggminitval", SqlValue::Null),
        ]));
    }
    Ok(rows)
}

pub(crate) const PG_AGGREGATE_COLUMNS: [(&str, &str); 22] = [
    ("aggfnoid", "regproc"),
    ("aggkind", "char"),
    ("aggnumdirectargs", "int2"),
    ("aggtransfn", "regproc"),
    ("aggfinalfn", "regproc"),
    ("aggcombinefn", "regproc"),
    ("aggserialfn", "regproc"),
    ("aggdeserialfn", "regproc"),
    ("aggmtransfn", "regproc"),
    ("aggminvtransfn", "regproc"),
    ("aggmfinalfn", "regproc"),
    ("aggfinalextra", "bool"),
    ("aggmfinalextra", "bool"),
    ("aggfinalmodify", "char"),
    ("aggmfinalmodify", "char"),
    ("aggsortop", "oid"),
    ("aggtranstype", "oid"),
    ("aggtransspace", "int4"),
    ("aggmtranstype", "oid"),
    ("aggmtransspace", "int4"),
    ("agginitval", "text"),
    ("aggminitval", "text"),
];

/// Rewrite a call of a user aggregate into its folding form (see the module
/// documentation). `None` when `function` is not such a call.
pub(crate) fn rewrite_aggregate_call(db: &BicDb, function: &Function) -> Result<Option<Expr>> {
    let FunctionArguments::List(list) = &function.args else {
        return Ok(None);
    };
    if list
        .args
        .first()
        .is_some_and(|arg| matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) if matches!(&value.value, sqlparser::ast::Value::SingleQuotedString(text) if text == FOLD_MARKER)))
    {
        return Ok(None);
    }
    let name = object_name(&function.name)?.to_ascii_lowercase();
    if find_aggregate(db, &name, list.args.len())?.is_none() {
        return Ok(None);
    }
    if !function.within_group.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "{name} is not an ordered-set aggregate, so WITHIN GROUP is not allowed"
        )));
    }
    let mut args = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
        sqlparser::ast::Value::SingleQuotedString(FOLD_MARKER.to_string()),
    )))];
    for arg in &list.args {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return Err(SqlError::Unsupported(format!(
                "aggregate {name} takes positional arguments"
            )));
        };
        let collected = Function {
            name: ObjectName::from(vec![Ident::new("array_agg")]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                duplicate_treatment: list.duplicate_treatment,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(expr.clone()))],
                clauses: list.clauses.clone(),
            }),
            filter: function.filter.clone(),
            null_treatment: None,
            over: function.over.clone(),
            within_group: Vec::new(),
        };
        args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
            collected,
        ))));
    }
    let fold = Function {
        name: function.name.clone(),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    };
    Ok(Some(Expr::Function(fold)))
}

thread_local! {
    static AGGREGATE_NAMES: std::cell::RefCell<Option<(u64, u64, Arc<rustc_hash::FxHashSet<String>>)>> =
        const { std::cell::RefCell::new(None) };
}

/// Names of the user-defined aggregates, per database and routine catalog
/// generation.
pub(crate) fn aggregate_names(db: &BicDb) -> Result<Arc<rustc_hash::FxHashSet<String>>> {
    let database_id = db.instance_id();
    let generation = db.collection_generation(ROUTINE_COLLECTION);
    if let Some(names) = AGGREGATE_NAMES.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, names)| {
            (*id == database_id && *cached == generation).then(|| names.clone())
        })
    }) {
        return Ok(names);
    }
    let names: rustc_hash::FxHashSet<String> = list_routines(db)?
        .into_iter()
        .filter(is_aggregate_routine)
        .map(|routine| routine.name.to_ascii_lowercase())
        .collect();
    let names = Arc::new(names);
    AGGREGATE_NAMES
        .with(|cache| *cache.borrow_mut() = Some((database_id, generation, names.clone())));
    Ok(names)
}

/// The error for an aggregate reached without its folding rewrite (a call
/// form this implementation does not bind), never a silent wrong result.
pub(crate) fn unbound_aggregate_call(name: &str) -> SqlError {
    SqlError::Unsupported(format!(
        "user-defined aggregate {name} cannot be called in this context"
    ))
}

/// Whether evaluated call arguments are the folding form of an aggregate.
pub(crate) fn is_fold_call(args: &[SqlValue]) -> bool {
    matches!(args.first(), Some(SqlValue::String(marker)) if marker == FOLD_MARKER)
}

/// The values one input array of the folding call holds.
fn collected_values(value: &SqlValue) -> Result<Vec<SqlValue>> {
    match value {
        SqlValue::Null => Ok(Vec::new()),
        other => match array_json_parts(other, "aggregate input")? {
            Some((array, _)) => Ok(array
                .as_array()
                .map(|values| values.iter().map(json_to_sql_value).collect())
                .unwrap_or_default()),
            None => Ok(Vec::new()),
        },
    }
}

/// What folding one aggregate call needs from the catalog.
pub(crate) struct FoldPlan {
    def: AggregateDef,
    sfunc_strict: bool,
    final_strict: bool,
}

pub(crate) fn fold_plan(db: &BicDb, name: &str, args: &[SqlValue]) -> Result<FoldPlan> {
    let arity = args.len().saturating_sub(1);
    let (_, def) = find_aggregate(db, name, arity)?
        .ok_or_else(|| SqlError::undefined_function(format!("aggregate {name} does not exist")))?;
    let strict = |function: &str| -> Result<bool> {
        Ok(routines_named(db, RoutineKind::Function, function)?
            .first()
            .map(routine_is_strict)
            // Built-in transition functions are strict.
            .unwrap_or(true))
    };
    let sfunc_strict = strict(&def.sfunc)?;
    let final_strict = match &def.finalfunc {
        Some(finalfunc) => strict(finalfunc)?,
        None => false,
    };
    Ok(FoldPlan {
        def,
        sfunc_strict,
        final_strict,
    })
}

/// Fold the collected inputs (`args[1..]`, one array per aggregate argument)
/// through the transition and final functions. `call(name, args, types)`
/// runs a user function, `None` when none of that name takes the arguments.
pub(crate) fn run_fold(
    plan: &FoldPlan,
    args: &[SqlValue],
    call: &mut dyn FnMut(&str, &[SqlValue], &[Option<String>]) -> Result<Option<SqlValue>>,
) -> Result<SqlValue> {
    let def = &plan.def;
    let inputs = args[1..]
        .iter()
        .map(collected_values)
        .collect::<Result<Vec<_>>>()?;
    let rows = inputs.first().map_or(0, Vec::len);
    let stype = def.stype.pg_type.clone();
    let mut state = match &def.initcond {
        Some(text) => cast_value_to_pg_type(SqlValue::String(text.clone()), &stype)?,
        None => SqlValue::Null,
    };
    let mut types = vec![Some(stype.clone())];
    types.extend(def.args.iter().map(|arg| Some(arg.pg_type.clone())));
    for row in 0..rows {
        let values = inputs
            .iter()
            .zip(&def.args)
            .map(|(column, kind)| {
                let value = column.get(row).cloned().unwrap_or(SqlValue::Null);
                if matches!(value, SqlValue::Null) {
                    Ok(value)
                } else {
                    cast_value_to_pg_type(value, &kind.pg_type)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if plan.sfunc_strict {
            // A strict transition skips rows with a NULL input; with no
            // initial condition the first such row's input is the state.
            if values.iter().any(|value| matches!(value, SqlValue::Null)) {
                continue;
            }
            if matches!(state, SqlValue::Null) {
                if def.initcond.is_none() {
                    state = cast_value_to_pg_type(values[0].clone(), &stype)?;
                }
                continue;
            }
        }
        let mut call_args = Vec::with_capacity(values.len() + 1);
        call_args.push(state);
        call_args.extend(values);
        state = invoke(&def.sfunc, &call_args, &types, call)?;
    }
    if let Some(finalfunc) = &def.finalfunc {
        if plan.final_strict && matches!(state, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        return invoke(finalfunc, &[state], &[Some(stype)], call);
    }
    Ok(state)
}

fn invoke(
    name: &str,
    args: &[SqlValue],
    types: &[Option<String>],
    call: &mut dyn FnMut(&str, &[SqlValue], &[Option<String>]) -> Result<Option<SqlValue>>,
) -> Result<SqlValue> {
    if let Some(value) = call(name, args, types)? {
        return Ok(value);
    }
    // The built-in transition functions aggregates commonly name.
    use sqlparser::ast::BinaryOperator;
    let op = match name.strip_prefix("pg_catalog.").unwrap_or(name) {
        "int2pl" | "int4pl" | "int8pl" | "float4pl" | "float8pl" | "numeric_add" | "int48pl"
        | "int84pl" | "int24pl" | "int42pl" => BinaryOperator::Plus,
        "int2mul" | "int4mul" | "int8mul" | "float4mul" | "float8mul" | "numeric_mul" => {
            BinaryOperator::Multiply
        }
        "textcat" => BinaryOperator::StringConcat,
        "int4larger" | "int8larger" | "float8larger" | "numeric_larger" | "text_larger" => {
            return Ok(match args {
                [left, right]
                    if compare_values(left, &BinaryOperator::Lt, right)? == Some(true) =>
                {
                    right.clone()
                }
                [left, _] => left.clone(),
                _ => SqlValue::Null,
            });
        }
        "int4smaller" | "int8smaller" | "float8smaller" | "numeric_smaller" | "text_smaller" => {
            return Ok(match args {
                [left, right]
                    if compare_values(left, &BinaryOperator::Gt, right)? == Some(true) =>
                {
                    right.clone()
                }
                [left, _] => left.clone(),
                _ => SqlValue::Null,
            });
        }
        _ => {
            return Err(SqlError::undefined_function(format!(
                "function {name} does not exist"
            )))
        }
    };
    match args {
        [left, right] => eval_arithmetic_value(left.clone(), &op, right.clone()),
        _ => Err(SqlError::undefined_function(format!(
            "function {name} does not exist"
        ))),
    }
}

impl SqlSession<'_> {
    /// `CREATE [OR REPLACE] AGGREGATE`.
    pub(crate) fn execute_raw_create_aggregate(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((or_replace, def)) = parse_create_aggregate(sql)? else {
            return Ok(None);
        };
        let exists = |name: &str| -> Result<bool> {
            Ok(
                !routines_named(self.db_ref(), RoutineKind::Function, name)?.is_empty()
                    || crate::builtin_signatures::PG_CATALOG_FUNCTION_SIGNATURES
                        .iter()
                        .any(|(candidate, _)| *candidate == name),
            )
        };
        if !exists(&def.sfunc)? {
            return Err(SqlError::undefined_function(format!(
                "function {} does not exist",
                signature_text(
                    &def.sfunc,
                    &std::iter::once(def.stype.clone())
                        .chain(def.args.iter().cloned())
                        .collect::<Vec<_>>()
                )
            )));
        }
        for function in def.finalfunc.iter().chain(def.combinefunc.iter()) {
            if !exists(function)? {
                return Err(SqlError::undefined_function(format!(
                    "function {}({}) does not exist",
                    function,
                    type_display(&def.stype)
                )));
            }
        }
        let return_type = match &def.finalfunc {
            Some(finalfunc) => routines_named(self.db_ref(), RoutineKind::Function, finalfunc)?
                .first()
                .map(|routine| RoutineTypeSchema {
                    pg_type: routine.return_type.clone(),
                    type_modifier: routine.return_type_modifier.clone(),
                })
                .unwrap_or_else(|| def.stype.clone()),
            None => def.stype.clone(),
        };
        let signature = def
            .args
            .iter()
            .map(|arg| arg.pg_type.clone())
            .collect::<Vec<_>>();
        let existing = routines_named(self.db_ref(), RoutineKind::Function, &def.name)?
            .into_iter()
            .find(|routine| {
                routine
                    .arg_types
                    .iter()
                    .map(|arg| arg.pg_type.clone())
                    .collect::<Vec<_>>()
                    == signature
            });
        if let Some(existing) = &existing {
            if !or_replace || !is_aggregate_routine(existing) {
                return Err(SqlError::data_exception_public(
                    "42723",
                    format!(
                        "function \"{}\" already exists with same argument types",
                        def.name
                    ),
                    None,
                ));
            }
        }
        let (schema, _) = routine_creation_schema_and_name(&def.name);
        let routine = aggregate_routine(
            &def,
            sql,
            return_type,
            current_user_from_gucs(&self.session_gucs),
            schema,
        );
        self.save_session_routine_if_missing(routine, false, or_replace)?;
        Ok(Some(SqlResult::command("CREATE AGGREGATE")))
    }

    /// `DROP AGGREGATE`: the owner (or a superuser) drops the aggregate's
    /// routine record.
    pub(crate) fn execute_raw_drop_aggregate(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((if_exists, targets)) = parse_drop_aggregate(sql)? else {
            return Ok(None);
        };
        let mut found = Vec::new();
        for (name, args) in &targets {
            let signature = args
                .iter()
                .map(|arg| arg.pg_type.clone())
                .collect::<Vec<_>>();
            let routine = routines_named(self.db_ref(), RoutineKind::Function, name)?
                .into_iter()
                .find(|routine| {
                    is_aggregate_routine(routine)
                        && routine
                            .arg_types
                            .iter()
                            .map(|arg| arg.pg_type.clone())
                            .collect::<Vec<_>>()
                            == signature
                });
            match routine {
                Some(routine) => found.push(routine),
                None if if_exists => {}
                None => {
                    return Err(SqlError::undefined_function(format!(
                        "aggregate {} does not exist",
                        signature_text(name, args)
                    )));
                }
            }
        }
        for routine in found {
            if !self.current_user_is_superuser()?
                && !self.current_user_holds_role(routine.owner())?
            {
                return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                    "must be owner of aggregate {}",
                    routine.name
                ))));
            }
            self.delete_session_routine(&routine)?;
        }
        Ok(Some(SqlResult::command("DROP AGGREGATE")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_drop_aggregate_parse() {
        let (replace, def) = parse_create_aggregate(
            "CREATE AGGREGATE sumsq(int) (sfunc = add_sq, stype = int, initcond = '0', finalfunc = fin)",
        )
        .unwrap()
        .unwrap();
        assert!(!replace);
        assert_eq!(def.name, "sumsq");
        assert_eq!(def.args.len(), 1);
        assert_eq!(def.args[0].pg_type, "int4");
        assert_eq!(def.sfunc, "add_sq");
        assert_eq!(def.stype.pg_type, "int4");
        assert_eq!(def.initcond.as_deref(), Some("0"));
        assert_eq!(def.finalfunc.as_deref(), Some("fin"));
        let (_, def) = parse_create_aggregate(
            "create or replace aggregate a2(x float8, y text) (SFUNC = f, STYPE = float8[], INITCOND = '{0,0}')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(def.args.len(), 2);
        assert_eq!(def.stype.pg_type, "float8[]");
        assert_eq!(def.initcond.as_deref(), Some("{0,0}"));
        let (if_exists, targets) =
            parse_drop_aggregate("DROP AGGREGATE IF EXISTS sumsq(int), a2(float8, text) CASCADE")
                .unwrap()
                .unwrap();
        assert!(if_exists);
        assert_eq!(targets.len(), 2);
        assert!(
            parse_create_aggregate("CREATE AGGREGATE x(int) (stype = int)")
                .unwrap_err()
                .sqlstate()
                .eq("42P13")
        );
    }
}
