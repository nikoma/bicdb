//! Call forms of user-defined functions that PostgreSQL resolves before
//! execution: named (`f(b => 2, a => 1)`, `f(a := 1)`) and mixed notation,
//! defaults filled around named arguments, and `VARIADIC` parameters (both
//! the spread form `f(1, 2, 3)` and `f(VARIADIC ARRAY[1, 2, 3])`).
//!
//! Each call is rewritten to the positional, fully supplied form the
//! executors already run: named arguments move to their parameter's
//! position, omitted parameters get their declared default expression (as
//! PostgreSQL inserts them at planning time) and variadic arguments are
//! collected into one array. Overload resolution then proceeds on the
//! positional call exactly as before.

use crate::*;
use sqlparser::ast::{
    Array, Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments, VisitMut,
    VisitorMut,
};
use std::ops::ControlFlow;

/// The parameter name the parse-compat rewrite gives a `VARIADIC` call
/// argument (`f(VARIADIC x)` reaches the parser as `f("…" => x)`).
pub(crate) const VARIADIC_ARGUMENT_MARKER: &str = "bicdb$variadic$argument";

/// Whether a stored routine argument declaration is `VARIADIC`.
pub(crate) fn routine_arg_is_variadic(arg: &str) -> bool {
    arg.trim_start()
        .split_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("variadic"))
}

/// The declaration without its leading `VARIADIC` keyword.
pub(crate) fn routine_arg_without_variadic(arg: &str) -> &str {
    let trimmed = arg.trim_start();
    if routine_arg_is_variadic(trimmed) {
        trimmed["variadic".len()..].trim_start()
    } else {
        arg
    }
}

/// `f(a := 1)` is PostgreSQL's older spelling of `f(a => 1)`; sqlparser
/// knows only the latter in this dialect. Rewrites `:=` inside parentheses
/// (outside strings, comments and dollar quotes); a PL/pgSQL assignment
/// `x := ...` is never parenthesized.
pub(crate) fn rewrite_assignment_named_args(sql: &str) -> Option<String> {
    if !sql.contains(":=") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut edits = Vec::new();
    let mut depth = 0usize;
    let mut index = 0usize;
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
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index += 1;
            }
            b'$' => {
                if let Some(delimiter) = crate::dollar_quote_delimiter(&sql[index..]) {
                    let body = index + delimiter.len();
                    match sql[body..].find(&delimiter) {
                        Some(end) => index = body + end + delimiter.len() - 1,
                        None => return None,
                    }
                }
            }
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b':' if depth > 0
                && bytes.get(index + 1) == Some(&b'=')
                && follows_bare_argument_name(bytes, index) =>
            {
                edits.push(index);
                index += 2;
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    if edits.is_empty() {
        return None;
    }
    let mut output = sql.to_string();
    for at in edits.into_iter().rev() {
        output.replace_range(at..at + 2, "=>");
    }
    Some(output)
}

/// Whether the text before `at` is `( name` or `, name`: an argument name,
/// not a declaration such as `(p NUMBER := 1)`.
fn follows_bare_argument_name(bytes: &[u8], at: usize) -> bool {
    let mut index = at;
    while index > 0 && bytes[index - 1].is_ascii_whitespace() {
        index -= 1;
    }
    let end = index;
    while index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_') {
        index -= 1;
    }
    if index == end {
        return false;
    }
    while index > 0 && bytes[index - 1].is_ascii_whitespace() {
        index -= 1;
    }
    index > 0 && matches!(bytes[index - 1], b'(' | b',')
}

struct InputParam {
    name: Option<String>,
    default_expr: Option<Expr>,
    variadic: bool,
}

fn candidate_inputs(routine: &RoutineSchema) -> Option<Vec<InputParam>> {
    let params = parse_routine_params(&routine.args).ok()?;
    Some(
        params
            .into_iter()
            .zip(&routine.args)
            .filter(|(param, _)| param.mode != RoutineArgMode::Out)
            .map(|(param, raw)| InputParam {
                name: param.name.as_deref().map(normalize_object_name),
                default_expr: param.default_expr,
                variadic: routine_arg_is_variadic(raw),
            })
            .collect(),
    )
}

enum CallArg {
    Positional(Expr),
    Named(String, Expr),
    Variadic(Expr),
}

/// The positional argument list `inputs` receives for `args`, or `None`
/// when the routine cannot take this call.
fn positional_arguments(inputs: &[InputParam], args: &[CallArg]) -> Option<Vec<Expr>> {
    let variadic_index = inputs
        .last()
        .filter(|param| param.variadic)
        .map(|_| inputs.len() - 1);
    let positional = args
        .iter()
        .filter(|arg| matches!(arg, CallArg::Positional(_)))
        .count();
    let explicit_variadic = args.iter().any(|arg| matches!(arg, CallArg::Variadic(_)));
    // As in PostgreSQL, named notation reaches a variadic parameter only
    // through `VARIADIC name => array`, which is not supported here.
    if variadic_index.is_some_and(|index| {
        args.iter().any(|arg| {
            matches!(arg, CallArg::Named(name, _) if inputs[index].name.as_deref() == Some(name))
        })
    }) {
        return None;
    }
    let named_variadic = false;
    let mut slots: Vec<Option<Expr>> = vec![None; inputs.len()];
    let mut next = 0usize;
    let mut spread = Vec::new();
    for arg in args {
        match arg {
            CallArg::Positional(expr) => {
                let spreading = variadic_index
                    .is_some_and(|index| next >= index && !explicit_variadic && !named_variadic);
                if spreading {
                    spread.push(expr.clone());
                    continue;
                }
                let slot = slots.get_mut(next)?;
                *slot = Some(expr.clone());
                next += 1;
            }
            CallArg::Variadic(expr) => {
                let index = variadic_index?;
                if next != index {
                    return None;
                }
                slots[index] = Some(expr.clone());
                next += 1;
            }
            CallArg::Named(name, expr) => {
                let index = inputs
                    .iter()
                    .position(|param| param.name.as_deref() == Some(name.as_str()))?;
                if slots[index].is_some() || index < positional.min(next) {
                    return None;
                }
                slots[index] = Some(expr.clone());
            }
        }
    }
    if let Some(index) = variadic_index {
        if !spread.is_empty() {
            slots[index] = Some(Expr::Array(Array {
                elem: spread,
                named: true,
            }));
        }
    }
    slots
        .into_iter()
        .zip(inputs)
        .map(|(slot, param)| slot.or_else(|| param.default_expr.clone()))
        .collect()
}

struct CallBinder<'db> {
    db: &'db BicDb,
    names: Arc<rustc_hash::FxHashSet<String>>,
    strict: bool,
    changed: bool,
    error: Option<SqlError>,
}

impl CallBinder<'_> {
    fn bind(&mut self, function: &mut Function) -> Result<()> {
        let FunctionArguments::List(list) = &function.args else {
            return Ok(());
        };
        if list.duplicate_treatment.is_some()
            || !list.clauses.is_empty()
            || function.over.is_some()
            || function.filter.is_some()
            || !function.within_group.is_empty()
        {
            return Ok(());
        }
        let name = object_name(&function.name)?.to_ascii_lowercase();
        if !self.names.contains(&name) {
            return Ok(());
        }
        let mut args = Vec::with_capacity(list.args.len());
        let mut named_seen = false;
        for arg in &list.args {
            let call_arg = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    if named_seen {
                        return Err(SqlError::data_exception_public(
                            "42601",
                            "positional argument cannot follow named argument",
                            None,
                        ));
                    }
                    CallArg::Positional(expr.clone())
                }
                FunctionArg::Named {
                    name: arg_name,
                    arg: FunctionArgExpr::Expr(expr),
                    operator,
                }
                | FunctionArg::ExprNamed {
                    name: Expr::Identifier(arg_name),
                    arg: FunctionArgExpr::Expr(expr),
                    operator,
                } if matches!(
                    operator,
                    FunctionArgOperator::RightArrow | FunctionArgOperator::Assignment
                ) =>
                {
                    if arg_name.value == VARIADIC_ARGUMENT_MARKER {
                        CallArg::Variadic(expr.clone())
                    } else {
                        named_seen = true;
                        let key = if arg_name.quote_style.is_some() {
                            arg_name.value.clone()
                        } else {
                            arg_name.value.to_ascii_lowercase()
                        };
                        if args
                            .iter()
                            .any(|seen| matches!(seen, CallArg::Named(other, _) if *other == key))
                        {
                            return Err(SqlError::data_exception_public(
                                "42601",
                                format!("argument name \"{key}\" used more than once"),
                                None,
                            ));
                        }
                        CallArg::Named(key, expr.clone())
                    }
                }
                _ => return Ok(()),
            };
            args.push(call_arg);
        }
        let special = args
            .iter()
            .any(|arg| !matches!(arg, CallArg::Positional(_)));
        let candidates = routines_named(self.db, RoutineKind::Function, &name)?
            .into_iter()
            .filter(|routine| routine.kind == RoutineKind::Function)
            .filter_map(|routine| candidate_inputs(&routine))
            .collect::<Vec<_>>();
        let any_variadic = candidates
            .iter()
            .any(|inputs| inputs.last().is_some_and(|param| param.variadic));
        if !special && !any_variadic {
            return Ok(());
        }
        if !special {
            // A plain positional call: a non-variadic overload that takes
            // this many arguments is preferred over spreading into a
            // variadic one, as PostgreSQL prefers it.
            let count = args.len();
            if candidates.iter().any(|inputs| {
                !inputs.last().is_some_and(|param| param.variadic)
                    && count <= inputs.len()
                    && inputs[count..]
                        .iter()
                        .all(|param| param.default_expr.is_some())
            }) {
                return Ok(());
            }
        }
        let mut chosen: Option<Vec<Expr>> = None;
        let mut ambiguous = false;
        for inputs in &candidates {
            let Some(positional) = positional_arguments(inputs, &args) else {
                continue;
            };
            match &chosen {
                None => chosen = Some(positional),
                Some(existing) if *existing == positional => {}
                Some(_) => ambiguous = true,
            }
        }
        if ambiguous {
            return Err(SqlError::Unsupported(format!(
                "named-argument call of overloaded function {name} whose overloads place the arguments differently"
            )));
        }
        let Some(positional) = chosen else {
            if !special {
                return Ok(());
            }
            return Err(SqlError::undefined_function(format!(
                "function {}({}) does not exist",
                function.name,
                args.iter()
                    .map(|arg| match arg {
                        CallArg::Positional(expr) => call_arg_type(expr),
                        CallArg::Named(name, expr) => format!("{name} => {}", call_arg_type(expr)),
                        CallArg::Variadic(expr) => format!("VARIADIC {}", call_arg_type(expr)),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };
        let FunctionArguments::List(list) = &mut function.args else {
            unreachable!("argument list checked above");
        };
        list.args = positional
            .into_iter()
            .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
            .collect();
        self.changed = true;
        Ok(())
    }
}

/// The type PostgreSQL names for an argument in "function does not exist".
fn call_arg_type(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::Number(number, _) => {
                if number.contains(['.', 'e', 'E']) {
                    "numeric"
                } else if number.parse::<i32>().is_ok() {
                    "integer"
                } else if number.parse::<i64>().is_ok() {
                    "bigint"
                } else {
                    "numeric"
                }
            }
            sqlparser::ast::Value::Boolean(_) => "boolean",
            _ => "unknown",
        }
        .to_string(),
        Expr::Cast { data_type, .. } => data_type.to_string().to_ascii_lowercase(),
        Expr::Nested(inner) => call_arg_type(inner),
        Expr::Array(array) => match array.elem.first().map(call_arg_type) {
            Some(element) if element != "unknown" => format!("{element}[]"),
            _ => "text[]".to_string(),
        },
        _ => "unknown".to_string(),
    }
}

impl VisitorMut for CallBinder<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Function(function) = expr {
            if let Err(error) = self.bind(function) {
                if self.strict {
                    self.error = Some(error);
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }
}

thread_local! {
    /// Names of the user functions with a VARIADIC parameter, per database
    /// and routine catalog generation.
    static VARIADIC_NAMES: std::cell::RefCell<Option<(u64, u64, Arc<rustc_hash::FxHashSet<String>>)>> =
        const { std::cell::RefCell::new(None) };
}

fn variadic_function_names(db: &BicDb) -> Result<Arc<rustc_hash::FxHashSet<String>>> {
    let database_id = db.instance_id();
    let generation = db.collection_generation(ROUTINE_COLLECTION);
    if let Some(names) = VARIADIC_NAMES.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, names)| {
            (*id == database_id && *cached == generation).then(|| names.clone())
        })
    }) {
        return Ok(names);
    }
    let names: rustc_hash::FxHashSet<String> = list_routines(db)?
        .into_iter()
        .filter(|routine| {
            routine.kind == RoutineKind::Function
                && routine.args.iter().any(|arg| routine_arg_is_variadic(arg))
        })
        .map(|routine| routine.name.to_ascii_lowercase())
        .collect();
    let names = Arc::new(names);
    VARIADIC_NAMES
        .with(|cache| *cache.borrow_mut() = Some((database_id, generation, names.clone())));
    Ok(names)
}

/// Whether `node` calls a user function by name with a named or VARIADIC
/// argument, or calls a variadic user function at all.
fn mentions_call_form(
    node: &impl sqlparser::ast::Visit,
    names: &rustc_hash::FxHashSet<String>,
    variadic: &rustc_hash::FxHashSet<String>,
) -> bool {
    use sqlparser::ast::Visitor;
    struct Probe<'a> {
        names: &'a rustc_hash::FxHashSet<String>,
        variadic: &'a rustc_hash::FxHashSet<String>,
    }
    impl Visitor for Probe<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            let FunctionArguments::List(list) = &function.args else {
                return ControlFlow::Continue(());
            };
            let Ok(name) = object_name(&function.name) else {
                return ControlFlow::Continue(());
            };
            let name = name.to_ascii_lowercase();
            if self.variadic.contains(&name)
                || (self.names.contains(&name)
                    && list.args.iter().any(|arg| {
                        matches!(
                            arg,
                            FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. }
                        )
                    }))
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    node.visit(&mut Probe { names, variadic }).is_break()
}

fn binder(db: &BicDb, strict: bool) -> Result<Option<CallBinder<'_>>> {
    if oracle::is_oracle_database(db)? {
        return Ok(None);
    }
    let names = user_function_names(db)?;
    if names.is_empty() {
        return Ok(None);
    }
    Ok(Some(CallBinder {
        db,
        names,
        strict,
        changed: false,
        error: None,
    }))
}

/// Rewrite the named, defaulted and variadic user-function calls of
/// `statement`: `Some` with the rewritten statement when any call changed.
pub(crate) fn bind_statement_calls(db: &BicDb, statement: &Statement) -> Result<Option<Statement>> {
    if matches!(
        statement,
        Statement::CreateFunction(_) | Statement::CreateProcedure { .. }
    ) {
        return Ok(None);
    }
    let Some(mut binder) = binder(db, true)? else {
        return Ok(None);
    };
    let variadic = variadic_function_names(db)?;
    if !mentions_call_form(statement, binder.names.as_ref(), variadic.as_ref()) {
        return Ok(None);
    }
    let mut bound = statement.clone();
    let _ = bound.visit(&mut binder);
    if let Some(error) = binder.error {
        return Err(error);
    }
    Ok(binder.changed.then_some(bound))
}

/// The same rewrite over a compiled routine body. A call that cannot be
/// resolved is left as written: it reports when (and if) it runs.
pub(crate) fn bind_routine_calls(
    db: &BicDb,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    let Some(mut binder) = binder(db, false)? else {
        return Ok(());
    };
    crate::search_path::walk_routine_parts(
        params,
        declarations,
        statements,
        handlers,
        &mut |node| {
            let _ = match node {
                crate::search_path::RoutineNode::Expr(expr) => expr.visit(&mut binder),
                crate::search_path::RoutineNode::Query(query) => query.visit(&mut binder),
                crate::search_path::RoutineNode::Statement(statement) => {
                    statement.visit(&mut binder)
                }
            };
        },
    );
    Ok(())
}

/// A function declared with OUT parameters and no RETURNS clause returns
/// its single OUT parameter's type, or `record` for several, as PostgreSQL
/// derives `prorettype`.
pub(crate) fn apply_output_return_type(routine: &mut RoutineSchema) -> Result<()> {
    if routine.kind != RoutineKind::Function || routine.return_type != "void" {
        return Ok(());
    }
    if oracle::preview_routine_language_version(&routine.language).is_some() {
        return Ok(());
    }
    let params = parse_routine_params(&routine.args)?;
    let outputs = params
        .iter()
        .enumerate()
        .filter(|(_, param)| matches!(param.mode, RoutineArgMode::Out | RoutineArgMode::InOut))
        .map(|(index, param)| {
            routine
                .arg_types
                .get(index)
                .cloned()
                .or_else(|| param.type_schema.clone())
        })
        .collect::<Vec<_>>();
    match outputs.as_slice() {
        [] => {}
        [Some(single)] => {
            routine.return_type = single.pg_type.clone();
            routine.return_type_modifier = single.type_modifier.clone();
            routine.return_type_declaration = Some(single.formatted());
        }
        _ => {
            routine.return_type = "record".to_string();
            routine.return_type_modifier = None;
            routine.return_type_declaration = Some("record".to_string());
        }
    }
    Ok(())
}

/// The OUT and INOUT parameters of a routine, in declaration order.
pub(crate) fn routine_output_params(params: &[RoutineParam]) -> Vec<&RoutineParam> {
    params
        .iter()
        .filter(|param| matches!(param.mode, RoutineArgMode::Out | RoutineArgMode::InOut))
        .collect()
}

/// The `record` a function with several OUT parameters returns: one field
/// per OUT parameter, named and typed as declared (unnamed ones are
/// `column<n>`, as PostgreSQL names them).
pub(crate) fn output_record_value(
    outputs: &[&RoutineParam],
    values: Vec<SqlValue>,
) -> Result<SqlValue> {
    let mut fields = Vec::with_capacity(outputs.len());
    let mut values = values.into_iter();
    for (position, param) in outputs.iter().enumerate() {
        let value = values.next().unwrap_or(SqlValue::Null);
        let pg_type = param
            .type_schema
            .as_ref()
            .map(|schema| schema.pg_type.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let value = if pg_type == "unknown" || matches!(value, SqlValue::Null) {
            value
        } else {
            cast_value_to_pg_type(value, &pg_type)?
        };
        fields.push(SqlCompositeField {
            name: param
                .name
                .as_deref()
                .map(normalize_object_name)
                .unwrap_or_else(|| format!("column{}", position + 1)),
            pg_type,
            value,
        });
    }
    Ok(SqlValue::Composite(SqlComposite {
        type_oid: None,
        type_name: "record".to_string(),
        fields,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(name: &str, default: Option<&str>, variadic: bool) -> InputParam {
        InputParam {
            name: Some(name.to_string()),
            default_expr: default.map(|text| crate::parse_routine_expr(text).unwrap()),
            variadic,
        }
    }

    fn expr(text: &str) -> Expr {
        crate::parse_routine_expr(text).unwrap()
    }

    fn render(args: Option<Vec<Expr>>) -> Option<String> {
        args.map(|args| {
            args.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        })
    }

    #[test]
    fn assignment_named_arguments_become_arrows() {
        assert_eq!(
            rewrite_assignment_named_args("SELECT f(a := 1, b := ':=')").unwrap(),
            "SELECT f(a => 1, b => ':=')"
        );
        assert!(rewrite_assignment_named_args("x := f(1)").is_none());
        assert!(rewrite_assignment_named_args("CURSOR c(p NUMBER := 1) IS SELECT 1").is_none());
    }

    #[test]
    fn named_and_mixed_notation_map_to_positions() {
        let inputs = [param("a", None, false), param("b", Some("1"), false)];
        let named = [
            CallArg::Named("b".into(), expr("2")),
            CallArg::Named("a".into(), expr("1")),
        ];
        assert_eq!(
            render(positional_arguments(&inputs, &named)).unwrap(),
            "1, 2"
        );
        let mixed = [
            CallArg::Positional(expr("4")),
            CallArg::Named("b".into(), expr("5")),
        ];
        assert_eq!(
            render(positional_arguments(&inputs, &mixed)).unwrap(),
            "4, 5"
        );
        let defaulted = [CallArg::Named("a".into(), expr("3"))];
        assert_eq!(
            render(positional_arguments(&inputs, &defaulted)).unwrap(),
            "3, 1"
        );
        let missing = [CallArg::Named("b".into(), expr("3"))];
        assert!(positional_arguments(&inputs, &missing).is_none());
        let unknown = [CallArg::Named("c".into(), expr("3"))];
        assert!(positional_arguments(&inputs, &unknown).is_none());
        let twice = [
            CallArg::Positional(expr("1")),
            CallArg::Named("a".into(), expr("2")),
        ];
        assert!(positional_arguments(&inputs, &twice).is_none());
    }

    #[test]
    fn variadic_arguments_collect_into_an_array() {
        let inputs = [param("sep", None, false), param("parts", None, true)];
        let spread = [
            CallArg::Positional(expr("'-'")),
            CallArg::Positional(expr("'a'")),
            CallArg::Positional(expr("'b'")),
        ];
        assert_eq!(
            render(positional_arguments(&inputs, &spread)).unwrap(),
            "'-', ARRAY['a', 'b']"
        );
        let explicit = [
            CallArg::Positional(expr("','")),
            CallArg::Variadic(expr("ARRAY['x']")),
        ];
        assert_eq!(
            render(positional_arguments(&inputs, &explicit)).unwrap(),
            "',', ARRAY['x']"
        );
        let named = [
            CallArg::Named("parts".into(), expr("ARRAY['y']")),
            CallArg::Named("sep".into(), expr("'+'")),
        ];
        assert!(positional_arguments(&inputs, &named).is_none());
        // A variadic parameter needs at least one argument.
        assert!(positional_arguments(&inputs, &[CallArg::Positional(expr("'-'"))]).is_none());
    }
}
