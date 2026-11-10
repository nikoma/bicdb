//! Function overloading: several routines may share a name when their input
//! argument types differ. The first routine of a name keeps the name's
//! catalog key; each further signature is stored under a key that carries its
//! input types. A call selects the candidate whose declared input types fit
//! the arguments best: by the arguments' static types where the caller knows
//! them, otherwise by whether each argument value converts to the declared
//! type. A name with one routine resolves exactly as before.
use crate::*;
use std::sync::Arc;

/// The input-type identity of a routine: declared IN/INOUT/VARIADIC argument
/// types, ignoring names, defaults, OUT arguments and type modifiers.
pub(crate) fn routine_input_signature(routine: &RoutineSchema) -> Result<String> {
    Ok(declared_inputs(routine)?
        .iter()
        .map(|(pg_type, _)| pg_type.as_deref().map(type_identity).unwrap_or_default())
        .collect::<Vec<_>>()
        .join(","))
}

/// Declared input arguments: type name and whether a default makes it optional.
/// Recorded argument types are authoritative; the declaration text is the
/// fallback for routines recorded without them.
fn declared_inputs(routine: &RoutineSchema) -> Result<Vec<(Option<String>, bool)>> {
    let params = parse_routine_params(&routine.args)?;
    let recorded = routine.arg_types.len() == params.len();
    Ok(params
        .iter()
        .enumerate()
        .filter(|(_, param)| param.mode != RoutineArgMode::Out)
        .map(|(index, param)| {
            let pg_type = if recorded {
                Some(routine.arg_types[index].pg_type.clone())
            } else {
                param
                    .type_schema
                    .as_ref()
                    .map(|schema| schema.pg_type.clone())
            };
            (pg_type, param.default_expr.is_some())
        })
        .collect())
}

/// Input-type identity of an `ALTER/DROP FUNCTION name(types)` signature.
pub(crate) fn requested_input_signature(
    signature: &[sqlparser::ast::OperateFunctionArg],
) -> Result<String> {
    Ok(signature
        .iter()
        .filter(|arg| arg.mode != Some(sqlparser::ast::ArgMode::Out))
        .map(|arg| routine_type_schema(&arg.data_type).map(|schema| type_identity(&schema.pg_type)))
        .collect::<Result<Vec<_>>>()?
        .join(","))
}

fn type_identity(pg_type: &str) -> String {
    pg_type_oid_by_name(pg_type)
        .or_else(|| {
            // Spelled-out SQL type names (`character varying`).
            raw_routine_type_details(pg_type)
                .ok()
                .and_then(|(schema, _)| pg_type_oid_by_name(&schema.pg_type))
        })
        .map(|oid| oid.to_string())
        .unwrap_or_else(|| pg_type.to_ascii_lowercase())
}

/// Catalog key of one routine: the plain name key for the first signature,
/// or the name key plus its input signature for a further overload.
pub(crate) fn routine_storage_key(routine: &RoutineSchema) -> String {
    match &routine.overload {
        Some(signature) => format!("{}({signature})", routine_key(routine.kind, &routine.name)),
        None => routine_key(routine.kind, &routine.name),
    }
}

type OverloadMap = FxHashMap<String, Arc<Vec<RoutineSchema>>>;

thread_local! {
    /// Names with more than one routine, per database and routine catalog
    /// generation; single-routine names are absent so their lookup stays one
    /// probe.
    static OVERLOADS: RefCell<Option<(u64, u64, Arc<OverloadMap>)>> = const { RefCell::new(None) };
}

fn overload_map(db: &BicDb) -> Result<Arc<OverloadMap>> {
    let database_id = db.instance_id();
    let generation = db.collection_generation(ROUTINE_COLLECTION);
    if let Some(map) = OVERLOADS.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .and_then(|(id, cached_generation, map)| {
                (*id == database_id && *cached_generation == generation).then(|| map.clone())
            })
    }) {
        return Ok(map);
    }
    let mut groups: FxHashMap<String, Vec<RoutineSchema>> = FxHashMap::default();
    for routine in list_routines(db)? {
        groups
            .entry(routine_key(routine.kind, &routine.name))
            .or_default()
            .push(routine);
    }
    let map: OverloadMap = groups
        .into_iter()
        .filter(|(_, routines)| routines.len() > 1)
        .map(|(key, mut routines)| {
            // The name's first routine leads; further overloads keep a stable order.
            routines.sort_by(|left, right| left.overload.cmp(&right.overload));
            (key, Arc::new(routines))
        })
        .collect();
    let map = Arc::new(map);
    OVERLOADS.with(|cache| *cache.borrow_mut() = Some((database_id, generation, map.clone())));
    Ok(map)
}

/// Every routine of a name when the name is overloaded, else `None`.
pub(crate) fn routine_overloads(
    db: &BicDb,
    kind: RoutineKind,
    name: &str,
) -> Result<Option<Arc<Vec<RoutineSchema>>>> {
    // A generation of 0 does not mean "no routines": a collection not yet
    // read in this process reports 0 too. The map is cached per generation.
    Ok(overload_map(db)?.get(&routine_key(kind, name)).cloned())
}

/// All routines of a name (the name's first routine first).
pub(crate) fn routines_named(
    db: &BicDb,
    kind: RoutineKind,
    name: &str,
) -> Result<Vec<RoutineSchema>> {
    if let Some(overloads) = routine_overloads(db, kind, name)? {
        return Ok(overloads.as_ref().clone());
    }
    Ok(load_routine(db, kind, name)?.into_iter().collect())
}

const NUMERIC: &[i32] = &[20, 21, 23, 26, 700, 701, 1700];
const STRING: &[i32] = &[18, 19, 25, 705, 1042, 1043];
const DATETIME: &[i32] = &[1082, 1114, 1184];
const POLYMORPHIC: &[i32] = &[2276, 2277, 2283, 2776, 5077, 5078];
const INTERVAL: i32 = 1186;
const BOOL: i32 = 16;

fn same_category(left: i32, right: i32) -> bool {
    [NUMERIC, STRING, DATETIME]
        .iter()
        .any(|category| category.contains(&left) && category.contains(&right))
}

/// How well an argument fits a declared type: higher is better, `None` means
/// the argument cannot be passed as that type.
fn argument_score(
    declared: Option<i32>,
    static_type: Option<&str>,
    value: &SqlValue,
) -> Option<u32> {
    let Some(declared) = declared else {
        return Some(1);
    };
    if POLYMORPHIC.contains(&declared) {
        return Some(1);
    }
    if let Some(actual) = static_type.and_then(|pg_type| {
        pg_type_oid_by_name(pg_type).or_else(|| type_identity(pg_type).parse().ok())
    }) {
        if actual == declared {
            return Some(4);
        }
        if actual == 705 {
            return Some(1);
        }
        return same_category(actual, declared).then_some(3);
    }
    match value {
        SqlValue::Null => Some(1),
        SqlValue::Bool(_) => (declared == BOOL).then_some(3),
        SqlValue::Int(_) => NUMERIC.contains(&declared).then_some(2),
        SqlValue::Float(_) => NUMERIC.contains(&declared).then_some(2),
        SqlValue::String(text) => {
            let timestamp = parse_timestamptz(text).is_ok();
            if STRING.contains(&declared) {
                Some(2)
            } else if DATETIME.contains(&declared) {
                timestamp.then_some(2)
            } else if declared == INTERVAL {
                (!timestamp && parse_interval_seconds(text).is_some()).then_some(2)
            } else if NUMERIC.contains(&declared) {
                text.trim().parse::<f64>().is_ok().then_some(1)
            } else {
                Some(1)
            }
        }
        _ => Some(1),
    }
}

// PostgreSQL resolves typed arguments through implicit casts, then favors a
// preferred type within a category. Oracle retains its existing conversion
// rules; in particular, numeric narrowing must not change there.
fn pg_argument_score(
    declared: Option<i32>,
    static_type: Option<&str>,
    value: &SqlValue,
) -> Option<u32> {
    let actual = static_type.and_then(static_oid);
    let (Some(actual), Some(declared)) = (actual, declared) else {
        return argument_score(declared, static_type, value);
    };
    if actual == declared {
        return Some(8);
    }
    if POLYMORPHIC.contains(&declared) {
        return Some(1);
    }
    let input = pg_type_spec_by_oid(actual)?;
    let target = pg_type_spec_by_oid(declared)?;
    pg_cast_allows(input.name, target.name, PgCastContext::Implicit).then_some(
        if input.category == target.category && target.preferred() {
            4
        } else {
            3
        },
    )
}

fn builtin_accepts_typed_arguments(name: &str, actual: &[Option<i32>]) -> bool {
    use crate::builtin_signatures::PG_CATALOG_FUNCTION_SIGNATURES as SIGNATURES;
    let start = SIGNATURES.partition_point(|(candidate, _)| *candidate < name);
    SIGNATURES[start..]
        .iter()
        .take_while(|(candidate, _)| *candidate == name)
        .any(|(_, signature)| {
            let declared = signature
                .split(',')
                .filter(|part| !part.is_empty())
                .map(|part| part.parse::<i32>().ok())
                .collect::<Vec<_>>();
            // Polymorphic/variadic builtins stay on their established dispatch path.
            if declared
                .iter()
                .flatten()
                .any(|oid| POLYMORPHIC.contains(oid))
            {
                return true;
            }
            declared.len() == actual.len()
                && actual.iter().zip(declared).all(|(input, target)| {
                    match (
                        input.and_then(pg_type_spec_by_oid),
                        target.and_then(pg_type_spec_by_oid),
                    ) {
                        (Some(input), Some(target)) => {
                            pg_cast_allows(input.name, target.name, PgCastContext::Implicit)
                        }
                        _ => false,
                    }
                })
        })
}

/// The candidate that best fits a call, or `None` when none accepts it.
pub(crate) fn select_overload<'a>(
    candidates: &'a [RoutineSchema],
    static_types: &[Option<String>],
    args: &[SqlValue],
) -> Option<&'a RoutineSchema> {
    select_overload_scored(candidates, static_types, args, argument_score)
}

fn select_overload_scored<'a>(
    candidates: &'a [RoutineSchema],
    static_types: &[Option<String>],
    args: &[SqlValue],
    score_argument: fn(Option<i32>, Option<&str>, &SqlValue) -> Option<u32>,
) -> Option<&'a RoutineSchema> {
    let mut best: Option<(u32, &RoutineSchema)> = None;
    for candidate in candidates {
        let Ok(inputs) = declared_inputs(candidate) else {
            continue;
        };
        let required = inputs.iter().filter(|(_, optional)| !optional).count();
        let count = if args.is_empty() {
            static_types.len()
        } else {
            args.len()
        };
        if count < required || count > inputs.len() {
            continue;
        }
        let mut total = 0;
        let mut accepted = true;
        for (index, (pg_type, _)) in inputs.iter().take(count).enumerate() {
            let declared = pg_type.as_deref().and_then(|pg_type| {
                pg_type_oid_by_name(pg_type).or_else(|| type_identity(pg_type).parse().ok())
            });
            let static_type = static_types.get(index).and_then(Option::as_deref);
            let value = args.get(index).unwrap_or(&SqlValue::Null);
            match score_argument(declared, static_type, value) {
                Some(score) => total += score,
                None => {
                    accepted = false;
                    break;
                }
            }
        }
        if accepted && best.is_none_or(|(score, _)| total > score) {
            best = Some((total, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}

/// Return type of a call to `name` with arguments of the given static types.
pub(crate) fn overload_return_type(
    db: &BicDb,
    name: &str,
    static_types: &[Option<String>],
) -> Option<String> {
    // Without every argument's type the call's overload is not known until
    // it runs; guessing would report the name's first routine's type.
    if static_types.iter().any(Option::is_none) {
        return None;
    }
    let overloads = routine_overloads(db, RoutineKind::Function, name).ok()??;
    let score = if oracle::is_oracle_database(db).ok()? {
        argument_score
    } else {
        pg_argument_score
    };
    select_overload_scored(&overloads, static_types, &[], score)
        .map(|routine| routine.return_type.clone())
}

/// The routine a call of `name` executes, compiled. Names with one routine
/// resolve through the ordinary routine cache.
pub(crate) fn resolve_routine_for_call(
    db: &BicDb,
    kind: RoutineKind,
    name: &str,
    static_types: &[Option<String>],
    args: &[SqlValue],
) -> Result<Option<CachedRoutine>> {
    let Some(overloads) = routine_overloads(db, kind, name)? else {
        return resolve_routine_cached(db, kind, name);
    };
    let score = if oracle::is_oracle_database(db)? {
        argument_score
    } else {
        pg_argument_score
    };
    let Some(selected) = select_overload_scored(&overloads, static_types, args, score) else {
        return resolve_routine_cached(db, kind, name);
    };
    if selected.overload.is_none() {
        return resolve_routine_cached(db, kind, name);
    }
    let schema = Arc::new(selected.clone().with_declared_metadata());
    oracle::validate_routine_language(db, &schema.language)?;
    let ir = compile_cached_routine_ir_in_catalog(&schema, Some(db))?;
    let search_path = crate::search_path::active_path();
    let compilation_scope = (oracle::typed_preview_routine_language(&schema.language)
        || search_path.is_some())
    .then(|| routine_compilation_scope(db));
    Ok(Some(CachedRoutine {
        schema,
        ir,
        compilation_scope,
        search_path,
    }))
}

thread_local! {
    /// Names of every user-defined function, per database and routine
    /// catalog generation.
    static FUNCTION_NAMES: RefCell<Option<(u64, u64, Arc<FxHashSet<String>>)>> =
        const { RefCell::new(None) };
}

/// Names of every user-defined function in `db`.
pub(crate) fn user_function_names(db: &BicDb) -> Result<Arc<FxHashSet<String>>> {
    let database_id = db.instance_id();
    let generation = db.collection_generation(ROUTINE_COLLECTION);
    if let Some(names) = FUNCTION_NAMES.with(|cache| {
        cache.borrow().as_ref().and_then(|(id, cached, names)| {
            (*id == database_id && *cached == generation).then(|| names.clone())
        })
    }) {
        return Ok(names);
    }
    let names: FxHashSet<String> = list_routines(db)?
        .into_iter()
        .filter(|routine| routine.kind == RoutineKind::Function)
        .map(|routine| routine.name.to_ascii_lowercase())
        .collect();
    let names = Arc::new(names);
    FUNCTION_NAMES
        .with(|cache| *cache.borrow_mut() = Some((database_id, generation, names.clone())));
    Ok(names)
}

fn static_oid(pg_type: &str) -> Option<i32> {
    pg_type_oid_by_name(pg_type)
        .or_else(|| type_identity(pg_type).parse().ok())
        .filter(|oid| *oid != 705)
}

/// Declared input oids of `candidate` for a call with `count` arguments, or
/// `None` when it cannot take that many.
fn candidate_oids(candidate: &RoutineSchema, count: usize) -> Option<Vec<Option<i32>>> {
    let inputs = declared_inputs(candidate).ok()?;
    let required = inputs.iter().filter(|(_, optional)| !optional).count();
    if count < required || count > inputs.len() {
        return None;
    }
    Some(
        inputs
            .iter()
            .take(count)
            .map(|(pg_type, _)| pg_type.as_deref().and_then(static_oid))
            .collect(),
    )
}

/// Whether the session's search_path reaches `pg_catalog` before `schema`.
/// `pg_catalog` not named on the path is searched first.
fn pg_catalog_precedes(gucs: &HashMap<String, String>, schema: &str) -> bool {
    let path = crate::search_path::parse_search_path(
        gucs.get("search_path")
            .map(String::as_str)
            .unwrap_or("\"$user\", public"),
        &current_user_from_gucs(gucs),
    );
    let schema = if schema.is_empty() { "public" } else { schema };
    let position = |name: &str| path.iter().position(|entry| entry == name);
    match (position("pg_catalog"), position(schema)) {
        (Some(catalog), Some(own)) => catalog < own,
        _ => true,
    }
}

/// Whether `pg_catalog` has a function `name` with exactly these input types.
fn builtin_has_signature(name: &str, oids: &[i32]) -> bool {
    use crate::builtin_signatures::PG_CATALOG_FUNCTION_SIGNATURES as SIGNATURES;
    let signature = oids
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let start = SIGNATURES.partition_point(|(candidate, _)| *candidate < name);
    SIGNATURES[start..]
        .iter()
        .take_while(|(candidate, _)| *candidate == name)
        .any(|(_, types)| *types == signature)
}

/// Whether an unqualified call of a function name that is also built in runs
/// a user-defined function of that name instead, as PostgreSQL resolves it:
/// a user function whose declared input types match the arguments' types
/// exactly is chosen over a built-in that needs a conversion; with argument
/// types unknown, a user function the argument values fit is. Otherwise the
/// built-in keeps the call.
///
/// When a built-in has the very same signature, the search_path decides:
/// the user function wins only when its schema precedes `pg_catalog` (which
/// is searched first unless the path names it).
pub(crate) fn user_function_takes_builtin_call(
    db: &BicDb,
    gucs: &HashMap<String, String>,
    name: &str,
    static_types: &[Option<String>],
    args: &[SqlValue],
) -> Result<bool> {
    Ok(chosen_user_function(db, gucs, name, static_types, Some(args))?.is_some())
}

/// Result type of a call of a built-in name that a user-defined function
/// takes by the arguments' static types (see
/// [`user_function_takes_builtin_call`]), for type inference before the call
/// runs.
pub(crate) fn user_function_result_type(
    db: &BicDb,
    gucs: &HashMap<String, String>,
    name: &str,
    static_types: &[Option<String>],
) -> Option<String> {
    chosen_user_function(db, gucs, name, static_types, None)
        .ok()
        .flatten()
        .map(|routine| routine.return_type)
}

fn chosen_user_function(
    db: &BicDb,
    gucs: &HashMap<String, String>,
    name: &str,
    static_types: &[Option<String>],
    values: Option<&[SqlValue]>,
) -> Result<Option<RoutineSchema>> {
    let lowered;
    let name = if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
        lowered = name.to_ascii_lowercase();
        lowered.as_str()
    } else {
        name
    };
    if name.contains('.') || !user_function_names(db)?.contains(name) {
        return Ok(None);
    }
    // The second profile keeps built-in functions ahead of user functions
    // of the same name; those are reached by qualified calls.
    if oracle::is_oracle_database(db)? {
        return Ok(None);
    }
    let count = values.map_or(static_types.len(), <[SqlValue]>::len);
    let candidates = routines_named(db, RoutineKind::Function, name)?;
    let actual: Vec<Option<i32>> = (0..count)
        .map(|index| {
            static_types
                .get(index)
                .and_then(Option::as_deref)
                .and_then(static_oid)
        })
        .collect();
    let mut by_value = None;
    let mut eligible = Vec::new();
    for candidate in &candidates {
        let Some(declared) = candidate_oids(candidate, count) else {
            continue;
        };
        // A built-in of the same signature found first on the path hides it.
        if declared.iter().all(Option::is_some) {
            let oids: Vec<i32> = declared.iter().flatten().copied().collect();
            if builtin_has_signature(name, &oids) && pg_catalog_precedes(gucs, &candidate.schema) {
                continue;
            }
        }
        eligible.push(candidate);
        if actual
            .iter()
            .zip(&declared)
            .all(|(actual, declared)| actual.is_some() && actual == declared)
        {
            return Ok(Some(candidate.clone()));
        }
        let Some(values) = values else {
            continue;
        };
        if by_value.is_none()
            && actual.iter().any(Option::is_none)
            && actual
                .iter()
                .zip(&declared)
                .zip(values)
                .all(|((actual, declared), value)| match (actual, declared) {
                    (Some(actual), Some(declared)) => {
                        actual == declared || same_category(*actual, *declared)
                    }
                    (Some(_), None) => true,
                    (None, declared) => {
                        !matches!(value, SqlValue::Null)
                            && argument_score(*declared, None, value)
                                .is_some_and(|score| score >= 2)
                    }
                })
        {
            by_value = Some(candidate.clone());
        }
    }
    if by_value.is_none()
        && actual.iter().all(Option::is_some)
        && !builtin_accepts_typed_arguments(name, &actual)
    {
        let eligible = eligible.into_iter().cloned().collect::<Vec<_>>();
        return Ok(select_overload_scored(
            &eligible,
            static_types,
            values.unwrap_or(&[]),
            pg_argument_score,
        )
        .cloned());
    }
    Ok(by_value)
}
