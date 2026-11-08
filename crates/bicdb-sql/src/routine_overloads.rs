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
    if db.collection_generation(ROUTINE_COLLECTION) == 0 {
        return Ok(None);
    }
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

/// The candidate that best fits a call, or `None` when none accepts it.
pub(crate) fn select_overload<'a>(
    candidates: &'a [RoutineSchema],
    static_types: &[Option<String>],
    args: &[SqlValue],
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
            match argument_score(declared, static_type, value) {
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
    let overloads = routine_overloads(db, RoutineKind::Function, name).ok()??;
    select_overload(&overloads, static_types, &[]).map(|routine| routine.return_type.clone())
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
    let Some(selected) = select_overload(&overloads, static_types, args) else {
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
