//! SqlSession: the top-level SQL entry point (statement execution/dispatch, transactions and savepoints, DDL orchestration with undo, DML with FK cascades, RLS enforcement, routine invocation) plus DdlUndo and SavepointMark.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use session::*;`.

mod domain_types;
pub(crate) mod functions_insert;
mod privilege_projection;
mod raw_ddl_analyze;
mod raw_index_fts;
mod row_triggers;
mod set_functions;
pub(crate) mod txn_update;
// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

const POSTGRES_SYSTEM_COLUMN_NAMES: [&str; 6] =
    ["tableoid", "xmin", "xmax", "cmin", "cmax", "ctid"];

/// Session attributes whose values are security authority rather than
/// application-controlled PostgreSQL GUCs. These names remain readable through
/// `current_setting` for PostgreSQL/BicDB application policy compatibility, but their
/// values are derived exclusively from the host-installed [`SecurityContext`].
pub const PROTECTED_SECURITY_SETTINGS: [&str; 20] = [
    "bicdb.current_tenant",
    "bicdb.current_workspace",
    "bicdb.current_user",
    "bicdb.current_client",
    "bicdb.current_roles",
    "bicdb.current_scopes",
    "bicdb.current_email",
    "bicdb.current_name",
    "bicdb.current_session",
    "bicdb.authentication_strength",
    // Compatibility aliases for packages and migrations produced before the
    // application contract became industry-neutral.
    "carrier.current_tenant",
    "carrier.current_workspace",
    "carrier.current_user",
    "carrier.current_client",
    "carrier.current_roles",
    "carrier.current_scopes",
    "carrier.current_email",
    "carrier.current_name",
    "carrier.current_session",
    "carrier.authentication_strength",
];

pub fn is_protected_security_setting(name: &str) -> bool {
    PROTECTED_SECURITY_SETTINGS
        .iter()
        .any(|protected| name.eq_ignore_ascii_case(protected))
}

const POSTGRES_VERSION_BANNER_GUC: &str = "bicdb.postgres_version_banner";

fn postgres_compatibility_gucs_enabled(settings: &HashMap<String, String>) -> bool {
    settings
        .get(POSTGRES_VERSION_BANNER_GUC)
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "on" | "true" | "yes"
            )
        })
}

fn strip_protected_security_settings(settings: &mut HashMap<String, String>) {
    settings.retain(|name, _| !is_protected_security_setting(name));
}

/// Shared-map twin of [`bind_trusted_security_settings`]. The per-statement
/// engine used to deep-clone the whole session GUC map only to run this
/// binding on the copy; with no host-bound context and no protected keys
/// present the binding is a no-op, so the `Arc` is passed through untouched
/// and nothing is allocated.
pub(crate) fn bind_trusted_security_settings_shared(
    settings: Arc<HashMap<String, String>>,
    context: Option<&SecurityContext>,
) -> Arc<HashMap<String, String>> {
    if context.is_none()
        && !settings
            .keys()
            .any(|name| is_protected_security_setting(name))
    {
        return settings;
    }
    let mut owned = (*settings).clone();
    bind_trusted_security_settings(&mut owned, context);
    Arc::new(owned)
}

pub(crate) fn bind_trusted_security_settings(
    settings: &mut HashMap<String, String>,
    context: Option<&SecurityContext>,
) {
    // The explicit PostgreSQL compatibility mode is a server-owned opt-in.
    // Without a host-bound security context, BicDB application's dotted settings are
    // ordinary custom PostgreSQL GUCs and must retain PostgreSQL semantics.
    // Clients cannot enable this mode themselves because the banner GUC is
    // immutable through SQL. Default BicDB mode and host-bound sessions keep
    // deriving these values exclusively from the trusted context below.
    if context.is_none() && postgres_compatibility_gucs_enabled(settings) {
        return;
    }
    strip_protected_security_settings(settings);
    let Some(context) = context else {
        return;
    };
    let roles = context.roles.iter().cloned().collect::<Vec<_>>().join(",");
    let scopes = context.scopes.iter().cloned().collect::<Vec<_>>().join(",");
    let email = context
        .policy_attributes
        .get("email")
        .map(String::as_str)
        .unwrap_or_default();
    let name = context
        .policy_attributes
        .get("name")
        .map(String::as_str)
        .unwrap_or_default();
    for (suffix, value) in [
        ("current_tenant", context.tenant_id.as_str()),
        (
            "current_workspace",
            context.workspace_id.as_deref().unwrap_or_default(),
        ),
        ("current_user", context.user_id.as_str()),
        (
            "current_client",
            context.client_id.as_deref().unwrap_or_default(),
        ),
        ("current_roles", roles.as_str()),
        ("current_scopes", scopes.as_str()),
        ("current_email", email),
        ("current_name", name),
        (
            "current_session",
            context.session_id.as_deref().unwrap_or_default(),
        ),
        (
            "authentication_strength",
            context.authentication_strength.as_str(),
        ),
    ] {
        // Missing authority stays SQL NULL under current_setting(..., true).
        // In particular, an upgraded login without a tenant binding must not
        // accidentally gain access to rows whose tenant column is empty.
        if !value.is_empty() {
            for prefix in ["bicdb", "carrier"] {
                settings.insert(format!("{prefix}.{suffix}"), value.to_string());
            }
        }
    }
}

fn protected_security_setting_error(name: &str) -> SqlError {
    SqlError::BicDb(BicDbError::Authorization(format!(
        "{name} is a protected session attribute"
    )))
}

fn reject_postgres_system_column_name(name: &str) -> Result<()> {
    if POSTGRES_SYSTEM_COLUMN_NAMES
        .iter()
        .any(|system| name.eq_ignore_ascii_case(system))
    {
        return Err(SqlError::data_exception(
            "42701",
            format!("column name \"{name}\" conflicts with a system column name"),
            Some(name.to_string()),
        ));
    }
    Ok(())
}

fn reject_postgis_extension(name: &str) -> Result<()> {
    let name = name.to_ascii_lowercase();
    if name == "postgis" || name.starts_with("postgis_") {
        return Err(SqlError::Unsupported(
            "PostGIS is not installed; BicDB Spatial is a distinct, explicitly limited API"
                .to_string(),
        ));
    }
    Ok(())
}

fn histogram_quantiles<T: Clone>(values: Vec<T>, limit: usize) -> Vec<T> {
    if values.len() <= limit {
        return values;
    }
    if limit <= 1 {
        return values.into_iter().take(limit).collect();
    }
    (0..limit)
        .map(|index| {
            let source = index * (values.len() - 1) / (limit - 1);
            values[source].clone()
        })
        .collect()
}

fn create_function_internal_symbol(create_function: &CreateFunction) -> Result<String> {
    let Some(CreateFunctionBody::AsBeforeOptions {
        body,
        link_symbol: None,
    }) = &create_function.function_body
    else {
        return Err(SqlError::Unsupported(
            "LANGUAGE internal requires exactly one registered symbol and no module path"
                .to_string(),
        ));
    };
    let symbol = match body {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(symbol),
            ..
        }) => symbol,
        Expr::Value(ValueWithSpan {
            value: Value::DollarQuotedString(symbol),
            ..
        }) => &symbol.value,
        _ => {
            return Err(SqlError::InvalidSql(
                "LANGUAGE internal symbol must be a string literal".to_string(),
            ));
        }
    };
    Ok(symbol.to_ascii_lowercase())
}

fn routine_pseudo_type_name(type_name: &str) -> Option<&'static str> {
    let normalized = collapse_sql_whitespace(type_name);
    let normalized = normalized.trim_matches('"').to_ascii_lowercase();
    let normalized = normalized
        .strip_prefix("pg_catalog.")
        .unwrap_or(&normalized)
        .trim_matches('"');
    if normalized == "record[]" {
        return Some("record[]");
    }
    pg_type_spec(&normalized)
        .filter(|spec| spec.pseudo)
        .map(|spec| spec.name)
}

fn polymorphic_type_family(type_name: &str) -> Option<u8> {
    match type_name {
        "anyelement" | "anyarray" | "anynonarray" | "anyenum" | "anyrange" | "anymultirange" => {
            Some(1)
        }
        "anycompatible"
        | "anycompatiblearray"
        | "anycompatiblenonarray"
        | "anycompatiblerange"
        | "anycompatiblemultirange" => Some(2),
        _ => None,
    }
}

fn polymorphic_result_is_deducible(result: &str, inputs: &[&str]) -> bool {
    let Some(family) = polymorphic_type_family(result) else {
        return true;
    };
    let same_family = |input: &&str| polymorphic_type_family(input) == Some(family);
    match result {
        "anyrange" => inputs.iter().any(|input| *input == "anyrange"),
        "anymultirange" => inputs
            .iter()
            .any(|input| matches!(*input, "anyrange" | "anymultirange")),
        "anycompatiblerange" => inputs.iter().any(|input| *input == "anycompatiblerange"),
        "anycompatiblemultirange" => inputs
            .iter()
            .any(|input| matches!(*input, "anycompatiblerange" | "anycompatiblemultirange")),
        _ => inputs.iter().any(same_family),
    }
}

fn routine_argument_directions(
    args: Option<&Vec<sqlparser::ast::OperateFunctionArg>>,
    arg_types: &[RoutineTypeSchema],
) -> (Vec<RoutineTypeSchema>, Vec<RoutineTypeSchema>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for (arg, schema) in args.into_iter().flatten().zip(arg_types) {
        match arg.mode.as_ref() {
            Some(sqlparser::ast::ArgMode::Out) => outputs.push(schema.clone()),
            Some(sqlparser::ast::ArgMode::InOut) => {
                inputs.push(schema.clone());
                outputs.push(schema.clone());
            }
            _ => inputs.push(schema.clone()),
        }
    }
    (inputs, outputs)
}

fn raw_routine_argument_directions(
    args: &[String],
    arg_types: &[RoutineTypeSchema],
) -> (Vec<RoutineTypeSchema>, Vec<RoutineTypeSchema>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for (arg, schema) in args.iter().zip(arg_types) {
        let words = split_sql_words(&routine_arg_without_default(arg));
        let mode = words
            .iter()
            .take(2)
            .find_map(|word| routine_mode_word(word));
        match mode {
            Some(RoutineArgMode::Out) => outputs.push(schema.clone()),
            Some(RoutineArgMode::InOut) => {
                inputs.push(schema.clone());
                outputs.push(schema.clone());
            }
            _ => inputs.push(schema.clone()),
        }
    }
    (inputs, outputs)
}

fn explicit_return_type_schemas(
    return_type: Option<&FunctionReturnType>,
) -> Result<Vec<RoutineTypeSchema>> {
    let Some(return_type) = return_type else {
        return Ok(Vec::new());
    };
    let data_type = match return_type {
        FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type) => data_type,
    };
    match data_type {
        DataType::Table(Some(columns)) => columns
            .iter()
            .map(|column| routine_type_schema(&column.data_type))
            .collect(),
        DataType::NamedTable { columns, .. } => columns
            .iter()
            .map(|column| routine_type_schema(&column.data_type))
            .collect(),
        _ => Ok(vec![routine_type_schema(data_type)?]),
    }
}

fn validate_routine_pseudo_types(
    input_types: &[RoutineTypeSchema],
    output_types: &[RoutineTypeSchema],
    declared_arg_count: usize,
    trigger_return: Option<&str>,
    language: &str,
) -> Result<()> {
    let pseudo_inputs = input_types
        .iter()
        .filter_map(|arg| routine_pseudo_type_name(&arg.pg_type))
        .collect::<Vec<_>>();
    let pseudo_outputs = output_types
        .iter()
        .filter_map(|output| routine_pseudo_type_name(&output.pg_type))
        .collect::<Vec<_>>();

    for result in pseudo_outputs
        .iter()
        .copied()
        .filter(|result| polymorphic_type_family(result).is_some())
    {
        if !polymorphic_result_is_deducible(result, &pseudo_inputs) {
            return Err(SqlError::data_exception(
                "42P13",
                "cannot determine result data type",
                Some(result.to_string()),
            ));
        }
    }
    if pseudo_outputs.contains(&"internal") && !pseudo_inputs.contains(&"internal") {
        return Err(SqlError::data_exception(
            "42P13",
            "unsafe use of pseudo-type \"internal\"",
            Some("internal".to_string()),
        ));
    }

    if language == "internal" {
        return Ok(());
    }

    if language == "sql" {
        for result in pseudo_outputs.iter().copied() {
            if !matches!(result, "record" | "void") && polymorphic_type_family(result).is_none() {
                return Err(SqlError::data_exception(
                    "42P13",
                    format!("SQL functions cannot return type {result}"),
                    Some(result.to_string()),
                ));
            }
        }
        if let Some(argument) = pseudo_inputs
            .iter()
            .find(|argument| polymorphic_type_family(argument).is_none())
        {
            return Err(SqlError::data_exception(
                "42P13",
                format!("SQL functions cannot have arguments of type {argument}"),
                Some((*argument).to_string()),
            ));
        }
        return Ok(());
    }

    if matches!(trigger_return, Some("trigger" | "event_trigger")) && declared_arg_count != 0 {
        let kind = if trigger_return == Some("trigger") {
            "trigger"
        } else {
            "event trigger"
        };
        return Err(SqlError::data_exception(
            "42P13",
            format!("{kind} functions cannot have declared arguments"),
            trigger_return.map(str::to_string),
        ));
    }
    for result in pseudo_outputs.iter().copied() {
        if !matches!(result, "record" | "void" | "trigger" | "event_trigger")
            && polymorphic_type_family(result).is_none()
        {
            return Err(SqlError::data_exception(
                "0A000",
                format!("PL/pgSQL functions cannot return type {result}"),
                Some(result.to_string()),
            ));
        }
    }
    if let Some(argument) = pseudo_inputs
        .iter()
        .find(|argument| polymorphic_type_family(argument).is_none())
    {
        return Err(SqlError::data_exception(
            "0A000",
            format!("PL/pgSQL functions cannot accept type {argument}"),
            Some((*argument).to_string()),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct BaseTypeDefinition {
    input: Option<String>,
    output: Option<String>,
    receive: Option<String>,
    send: Option<String>,
    like_type: Option<String>,
    internal_length: Option<i64>,
    passed_by_value: Option<bool>,
    alignment: Option<char>,
    storage: Option<char>,
    category: Option<char>,
    preferred: bool,
    default_expr: Option<String>,
    element_type: Option<String>,
    delimiter: Option<char>,
    collatable: bool,
}

#[derive(Default)]
struct RangeTypeDefinition {
    subtype: Option<DataType>,
    subtype_opclass: Option<String>,
    collation: Option<String>,
    canonical: Option<String>,
    subtype_diff: Option<String>,
    multirange_type_name: Option<ObjectName>,
}

fn parse_range_type_definition(
    options: &[sqlparser::ast::UserDefinedTypeRangeOption],
) -> Result<RangeTypeDefinition> {
    use sqlparser::ast::UserDefinedTypeRangeOption;

    let mut definition = RangeTypeDefinition::default();
    for option in options {
        match option {
            UserDefinedTypeRangeOption::Subtype(data_type) => {
                set_base_type_option(&mut definition.subtype, data_type.clone(), "SUBTYPE")?
            }
            UserDefinedTypeRangeOption::SubtypeOpClass(name) => set_base_type_option(
                &mut definition.subtype_opclass,
                object_name(name)?.to_ascii_lowercase(),
                "SUBTYPE_OPCLASS",
            )?,
            UserDefinedTypeRangeOption::Collation(name) => set_base_type_option(
                &mut definition.collation,
                normalize_column_collation(name)?,
                "COLLATION",
            )?,
            UserDefinedTypeRangeOption::Canonical(name) => {
                set_base_type_option(&mut definition.canonical, relation_name(name)?, "CANONICAL")?
            }
            UserDefinedTypeRangeOption::SubtypeDiff(name) => set_base_type_option(
                &mut definition.subtype_diff,
                relation_name(name)?,
                "SUBTYPE_DIFF",
            )?,
            UserDefinedTypeRangeOption::MultirangeTypeName(name) => set_base_type_option(
                &mut definition.multirange_type_name,
                name.clone(),
                "MULTIRANGE_TYPE_NAME",
            )?,
        }
    }
    Ok(definition)
}

fn default_multirange_name(range_name: &str) -> String {
    if range_name.to_ascii_lowercase().ends_with("range") {
        format!(
            "{}multirange",
            &range_name[..range_name.len() - "range".len()]
        )
    } else {
        format!("{range_name}_multirange")
    }
}

fn paired_type_identity(name: &ObjectName, default_schema: &str) -> Result<(String, String)> {
    let parts = object_name_parts(name);
    match parts.as_slice() {
        [name] => Ok((default_schema.to_string(), name.clone())),
        [schema_name, name] => Ok((schema_name.clone(), name.clone())),
        _ => Err(SqlError::InvalidSql(format!(
            "invalid multirange type name {}",
            object_name(name)?
        ))),
    }
}

fn builtin_range_spec_for_subtype(subtype: &str) -> Option<&'static PgTypeSpec> {
    let range_type = match subtype {
        "int4" => "int4range",
        "int8" => "int8range",
        "numeric" => "numrange",
        "date" => "daterange",
        "timestamp" => "tsrange",
        "timestamptz" => "tstzrange",
        _ => return None,
    };
    pg_type_spec(range_type)
}

fn default_range_opclass_name(subtype: &str) -> &'static str {
    match subtype {
        "int4" => "int4_ops",
        "int8" => "int8_ops",
        "numeric" => "numeric_ops",
        "date" => "date_ops",
        "timestamp" => "timestamp_ops",
        "timestamptz" => "timestamptz_ops",
        _ => unreachable!("range subtype was validated"),
    }
}

fn default_range_subdiff_name(subtype: &str) -> &'static str {
    match subtype {
        "int4" => "int4range_subdiff",
        "int8" => "int8range_subdiff",
        "numeric" => "numrange_subdiff",
        "date" => "daterange_subdiff",
        "timestamp" => "tsrange_subdiff",
        "timestamptz" => "tstzrange_subdiff",
        _ => unreachable!("range subtype was validated"),
    }
}

fn range_canonical_symbol_for_subtype(subtype: &str) -> Option<&'static str> {
    match subtype {
        "int4" => Some("int4range_canonical"),
        "int8" => Some("int8range_canonical"),
        "date" => Some("daterange_canonical"),
        _ => None,
    }
}

fn set_base_type_option<T>(slot: &mut Option<T>, value: T, option: &str) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(SqlError::InvalidSql(format!(
            "CREATE TYPE option {option} specified more than once"
        )));
    }
    Ok(())
}

fn parse_base_type_definition(
    options: &[UserDefinedTypeSqlDefinitionOption],
) -> Result<BaseTypeDefinition> {
    let mut definition = BaseTypeDefinition::default();
    for option in options {
        match option {
            UserDefinedTypeSqlDefinitionOption::Input(name) => {
                set_base_type_option(&mut definition.input, relation_name(name)?, "INPUT")?
            }
            UserDefinedTypeSqlDefinitionOption::Output(name) => {
                set_base_type_option(&mut definition.output, relation_name(name)?, "OUTPUT")?
            }
            UserDefinedTypeSqlDefinitionOption::Receive(name) => {
                set_base_type_option(&mut definition.receive, relation_name(name)?, "RECEIVE")?
            }
            UserDefinedTypeSqlDefinitionOption::Send(name) => {
                set_base_type_option(&mut definition.send, relation_name(name)?, "SEND")?
            }
            UserDefinedTypeSqlDefinitionOption::InternalLength(length) => {
                let length = match length {
                    sqlparser::ast::UserDefinedTypeInternalLength::Variable => -1,
                    sqlparser::ast::UserDefinedTypeInternalLength::Fixed(length) => {
                        i64::try_from(*length).map_err(|_| {
                            SqlError::numeric_value_out_of_range(
                                "base type internal length exceeds bigint",
                            )
                        })?
                    }
                };
                set_base_type_option(&mut definition.internal_length, length, "INTERNALLENGTH")?;
            }
            UserDefinedTypeSqlDefinitionOption::PassedByValue => {
                set_base_type_option(&mut definition.passed_by_value, true, "PASSEDBYVALUE")?;
            }
            UserDefinedTypeSqlDefinitionOption::Alignment(alignment) => {
                let alignment = match alignment {
                    sqlparser::ast::Alignment::Char => 'c',
                    sqlparser::ast::Alignment::Int2 => 's',
                    sqlparser::ast::Alignment::Int4 => 'i',
                    sqlparser::ast::Alignment::Double => 'd',
                };
                set_base_type_option(&mut definition.alignment, alignment, "ALIGNMENT")?;
            }
            UserDefinedTypeSqlDefinitionOption::Storage(storage) => {
                let storage = match storage {
                    sqlparser::ast::UserDefinedTypeStorage::Plain => 'p',
                    sqlparser::ast::UserDefinedTypeStorage::External => 'e',
                    sqlparser::ast::UserDefinedTypeStorage::Extended => 'x',
                    sqlparser::ast::UserDefinedTypeStorage::Main => 'm',
                };
                set_base_type_option(&mut definition.storage, storage, "STORAGE")?;
            }
            UserDefinedTypeSqlDefinitionOption::Like(name) => set_base_type_option(
                &mut definition.like_type,
                object_name(name)?.to_ascii_lowercase(),
                "LIKE",
            )?,
            UserDefinedTypeSqlDefinitionOption::Category(category) => {
                set_base_type_option(&mut definition.category, *category, "CATEGORY")?;
            }
            UserDefinedTypeSqlDefinitionOption::Preferred(preferred) => {
                definition.preferred = *preferred;
            }
            UserDefinedTypeSqlDefinitionOption::Default(expr) => {
                set_base_type_option(&mut definition.default_expr, expr.to_string(), "DEFAULT")?
            }
            UserDefinedTypeSqlDefinitionOption::Element(data_type) => {
                let element_type = pg_type_from_data_type(data_type)?.0;
                set_base_type_option(&mut definition.element_type, element_type, "ELEMENT")?;
            }
            UserDefinedTypeSqlDefinitionOption::Delimiter(delimiter) => {
                let mut chars = delimiter.chars();
                let delimiter = chars.next().ok_or_else(|| {
                    SqlError::InvalidSql("CREATE TYPE DELIMITER cannot be empty".to_string())
                })?;
                if chars.next().is_some() {
                    return Err(SqlError::InvalidSql(
                        "CREATE TYPE DELIMITER must be a single character".to_string(),
                    ));
                }
                set_base_type_option(&mut definition.delimiter, delimiter, "DELIMITER")?;
            }
            UserDefinedTypeSqlDefinitionOption::Collatable(collatable) => {
                definition.collatable = *collatable;
            }
            UserDefinedTypeSqlDefinitionOption::TypmodIn(_)
            | UserDefinedTypeSqlDefinitionOption::TypmodOut(_)
            | UserDefinedTypeSqlDefinitionOption::Analyze(_)
            | UserDefinedTypeSqlDefinitionOption::Subscript(_) => {
                return Err(SqlError::Unsupported(format!(
                    "CREATE TYPE option {option} requires an unregistered native extension hook"
                )));
            }
        }
    }
    Ok(definition)
}

fn routine_type_matches(actual: &str, expected: &str) -> bool {
    let actual = actual.trim_matches('"').to_ascii_lowercase();
    let expected = expected.trim_matches('"').to_ascii_lowercase();
    actual == expected || actual.rsplit('.').next() == expected.rsplit('.').next()
}

fn routine_depends_on_user_type(routine: &RoutineSchema, user_type: &UserTypeSchema) -> bool {
    let identity = if user_type.schema_name == "public" {
        user_type.name.clone()
    } else {
        format!("{}.{}", user_type.schema_name, user_type.name)
    };
    routine_type_matches(&routine.return_type, &identity)
        || routine
            .arg_types
            .iter()
            .any(|argument| routine_type_matches(&argument.pg_type, &identity))
}

fn rewrite_routine_type_identity(
    declaration: &mut String,
    previous: &UserTypeSchema,
    updated: &UserTypeSchema,
) -> bool {
    let array = declaration.trim_end().ends_with("[]");
    let bare = declaration
        .trim()
        .strip_suffix("[]")
        .unwrap_or(declaration.trim());
    let previous_identity = if previous.schema_name == "public" {
        previous.name.clone()
    } else {
        format!("{}.{}", previous.schema_name, previous.name)
    };
    if !routine_type_matches(bare, &previous_identity) {
        return false;
    }
    let mut identity = if updated.schema_name == "public" {
        updated.name.clone()
    } else {
        format!("{}.{}", updated.schema_name, updated.name)
    };
    if array {
        identity.push_str("[]");
    }
    *declaration = identity;
    true
}

fn rewrite_routine_argument_type_identity(
    argument: &mut String,
    previous: &UserTypeSchema,
    updated: &UserTypeSchema,
) -> bool {
    let previous_identity = if previous.schema_name == "public" {
        previous.name.clone()
    } else {
        format!("{}.{}", previous.schema_name, previous.name)
    };
    let updated_identity = if updated.schema_name == "public" {
        updated.name.clone()
    } else {
        format!("{}.{}", updated.schema_name, updated.name)
    };
    let lowercase = argument.to_ascii_lowercase();
    let needle = previous_identity.to_ascii_lowercase();
    let Some(index) = lowercase.find(&needle) else {
        return false;
    };
    let before_is_identifier = argument[..index]
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '.'));
    let end = index + needle.len();
    let after_is_identifier = argument[end..]
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$' | '.'));
    if before_is_identifier || after_is_identifier {
        return false;
    }
    argument.replace_range(index..end, &updated_identity);
    true
}

fn base_type_uses_routine(user_type: &UserTypeSchema, routine_name: &str) -> bool {
    let UserTypeKind::Base {
        input,
        output,
        receive,
        send,
        ..
    } = &user_type.kind
    else {
        return false;
    };
    input.eq_ignore_ascii_case(routine_name)
        || output.eq_ignore_ascii_case(routine_name)
        || receive
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(routine_name))
        || send
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(routine_name))
}

fn resolve_base_codec_routine<'a>(
    db: &'a BicDb,
    routine_name: &str,
    expected_arg: &str,
    expected_return: &str,
    direction: PgInternalCodecDirection,
) -> Result<(RoutineSchema, &'static PgTypeSpec)> {
    let routine = load_routine(db, RoutineKind::Function, routine_name)?.ok_or_else(|| {
        SqlError::InvalidSql(format!("type codec function {routine_name} does not exist"))
    })?;
    if routine.language != "internal" || routine.arg_types.len() != 1 {
        return Err(SqlError::InvalidSql(format!(
            "type codec function {routine_name} must be a one-argument LANGUAGE internal function"
        )));
    }
    if !routine_type_matches(&routine.arg_types[0].pg_type, expected_arg)
        || !routine_type_matches(&routine.return_type, expected_return)
    {
        return Err(SqlError::InvalidSql(format!(
            "type codec function {routine_name} has an invalid signature"
        )));
    }
    let symbol = routine.internal_symbol.as_deref().ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "type codec function {routine_name} has no registered internal symbol"
        ))
    })?;
    let (spec, actual_direction) = pg_internal_codec_type(symbol).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "type codec function {routine_name} uses an unregistered internal symbol"
        ))
    })?;
    if actual_direction != direction {
        return Err(SqlError::InvalidSql(format!(
            "type codec function {routine_name} uses the wrong codec direction"
        )));
    }
    Ok((routine, spec))
}

#[derive(Debug)]
pub struct SqlSession<'db> {
    /// Per-statement memo for the unique arbiter backing an ON CONFLICT
    /// target. Resolving it calls `index_definitions()`, which read-locks and
    /// CLONES every index definition in the database and sorts them — and it
    /// was resolved once per input ROW, so a 100-row upsert on a store with
    /// hundreds of indexes cloned the whole index catalog hundreds of times.
    /// The answer depends only on (table, conflict columns), which are
    /// statement-invariant.
    conflict_arbiter_memo:
        std::cell::RefCell<Option<(String, Vec<String>, Option<records::UniqueConflictArbiter>)>>,
    /// Identity of the compiled routine IR whose statements are executing
    /// (`&RoutineIR as usize`), or None outside routine bodies. Keys the
    /// per-node expression-type memo: IR nodes are immutable and live as long
    /// as the routine cache holds the IR, which only changes with routine DDL
    /// (the routine collection generation is part of the memo key).
    pub(crate) current_routine_ir: Option<usize>,
    /// True while the statement being executed is owned by the current
    /// routine's IR (its AST nodes are stable for the life of the IR). Off for
    /// dynamic EXECUTE and per-frame cursor query clones. Gates memos keyed by
    /// AST node address.
    pub(crate) ir_owned_statement: bool,
    /// Bound row contexts shared by every engine this session builds; see
    /// `SqlEngine::bound_context_cache`.
    pub(crate) bound_context_cache: crate::engine::SharedBoundContextCache,
    // Raw handle to the database. Reads and write-buffering go through
    // `db_ref()` (shared); operations needing exclusive access go through
    // `db_mut()`, which errors when the session was created for shared
    // (concurrent) execution. `exclusive` records which mode this session is in.
    pub(crate) db: NonNull<BicDb>,
    pub(crate) exclusive: bool,
    // When set, an autocommit stored-procedure call leaves its transaction
    // pending instead of committing it, so a caller holding the database write
    // lock can apply it. Used by the concurrent execute-under-read-lock path.
    pub(crate) defer_commit: bool,
    // Floor for a newly begun transaction's read/conflict snapshot (a commit_seq).
    // Set to the connection's own last commit_seq so a connection reads — and
    // does not spuriously self-conflict against — its own just-committed writes
    // even while the contiguous visibility watermark lags concurrent commits.
    pub(crate) snapshot_floor: u64,
    pub(crate) _marker: PhantomData<&'db BicDb>,
    pub(crate) security_context: Option<SecurityContext>,
    pub(crate) runtime: Option<Arc<dyn SqlSessionRuntime>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) fts_limits: bicdb_core::FtsQueryLimits,
    pub(crate) tx: Option<Transaction>,
    pub(crate) savepoints: Vec<SavepointMark>,
    pub(crate) ddl_undo: Vec<DdlUndo>,
    pub(crate) settings: SqlSettings,
    pub(crate) session_gucs: Arc<HashMap<String, String>>,
    pub(crate) guc_transaction_start: Option<SqlSessionGucSnapshot>,
    pub(crate) transaction_timestamp_seconds: Option<i64>,
    pub(crate) guc_local_restore: HashMap<String, Option<String>>,
    pub(crate) guc_local_settings_restore: SqlSettingsRestore,
    pub(crate) routine_vars: Arc<BTreeMap<String, SqlValue>>,
    /// The running routine frame's slots while one of its embedded statements
    /// executes (see `RoutineSlotBinding`); `None` outside routines.
    pub(crate) routine_slots: Option<crate::engine::RoutineSlotBinding>,
    /// Compiler-signed application mutation grants keyed by relation. Ordinary
    /// pgwire sessions never populate this map.
    pub(crate) signed_mutation_grants: BTreeMap<String, MutationGrantId>,
    /// BicDB application applications historically use transaction-local `set_config`
    /// calls for PostgreSQL RLS setup. A Cell already binds stronger identity
    /// values from its verified actor, so signed application SQL may accept
    /// those calls as compatibility no-ops without granting authority to
    /// replace the host-bound security context. Ordinary SQL sessions never
    /// enable this mode.
    pub(crate) signed_application_security_guc_compatibility: bool,
    pub(crate) currvals: FxHashMap<String, i64>,
    pub(crate) schema_cache_generation: u64,
    pub(crate) schema_cache: SqlSchemaCacheMap,
    pub(crate) schema_list_cache: SqlSchemaListCacheMap,
    /// Parsed statements keyed on their literal-normalized template
    /// (`statement_cache`); bounded, cleared when full.
    pub(crate) parsed_statement_cache: FxHashMap<String, crate::statement_cache::CachedTemplate>,
    // Transaction-scoped index lookup caches (`BICDB_TXN_LOOKUP_CACHE=1`,
    // see `txn_lookup_cache_enabled`). Rebuilt whenever the owning
    // transaction id or the schema/index catalog generation changes, so a
    // stale entry can never outlive its transaction or a DDL change.
    pub(crate) txn_lookup_caches: std::cell::RefCell<Option<TxnLookupCaches>>,
    /// Current trigger nesting depth, bounding trigger-writes-table-with-
    /// triggers recursion (see session/row_triggers.rs).
    pub(crate) trigger_depth: usize,
}

#[derive(Clone, Debug, Default)]
pub struct SqlSessionGucState {
    settings: SqlSettings,
    session_gucs: Arc<HashMap<String, String>>,
    transaction_start: Option<SqlSessionGucSnapshot>,
    transaction_timestamp_seconds: Option<i64>,
    local_restore: HashMap<String, Option<String>>,
    local_settings_restore: SqlSettingsRestore,
    currvals: FxHashMap<String, i64>,
}

impl SqlSessionGucState {
    pub fn from_session_gucs(session_gucs: HashMap<String, String>) -> Self {
        let mut session_gucs = session_gucs;
        strip_protected_security_settings(&mut session_gucs);
        Self {
            session_gucs: Arc::new(session_gucs),
            ..Self::default()
        }
    }

    pub fn session_gucs(&self) -> &HashMap<String, String> {
        &self.session_gucs
    }

    pub fn insert_session_guc(&mut self, key: String, value: String) {
        if !is_protected_security_setting(&key) {
            Arc::make_mut(&mut self.session_gucs).insert(key, value);
        }
    }

    fn strip_protected_security_settings(&mut self) {
        strip_protected_security_settings(Arc::make_mut(&mut self.session_gucs));
        if let Some(snapshot) = self.transaction_start.as_mut() {
            strip_protected_security_settings(Arc::make_mut(&mut snapshot.session_gucs));
        }
        self.local_restore
            .retain(|name, _| !is_protected_security_setting(name));
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction_start.is_some()
    }

    pub fn begin_transaction(&mut self) {
        if self.transaction_start.is_none() {
            self.transaction_start = Some(SqlSessionGucSnapshot {
                settings: self.settings,
                session_gucs: self.session_gucs.clone(),
            });
            self.transaction_timestamp_seconds = Some(unix_now());
        }
    }

    pub fn commit_transaction(&mut self) {
        for (key, prior) in self.local_restore.drain() {
            match prior {
                Some(value) => {
                    Arc::make_mut(&mut self.session_gucs).insert(key, value);
                }
                None => {
                    Arc::make_mut(&mut self.session_gucs).remove(&key);
                }
            }
        }
        if let Some(vector_search) = self.local_settings_restore.vector_search.take() {
            self.settings.vector_search = vector_search;
        }
        if let Some(ef_search) = self.local_settings_restore.ef_search.take() {
            self.settings.ef_search = ef_search;
        }
        self.transaction_start = None;
        self.transaction_timestamp_seconds = None;
    }

    pub fn rollback_transaction(&mut self) {
        if let Some(start) = self.transaction_start.take() {
            let lastval = self.session_gucs.get(LASTVAL_SESSION_KEY).cloned();
            self.settings = start.settings;
            self.session_gucs = start.session_gucs;
            match lastval {
                Some(lastval) => {
                    Arc::make_mut(&mut self.session_gucs)
                        .insert(LASTVAL_SESSION_KEY.to_string(), lastval);
                }
                None => {
                    Arc::make_mut(&mut self.session_gucs).remove(LASTVAL_SESSION_KEY);
                }
            }
        }
        self.local_restore.clear();
        self.local_settings_restore = SqlSettingsRestore::default();
        self.transaction_timestamp_seconds = None;
    }

    pub fn restore_savepoint(&mut self, savepoint: &Self) {
        let lastval = self.session_gucs.get(LASTVAL_SESSION_KEY).cloned();
        self.settings = savepoint.settings;
        self.session_gucs = savepoint.session_gucs.clone();
        self.transaction_start = savepoint.transaction_start.clone();
        self.transaction_timestamp_seconds = savepoint.transaction_timestamp_seconds;
        self.local_restore = savepoint.local_restore.clone();
        self.local_settings_restore = savepoint.local_settings_restore.clone();
        match lastval {
            Some(lastval) => {
                Arc::make_mut(&mut self.session_gucs)
                    .insert(LASTVAL_SESSION_KEY.to_string(), lastval);
            }
            None => {
                Arc::make_mut(&mut self.session_gucs).remove(LASTVAL_SESSION_KEY);
            }
        }
    }

    /// Conservatively estimates memory owned by this opaque state, including
    /// effective values, transaction-start snapshots, and local restore data.
    pub fn memory_estimate(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(string_map_memory_estimate(&self.session_gucs))
            .saturating_add(
                self.transaction_start
                    .as_ref()
                    .map(SqlSessionGucSnapshot::memory_estimate)
                    .unwrap_or_default(),
            )
            .saturating_add(optional_string_map_memory_estimate(&self.local_restore))
            .saturating_add(sequence_value_map_memory_estimate(&self.currvals))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SqlSessionGucSnapshot {
    settings: SqlSettings,
    session_gucs: Arc<HashMap<String, String>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SqlSettingsRestore {
    vector_search: Option<VectorSearchMode>,
    ef_search: Option<usize>,
}

#[derive(Clone, Debug)]
enum CompositeAttributeMutation {
    Add(CompositeAttributeSchema),
    Drop(usize),
    Rename {
        index: usize,
        name: String,
    },
    AlterType {
        index: usize,
        attribute: CompositeAttributeSchema,
    },
    RenameType {
        name: String,
    },
}

fn composite_attribute_from_column_schema(column: ColumnSchema) -> CompositeAttributeSchema {
    CompositeAttributeSchema {
        name: column.name,
        pg_type: column.pg_type,
        user_type: column.user_type,
        collation: column.collation,
        type_modifier: column.type_modifier,
        array_ndims: column.array_ndims,
        dropped: false,
    }
}

fn replace_user_type_kind_references(
    kind: &mut UserTypeKind,
    replacements: &BTreeMap<i64, UserTypeSchema>,
) -> bool {
    match kind {
        UserTypeKind::Shell | UserTypeKind::Base { .. } | UserTypeKind::Enum { .. } => false,
        UserTypeKind::Range { value, .. } | UserTypeKind::Multirange { value, .. } => value
            .subtype_user_type
            .as_deref_mut()
            .is_some_and(|column| replace_user_type_column_reference(column, replacements)),
        UserTypeKind::Domain { base_user_type, .. } => base_user_type
            .as_deref_mut()
            .is_some_and(|column| replace_user_type_column_reference(column, replacements)),
        UserTypeKind::Composite { attributes, .. } => {
            let mut changed = false;
            for attribute in attributes.iter_mut().filter(|attribute| !attribute.dropped) {
                let Some(column) = &mut attribute.user_type else {
                    continue;
                };
                if replace_user_type_column_reference(column, replacements) {
                    attribute.pg_type = column.formatted_name();
                    changed = true;
                }
            }
            changed
        }
    }
}

fn user_type_kind_depends_on_oid(kind: &UserTypeKind, oid: i64) -> bool {
    match kind {
        UserTypeKind::Shell | UserTypeKind::Base { .. } | UserTypeKind::Enum { .. } => false,
        UserTypeKind::Range { value, .. } => value
            .subtype_user_type
            .as_deref()
            .is_some_and(|column| user_type_column_depends_on_oid(column, oid)),
        UserTypeKind::Multirange {
            value, range_oid, ..
        } => {
            *range_oid == oid
                || value
                    .subtype_user_type
                    .as_deref()
                    .is_some_and(|column| user_type_column_depends_on_oid(column, oid))
        }
        UserTypeKind::Domain { base_user_type, .. } => base_user_type
            .as_deref()
            .is_some_and(|column| user_type_column_depends_on_oid(column, oid)),
        UserTypeKind::Composite { attributes, .. } => attributes.iter().any(|attribute| {
            !attribute.dropped
                && attribute
                    .user_type
                    .as_ref()
                    .is_some_and(|column| user_type_column_depends_on_oid(column, oid))
        }),
    }
}

fn user_type_column_depends_on_oid(column: &UserTypeColumnSchema, oid: i64) -> bool {
    column.oid == oid || user_type_kind_depends_on_oid(&column.kind, oid)
}

fn replace_user_type_column_reference(
    column: &mut UserTypeColumnSchema,
    replacements: &BTreeMap<i64, UserTypeSchema>,
) -> bool {
    if let Some(replacement) = replacements.get(&column.oid) {
        let next = replacement.column_type(column.array);
        if *column != next {
            *column = next;
            return true;
        }
        return false;
    }
    replace_user_type_kind_references(&mut column.kind, replacements)
}

fn replace_column_user_type_references(
    column: &mut ColumnSchema,
    replacements: &BTreeMap<i64, UserTypeSchema>,
) -> bool {
    let Some(user_type) = &mut column.user_type else {
        return false;
    };
    if !replace_user_type_column_reference(user_type, replacements) {
        return false;
    }
    column.pg_type = user_type.formatted_name();
    true
}

fn rewrite_composite_value(
    value: SqlValue,
    target_oid: i64,
    target_name: &str,
    mutation: &CompositeAttributeMutation,
) -> Result<SqlValue> {
    match value {
        SqlValue::Composite(mut composite) => {
            for field in &mut composite.fields {
                field.value = rewrite_composite_value(
                    std::mem::replace(&mut field.value, SqlValue::Null),
                    target_oid,
                    target_name,
                    mutation,
                )?;
            }
            let is_target = composite.type_oid == u32::try_from(target_oid).ok()
                || composite.type_name.eq_ignore_ascii_case(target_name);
            if is_target {
                match mutation {
                    CompositeAttributeMutation::Add(attribute) => {
                        composite.fields.push(SqlCompositeField {
                            name: attribute.name.clone(),
                            pg_type: attribute.pg_type.clone(),
                            value: SqlValue::Null,
                        });
                    }
                    CompositeAttributeMutation::Drop(index) => {
                        if *index < composite.fields.len() {
                            composite.fields.remove(*index);
                        }
                    }
                    CompositeAttributeMutation::Rename { index, name } => {
                        if let Some(field) = composite.fields.get_mut(*index) {
                            field.name = name.clone();
                        }
                    }
                    CompositeAttributeMutation::AlterType { index, attribute } => {
                        if let Some(field) = composite.fields.get_mut(*index) {
                            field.pg_type = attribute.pg_type.clone();
                        }
                    }
                    CompositeAttributeMutation::RenameType { name } => {
                        composite.type_name = name.clone();
                    }
                }
            }
            Ok(SqlValue::Composite(composite))
        }
        SqlValue::Json(value) => {
            if let Some(composite) = pg_composite_from_array_json(&value) {
                return rewrite_composite_value(
                    SqlValue::Composite(composite),
                    target_oid,
                    target_name,
                    mutation,
                )
                .map(composite_array_element_json)
                .map(SqlValue::Json);
            }
            match value {
                JsonValue::Array(values) => values
                    .into_iter()
                    .map(|value| {
                        rewrite_composite_value(
                            SqlValue::Json(value),
                            target_oid,
                            target_name,
                            mutation,
                        )
                        .map(|value| match value {
                            SqlValue::Json(value) => value,
                            value => sql_value_to_json(value),
                        })
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(JsonValue::Array)
                    .map(SqlValue::Json),
                JsonValue::Object(values) => values
                    .into_iter()
                    .map(|(key, value)| {
                        rewrite_composite_value(
                            SqlValue::Json(value),
                            target_oid,
                            target_name,
                            mutation,
                        )
                        .map(|value| {
                            (
                                key,
                                match value {
                                    SqlValue::Json(value) => value,
                                    value => sql_value_to_json(value),
                                },
                            )
                        })
                    })
                    .collect::<Result<serde_json::Map<_, _>>>()
                    .map(JsonValue::Object)
                    .map(SqlValue::Json),
                value => Ok(SqlValue::Json(value)),
            }
        }
        value => Ok(value),
    }
}

impl SqlSessionGucSnapshot {
    fn memory_estimate(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(string_map_memory_estimate(&self.session_gucs))
    }
}

fn conservative_hash_map_table_memory_estimate<K, V>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    // HashMap does not expose its bucket/control allocation. Two buckets per
    // reported capacity plus one control byte per bucket is conservative.
    capacity
        .saturating_mul(2)
        .saturating_mul(std::mem::size_of::<(K, V)>().saturating_add(1))
}

fn string_map_memory_estimate(map: &HashMap<String, String>) -> usize {
    map.iter().fold(
        conservative_hash_map_table_memory_estimate::<String, String>(map.capacity()),
        |estimate, (key, value)| {
            estimate
                .saturating_add(key.capacity())
                .saturating_add(value.capacity())
        },
    )
}

fn optional_string_map_memory_estimate(map: &HashMap<String, Option<String>>) -> usize {
    map.iter().fold(
        conservative_hash_map_table_memory_estimate::<String, Option<String>>(map.capacity()),
        |estimate, (key, value)| {
            estimate
                .saturating_add(key.capacity())
                .saturating_add(value.as_ref().map(String::capacity).unwrap_or_default())
        },
    )
}

fn sequence_value_map_memory_estimate(map: &FxHashMap<String, i64>) -> usize {
    map.iter().fold(
        conservative_hash_map_table_memory_estimate::<String, i64>(map.capacity()),
        |estimate, (key, _)| estimate.saturating_add(key.capacity()),
    )
}

#[derive(Clone, Debug, Default)]
struct GucAssignmentTargets {
    gucs: BTreeSet<String>,
    vector_search: bool,
    ef_search: bool,
}

impl GucAssignmentTargets {
    fn setting(setting: &str) -> Self {
        match setting {
            "bicdb.vector_search" => Self {
                vector_search: true,
                ..Self::default()
            },
            "bicdb.ef_search" => Self {
                ef_search: true,
                ..Self::default()
            },
            SESSION_AUTHORIZATION_GUC => Self {
                gucs: BTreeSet::from([
                    SESSION_AUTHORIZATION_GUC.to_string(),
                    CURRENT_ROLE_GUC.to_string(),
                ]),
                ..Self::default()
            },
            _ => Self {
                gucs: BTreeSet::from([setting.to_string()]),
                ..Self::default()
            },
        }
    }

    fn reset_all(local_restore: &HashMap<String, Option<String>>) -> Self {
        let preserved = [
            SESSION_AUTHORIZATION_GUC,
            CURRENT_ROLE_GUC,
            INITIAL_SESSION_AUTHORIZATION_GUC,
        ];
        Self {
            gucs: local_restore
                .keys()
                .filter(|key| !preserved.contains(&key.as_str()))
                .cloned()
                .collect(),
            vector_search: true,
            ef_search: true,
        }
    }

    fn clear_local_restore(self, session: &mut SqlSession<'_>) {
        for key in self.gucs {
            session.guc_local_restore.remove(&key);
        }
        if self.vector_search {
            session.guc_local_settings_restore.vector_search = None;
        }
        if self.ef_search {
            session.guc_local_settings_restore.ef_search = None;
        }
    }
}

/// Committed-state index lookup caches pinned to one transaction. The caches
/// hold only committed index state (pending-write overlays are applied by the
/// consumers after the cache probe), so within one transaction they are
/// invalidation-free; the stamps below guard the transaction and DDL
/// boundaries.
#[derive(Debug)]
pub(crate) struct TxnLookupCaches {
    tx_id: bicdb_core::TransactionId,
    schema_generation: u64,
    index_catalog_len: usize,
    index_lookup_cache: IndexLookupCache,
    rowid_index_lookup_cache: RowIdIndexLookupCache,
}

#[derive(Clone, Debug, Default)]
pub struct SqlSessionCatalogCache {
    pub(crate) schema_cache_generation: u64,
    pub(crate) schema_cache: SqlSchemaCacheMap,
    pub(crate) schema_list_cache: SqlSchemaListCacheMap,
    /// Parsed statements keyed on their literal-normalized template. pgwire
    /// builds a session per statement, so the cache lives here, per
    /// connection, or it would be empty on every CALL.
    pub(crate) parsed_statement_cache: FxHashMap<String, crate::statement_cache::CachedTemplate>,
}

impl SqlSessionCatalogCache {
    #[doc(hidden)]
    pub fn schema_cache_len(&self) -> usize {
        self.schema_cache.values().map(FxHashMap::len).sum()
    }
}

#[derive(Debug)]
struct RawCreateMemoryIndex {
    index_name: String,
    table: String,
    field: String,
    model: Option<String>,
    mode: Option<String>,
}

#[derive(Debug)]
struct RawSimilarToSelect {
    columns: String,
    table: String,
    field: String,
    query: String,
    limit: usize,
}

#[derive(Debug)]
struct RawCreateMemoryTable {
    table: String,
    fields: Vec<String>,
    table_sql: String,
}

const SQL_IDENTIFIER_PATTERN: &str = r#"(?:[A-Za-z_][A-Za-z0-9_]*|"(?:[^"]|"")+")"#;

/// ASCII case-insensitive substring test — the pre-screen that keeps the
/// raw-statement dispatchers from even running their (memoized) regexes on
/// ordinary statements.
fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() || haystack.len() < needle.len() {
        return needle.is_empty();
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn parse_raw_create_memory_index(sql: &str) -> Result<Option<RawCreateMemoryIndex>> {
    // Compiling these dispatch regexes per statement was 52% of a selective
    // ranked query's wall time (measured with perf): every `execute` probed
    // the raw text against them. Compile once; a cheap substring screen
    // skips even the (fast) match for the overwhelmingly common statements.
    if !contains_ascii_case_insensitive(sql, "MEMORY") {
        return Ok(None);
    }
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        let pattern = format!(
            r#"^\s*CREATE\s+MEMORY\s+INDEX\s+ON\s+({ident})\s*\(\s*({ident})\s*\)\s*(?:WITH\s*\((.*)\))?\s*;?\s*$"#,
            ident = SQL_IDENTIFIER_PATTERN
        );
        RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .dot_matches_new_line(true)
            .build()
            .expect("static dispatch pattern compiles")
    });
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let table = unquote_sql_identifier(captures.get(1).unwrap().as_str());
    let field = unquote_sql_identifier(captures.get(2).unwrap().as_str());
    let options = captures
        .get(3)
        .map(|options| parse_memory_index_options(options.as_str()))
        .transpose()?
        .unwrap_or_default();
    Ok(Some(RawCreateMemoryIndex {
        index_name: format!("idx_{table}_{field}_memory"),
        table,
        field,
        model: options.get("model").cloned(),
        mode: options.get("mode").map(|mode| mode.to_ascii_lowercase()),
    }))
}

fn parse_raw_create_memory_table(sql: &str) -> Result<Option<RawCreateMemoryTable>> {
    if !contains_ascii_case_insensitive(sql, "CREATE") {
        return Ok(None);
    }
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        let pattern = format!(
            r#"^\s*(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)({ident})(\s*)\((.*)\)\s*;?\s*$"#,
            ident = SQL_IDENTIFIER_PATTERN
        );
        RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .dot_matches_new_line(true)
            .build()
            .expect("static dispatch pattern compiles")
    });
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let prefix = captures.get(1).unwrap().as_str();
    let table_token = captures.get(2).unwrap().as_str();
    let spacing = captures.get(3).unwrap().as_str();
    let body = captures.get(4).unwrap().as_str();
    let mut fields = Vec::new();
    let mut rewritten_parts = Vec::new();
    for part in split_top_level_commas_nested(body) {
        let Some((column_token, rest)) = split_first_ident(&part) else {
            rewritten_parts.push(part);
            continue;
        };
        let first = unquote_sql_identifier(column_token).to_ascii_lowercase();
        if matches!(
            first.as_str(),
            "constraint" | "primary" | "foreign" | "unique" | "check" | "exclude"
        ) {
            rewritten_parts.push(part);
            continue;
        }
        let (rewritten_rest, memory_column) = strip_top_level_memory_keyword(rest);
        if memory_column {
            fields.push(unquote_sql_identifier(column_token));
            rewritten_parts.push(format!("{column_token}{rewritten_rest}"));
        } else {
            rewritten_parts.push(part);
        }
    }
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(RawCreateMemoryTable {
        table: unquote_sql_identifier(table_token),
        fields,
        table_sql: format!(
            "{prefix}{table_token}{spacing}({})",
            rewritten_parts.join(", ")
        ),
    }))
}

fn parse_memory_index_options(options: &str) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for part in options.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            return Err(SqlError::InvalidSql(format!(
                "invalid memory index option `{}`",
                part.trim()
            )));
        };
        parsed.insert(key.trim().to_ascii_lowercase(), unquote_sql_string(value)?);
    }
    Ok(parsed)
}

fn parse_raw_process_memory_jobs(sql: &str) -> Result<Option<usize>> {
    if !contains_ascii_case_insensitive(sql, "bicdb_process_memory_jobs") {
        return Ok(None);
    }
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        RegexBuilder::new(r"^\s*SELECT\s+bicdb_process_memory_jobs\s*\(\s*(\d*)\s*\)\s*;?\s*$")
            .case_insensitive(true)
            .build()
            .expect("static dispatch pattern compiles")
    });
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let limit = captures
        .get(1)
        .map(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<usize>().map_err(|_| {
                SqlError::InvalidSql(
                    "bicdb_process_memory_jobs limit must be an integer".to_string(),
                )
            })
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    Ok(Some(limit))
}

fn parse_raw_similar_to_select(sql: &str) -> Result<Option<RawSimilarToSelect>> {
    if !contains_ascii_case_insensitive(sql, "SIMILAR_TO") {
        return Ok(None);
    }
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        let pattern = format!(
            r#"^\s*SELECT\s+(.+?)\s+FROM\s+({ident})\s+ORDER\s+BY\s+SIMILAR_TO\s*\(\s*({ident})\s*,\s*('(?:''|[^'])*')\s*\)\s+LIMIT\s+(\d+)\s*;?\s*$"#,
            ident = SQL_IDENTIFIER_PATTERN
        );
        RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .dot_matches_new_line(true)
            .build()
            .expect("static dispatch pattern compiles")
    });
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    Ok(Some(RawSimilarToSelect {
        columns: captures.get(1).unwrap().as_str().trim().to_string(),
        table: unquote_sql_identifier(captures.get(2).unwrap().as_str()),
        field: unquote_sql_identifier(captures.get(3).unwrap().as_str()),
        query: unquote_sql_string(captures.get(4).unwrap().as_str())?,
        limit: captures
            .get(5)
            .unwrap()
            .as_str()
            .parse::<usize>()
            .map_err(|_| SqlError::InvalidSql("LIMIT must be an integer".to_string()))?,
    }))
}

fn strip_top_level_memory_keyword(value: &str) -> (String, bool) {
    let mut depth = 0_i32;
    let mut in_string = false;
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_string {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && (idx == 0
                    || value[..idx]
                        .chars()
                        .next_back()
                        .map(|ch| !is_sql_ident_char(ch))
                        .unwrap_or(true))
                && starts_with_memory_keyword(&value[idx..]) =>
            {
                let end = idx + "memory".len();
                let mut rewritten = String::new();
                rewritten.push_str(value[..idx].trim_end());
                if !rewritten.is_empty() && !value[end..].trim_start().is_empty() {
                    rewritten.push(' ');
                }
                rewritten.push_str(value[end..].trim_start());
                return (rewritten, true);
            }
            _ => {}
        }
        idx += 1;
    }
    (value.to_string(), false)
}

fn starts_with_memory_keyword(value: &str) -> bool {
    let Some(prefix) = value.get(.."memory".len()) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case("memory") {
        return false;
    }
    let after_ok = value
        .get("memory".len()..)
        .and_then(|rest| rest.chars().next())
        .map(|ch| !is_sql_ident_char(ch))
        .unwrap_or(true);
    after_ok
}

fn is_sql_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn unquote_sql_string(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        Ok(trimmed[1..trimmed.len() - 1].replace("''", "'"))
    } else {
        Ok(trimmed.to_string())
    }
}

fn unquote_sql_identifier(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}

fn normalize_select_column_identifier(value: &str) -> String {
    let trimmed = value.trim();
    let unqualified = trimmed.rsplit('.').next().unwrap_or(trimmed);
    unquote_sql_identifier(unqualified)
}

#[derive(Clone, Debug, Default)]
pub struct SqlSessionDdlUndoLog {
    entries: Vec<DdlUndo>,
}

impl SqlSessionDdlUndoLog {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum DdlUndo {
    RestoreRole {
        name: String,
        previous: Option<RoleSchema>,
    },
    RestoreMembership {
        role: String,
        member: String,
        previous: Option<RoleMembership>,
    },
    DropCollection {
        name: String,
    },
    RestoreCollection {
        name: String,
        records: Vec<Record>,
    },
    RestoreTableState {
        table: String,
        schema: TableSchema,
        records: Vec<Record>,
    },
    RestoreRenamedTable {
        old_table: String,
        new_table: String,
        records: Vec<Record>,
        indexes: Vec<IndexDefinition>,
        schema: TableSchema,
        sequences: Vec<SequenceSchema>,
    },
    DropIndex {
        name: String,
    },
    RestoreIndex {
        definition: IndexDefinition,
    },
    RenameIndex {
        old_name: String,
        new_name: String,
    },
    DeleteSchema {
        table: String,
    },
    RestoreSchema {
        schema: TableSchema,
    },
    DeleteSequence {
        sequence: String,
    },
    RestoreSequence {
        sequence: SequenceSchema,
    },
    DeleteView {
        view: String,
    },
    RestoreView {
        view: ViewSchema,
    },
    DeleteTrigger {
        table: String,
        trigger: String,
    },
    RestoreTrigger {
        trigger: TriggerSchema,
    },
    DeleteUserType {
        schema_name: String,
        name: String,
    },
    RestoreUserType {
        user_type: UserTypeSchema,
    },
    DeletePrivilege {
        grant: PrivilegeGrant,
    },
    RestorePrivilege {
        grant: PrivilegeGrant,
    },
    DeleteDefaultPrivilege {
        grant: DefaultPrivilegeGrant,
    },
    RestoreDefaultPrivilege {
        grant: DefaultPrivilegeGrant,
    },
    DeleteRoutine {
        kind: RoutineKind,
        name: String,
    },
    RestoreRoutine {
        routine: RoutineSchema,
    },
    DeleteNamespace {
        namespace: String,
    },
    RestoreNamespace {
        namespace: NamespaceSchema,
    },
    DeleteExtensionInstallation {
        name: String,
    },
    DeleteLegacyExtension {
        name: String,
    },
    RestoreExtensionInstallation {
        installation: ExtensionInstallation,
    },
    RestoreLegacyExtension {
        extension: ExtensionSchema,
    },
    DeleteExtensionResource {
        name: String,
    },
    RestoreExtensionResource {
        resource: RestResourceDefinition,
    },
    DeleteExtensionEventBinding {
        name: String,
    },
    RestoreExtensionEventBinding {
        binding: EventBindingDefinition,
    },
    DeleteExtensionWebsite {
        name: String,
    },
    RestoreExtensionWebsite {
        website: WebsiteDefinition,
    },
    DeleteExtensionWebsiteRelease {
        website: String,
        version: String,
    },
    RestoreExtensionWebsiteRelease {
        release: WebsiteRelease,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct SavepointMark {
    pub(crate) name: String,
    pub(crate) transaction: bicdb_core::TransactionRollbackMark,
    pub(crate) ddl_undo_len: usize,
    pub(crate) settings: SqlSettings,
    pub(crate) session_gucs: Arc<HashMap<String, String>>,
    pub(crate) guc_local_restore: HashMap<String, Option<String>>,
    pub(crate) guc_local_settings_restore: SqlSettingsRestore,
}

pub(crate) fn user_type_identity(name: &ObjectName) -> Result<(String, String)> {
    let parts = object_name_parts(name);
    match parts.as_slice() {
        [name] => Ok(("public".to_string(), name.clone())),
        [schema_name, name] => Ok((schema_name.clone(), name.clone())),
        _ => Err(SqlError::InvalidSql(format!(
            "invalid user-defined type name {}",
            object_name(name)?
        ))),
    }
}

fn domain_not_null_marker_name(name: &Ident) -> Option<Option<String>> {
    if name.value == DOMAIN_NOT_NULL_MARKER {
        return Some(None);
    }
    name.value
        .strip_prefix(&format!("{DOMAIN_NOT_NULL_MARKER}::"))
        .map(|name| Some(name.to_string()))
}

pub(crate) fn rename_enum_label_in_value(value: &mut SqlValue, from: &str, to: &str) -> bool {
    fn rename_json(value: &mut JsonValue, from: &str, to: &str) -> bool {
        match value {
            JsonValue::String(label) if label == from => {
                *label = to.to_string();
                true
            }
            JsonValue::Array(values) => values.iter_mut().fold(false, |changed, value| {
                rename_json(value, from, to) || changed
            }),
            JsonValue::Object(object) => object
                .get_mut("$bicdb_array_input")
                .and_then(JsonValue::as_object_mut)
                .and_then(|input| input.get_mut("value"))
                .is_some_and(|value| rename_json(value, from, to)),
            _ => false,
        }
    }

    match value {
        SqlValue::String(label) if label == from => {
            *label = to.to_string();
            true
        }
        SqlValue::Json(value) => rename_json(value, from, to),
        _ => false,
    }
}

fn column_assignment_cast_allowed(source: &ColumnSchema, target: &ColumnSchema) -> bool {
    if source.type_oid() == target.type_oid() {
        return true;
    }
    let source = column_assignment_cast_type(source);
    let target = column_assignment_cast_type(target);
    pg_cast_allows(&source, &target, PgCastContext::Assignment)
}

fn column_assignment_cast_type(column: &ColumnSchema) -> String {
    let Some(user_type) = column.user_type.as_ref() else {
        return column.pg_type.clone();
    };
    user_type_assignment_cast_type(user_type)
}

fn user_type_assignment_cast_type(user_type: &UserTypeColumnSchema) -> String {
    if user_type.array {
        let mut scalar = user_type.clone();
        scalar.array = false;
        return format!("{}[]", user_type_assignment_cast_type(&scalar));
    }
    match &user_type.kind {
        UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        } => base_user_type
            .as_deref()
            .map(user_type_assignment_cast_type)
            .unwrap_or_else(|| base_type.clone()),
        _ => user_type.formatted_name(),
    }
}

fn column_type_change_requires_rewrite(source: &ColumnSchema, target: &ColumnSchema) -> bool {
    if source.type_oid() != target.type_oid() {
        return true;
    }
    if source.type_modifier == target.type_modifier && source.vector_dim == target.vector_dim {
        return false;
    }
    let metadata_only = match (&source.type_modifier, &target.type_modifier) {
        (
            Some(PgTypeModifier::Character { length: source }),
            Some(PgTypeModifier::Character { length: target }),
        ) => target >= source,
        (
            Some(PgTypeModifier::Numeric {
                precision: source_precision,
                scale: source_scale,
            }),
            Some(PgTypeModifier::Numeric {
                precision: target_precision,
                scale: target_scale,
            }),
        ) => target_precision >= source_precision && target_scale == source_scale,
        _ => false,
    };
    !metadata_only
}

fn sql_contains_identifier(sql: &str, identifier: &str) -> bool {
    replace_identifier_token(sql, identifier, "__bicdb_dependency_probe") != sql
}

fn validate_foreign_key_type_pair(
    constraint: &str,
    local_name: &str,
    local: &ColumnSchema,
    referred_name: &str,
    referred: &ColumnSchema,
) -> Result<()> {
    let local_type = column_assignment_cast_type(local);
    let referred_type = column_assignment_cast_type(referred);
    if local.type_oid() == referred.type_oid()
        || pg_cast_allows(&local_type, &referred_type, PgCastContext::Implicit)
        || pg_cast_allows(&referred_type, &local_type, PgCastContext::Implicit)
    {
        return Ok(());
    }
    Err(SqlError::data_exception(
        "42804",
        format!(
            "foreign key constraint \"{constraint}\" cannot be implemented: key columns \"{local_name}\" and \"{referred_name}\" are of incompatible types: {} and {}",
            local.formatted_pg_type(),
            referred.formatted_pg_type()
        ),
        Some(constraint.to_string()),
    ))
}

impl<'db> SqlSession<'db> {
    pub fn new(db: &'db mut BicDb) -> Self {
        let schema_cache_generation = db.collection_generation(SCHEMA_COLLECTION);
        Self::from_parts(NonNull::from(db), true, None, schema_cache_generation)
    }

    /// A session that holds NO authority.
    ///
    /// **The absence of a security context must never increase privilege.**
    /// A session with no identity resolves its user through the GUC chain
    /// to the bootstrap role, which `current_user_is_superuser` treats as
    /// superuser and which owns every table by default — so "forgot to set
    /// a context" silently meant "full administrative access, RLS
    /// bypassed". That is the correct default for the embedded API, whose
    /// caller already owns the database it is holding; it is exactly wrong
    /// for a network service that builds a session per request.
    ///
    /// This constructor seeds the identity GUCs with `role`, so every
    /// existing check — superuser tests, table ownership, RLS
    /// owner-bypass, GRANT lookups — evaluates against a real, ordinary
    /// role instead of the bootstrap identity. Nothing new to keep in
    /// sync: the authority machinery is the same, it is simply given
    /// someone unprivileged to be.
    pub fn new_unprivileged(db: &'db mut BicDb, role: &str) -> Self {
        let mut session = Self::new(db);
        session.assume_unprivileged_role(role);
        session
    }

    /// Shared-access sibling of [`Self::new_unprivileged`].
    pub fn new_shared_unprivileged(db: &'db BicDb, role: &str) -> Self {
        let mut session = Self::new_shared(db);
        session.assume_unprivileged_role(role);
        session
    }

    fn assume_unprivileged_role(&mut self, role: &str) {
        let role = normalize_role_name(role);
        // Refuse to hand out the bootstrap identity through a constructor
        // whose entire purpose is to withhold it.
        let role = if role.is_empty() || role == BOOTSTRAP_ROLE_NAME {
            "bicdb_restricted".to_string()
        } else {
            role
        };
        Arc::make_mut(&mut self.session_gucs)
            .insert(SESSION_AUTHORIZATION_GUC.to_string(), role.clone());
        Arc::make_mut(&mut self.session_gucs)
            .insert(INITIAL_SESSION_AUTHORIZATION_GUC.to_string(), role.clone());
        Arc::make_mut(&mut self.session_gucs).insert(CURRENT_ROLE_GUC.to_string(), role);
    }

    pub fn new_secure(db: &'db mut BicDb, ctx: SecurityContext) -> Self {
        let schema_cache_generation = db.collection_generation(SCHEMA_COLLECTION);
        Self::from_parts(NonNull::from(db), true, Some(ctx), schema_cache_generation)
    }

    /// Creates a session that executes against a shared `&BicDb` for concurrent
    /// execution. Operations that need exclusive access (`db_mut`) return an
    /// error so the caller can fall back to an exclusive session.
    pub fn new_shared(db: &'db BicDb) -> Self {
        let schema_cache_generation = db.collection_generation(SCHEMA_COLLECTION);
        Self::from_parts(NonNull::from(db), false, None, schema_cache_generation)
    }

    /// Creates a shared session with a security context. See [`Self::new_shared`].
    pub fn new_shared_secure(db: &'db BicDb, ctx: SecurityContext) -> Self {
        let schema_cache_generation = db.collection_generation(SCHEMA_COLLECTION);
        Self::from_parts(NonNull::from(db), false, Some(ctx), schema_cache_generation)
    }

    pub(crate) fn from_parts(
        db: NonNull<BicDb>,
        exclusive: bool,
        security_context: Option<SecurityContext>,
        schema_cache_generation: u64,
    ) -> Self {
        let mut session = Self {
            conflict_arbiter_memo: std::cell::RefCell::new(None),
            current_routine_ir: None,
            ir_owned_statement: false,
            bound_context_cache: Rc::new(RefCell::new(BTreeMap::new())),
            routine_slots: None,
            trigger_depth: 0,
            db,
            exclusive,
            defer_commit: false,
            snapshot_floor: 0,
            _marker: PhantomData,
            security_context,
            runtime: None,
            cancellation: CancellationToken::uncancelable(),
            fts_limits: bicdb_core::FtsQueryLimits::UNLIMITED,
            tx: None,
            savepoints: Vec::new(),
            ddl_undo: Vec::new(),
            settings: SqlSettings::default(),
            session_gucs: Arc::new(HashMap::new()),
            guc_transaction_start: None,
            transaction_timestamp_seconds: None,
            guc_local_restore: HashMap::new(),
            guc_local_settings_restore: SqlSettingsRestore::default(),
            routine_vars: Arc::new(BTreeMap::new()),
            signed_mutation_grants: BTreeMap::new(),
            signed_application_security_guc_compatibility: false,
            currvals: FxHashMap::default(),
            schema_cache_generation,
            schema_cache: SqlSchemaCacheMap::default(),
            schema_list_cache: BTreeMap::new(),
            parsed_statement_cache: FxHashMap::default(),
            txn_lookup_caches: std::cell::RefCell::new(None),
        };
        session.rebind_trusted_security_settings();
        session
    }

    /// Shared access to the database. Sound while the caller holds at least a
    /// read lock on the database (the pgwire layer guarantees this for the
    /// lifetime `'db`).
    pub(crate) fn db_ref(&self) -> &BicDb {
        // Safety: the session borrows the database for `'db` (see `_marker`),
        // and only hands out shared references here.
        unsafe { self.db.as_ref() }
    }

    /// Exclusive access to the database. Errors for shared sessions so that
    /// operations requiring `&mut BicDb` cannot alias under concurrent
    /// execution; the caller retries under an exclusive session instead.
    pub(crate) fn db_mut(&mut self) -> Result<&mut BicDb> {
        if !self.exclusive {
            return Err(SqlError::Unsupported(
                "operation requires exclusive database access".to_string(),
            ));
        }
        // Safety: `exclusive` sessions are created from a `&'db mut BicDb`, so
        // this is the only live reference to the database for `'db`.
        Ok(unsafe { self.db.as_mut() })
    }

    pub fn in_transaction(&self) -> bool {
        self.tx.is_some()
    }

    pub fn with_fts_limits(mut self, limits: bicdb_core::FtsQueryLimits) -> Self {
        self.fts_limits = limits;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<dyn SqlSessionRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Replaces the trusted identity attached by the host. This is intentionally
    /// a Rust API, not a SQL operation. Pgwire uses it to bind the independently
    /// authenticated identity of each physical connection.
    pub fn with_security_context(mut self, security_context: Option<SecurityContext>) -> Self {
        self.security_context = security_context;
        self.rebind_trusted_security_settings();
        self
    }

    pub fn with_session_gucs(mut self, session_gucs: HashMap<String, String>) -> Self {
        self.session_gucs = Arc::new(session_gucs);
        self.rebind_trusted_security_settings();
        self
    }

    pub fn with_session_guc_state(mut self, state: SqlSessionGucState) -> Self {
        self.install_session_guc_state(state);
        self
    }

    fn install_session_guc_state(&mut self, state: SqlSessionGucState) {
        self.settings = state.settings;
        self.session_gucs = state.session_gucs;
        self.guc_transaction_start = state.transaction_start;
        self.transaction_timestamp_seconds = state.transaction_timestamp_seconds;
        self.guc_local_restore = state.local_restore;
        self.guc_local_settings_restore = state.local_settings_restore;
        self.currvals = state.currvals;
        self.rebind_trusted_security_settings();
    }

    fn take_session_guc_state(&mut self) -> SqlSessionGucState {
        let preserve_postgres_compatibility_gucs = self.security_context.is_none()
            && postgres_compatibility_gucs_enabled(&self.session_gucs);
        let mut state = SqlSessionGucState {
            settings: self.settings,
            session_gucs: std::mem::take(&mut self.session_gucs),
            transaction_start: self.guc_transaction_start.take(),
            transaction_timestamp_seconds: self.transaction_timestamp_seconds.take(),
            local_restore: std::mem::take(&mut self.guc_local_restore),
            local_settings_restore: std::mem::take(&mut self.guc_local_settings_restore),
            currvals: std::mem::take(&mut self.currvals),
        };
        if !preserve_postgres_compatibility_gucs {
            state.strip_protected_security_settings();
        }
        state
    }

    pub(crate) fn postgres_compatibility_security_gucs_enabled(&self) -> bool {
        self.security_context.is_none() && postgres_compatibility_gucs_enabled(&self.session_gucs)
    }

    fn rebind_trusted_security_settings(&mut self) {
        let preserve_postgres_compatibility_gucs =
            self.postgres_compatibility_security_gucs_enabled();
        // The shared form leaves the `Arc` untouched when the binding is a
        // no-op (no host-bound context, no protected keys) — the common
        // pgwire statement, which used to deep-clone the connection's GUC map
        // here three times per statement (construction, security context,
        // GUC state install).
        let context = self.security_context.as_ref();
        self.session_gucs =
            bind_trusted_security_settings_shared(Arc::clone(&self.session_gucs), context);
        if let Some(snapshot) = self.guc_transaction_start.as_mut() {
            snapshot.session_gucs =
                bind_trusted_security_settings_shared(Arc::clone(&snapshot.session_gucs), context);
        }
        if !preserve_postgres_compatibility_gucs {
            self.guc_local_restore
                .retain(|name, _| !is_protected_security_setting(name));
        }
    }

    /// Enables deferred-commit mode: an autocommit stored-procedure call leaves
    /// its transaction pending so a caller holding the database write lock can
    /// apply it. Intended for concurrent execute-under-read-lock execution.
    pub fn with_deferred_commit(mut self) -> Self {
        self.defer_commit = true;
        self
    }

    /// Floors this session's transaction snapshots at `floor` (a commit_seq),
    /// typically the connection's own last commit_seq for read-your-writes.
    pub fn with_snapshot_floor(mut self, floor: u64) -> Self {
        self.snapshot_floor = floor;
        self
    }

    /// Begin a transaction whose snapshot is floored at this session's
    /// `snapshot_floor` (connection read-your-writes).
    pub(crate) fn begin_session_transaction(&self) -> Result<Transaction> {
        Ok(self.db_ref().begin_transaction_after(self.snapshot_floor)?)
    }

    /// Takes the pending (uncommitted) transaction produced by a deferred
    /// autocommit execution, if one is present.
    pub fn take_pending_transaction(&mut self) -> Option<Transaction> {
        self.tx.take()
    }

    /// Continue an already-open transaction owned by an outer protocol session.
    /// The caller should recover it with [`Self::take_pending_transaction`] after
    /// executing the statement.
    pub fn with_pending_transaction(mut self, tx: Transaction) -> Self {
        self.tx = Some(tx);
        self
    }

    /// Binds already-typed values to PostgreSQL `$1..$n` placeholders without
    /// interpolating SQL text. Embedded application hosts use this together
    /// with a compiler-signed statement and an existing transaction.
    pub fn with_positional_parameters(mut self, parameters: Vec<SqlValue>) -> Self {
        self.routine_vars = Arc::new(
            parameters
                .into_iter()
                .enumerate()
                .map(|(index, value)| (format!("${}", index + 1), value))
                .collect(),
        );
        self
    }

    /// Makes protected DML use host-issued, transaction-local mutation grants.
    /// This is intentionally separate from SQL identity and cannot be set with
    /// SQL, pgwire startup parameters, or GUCs.
    pub fn with_signed_mutation_grants(
        mut self,
        grants: BTreeMap<String, MutationGrantId>,
    ) -> Self {
        self.signed_mutation_grants = grants;
        self
    }

    /// Accept BicDB application's signed, transaction-local security-GUC setup while
    /// retaining the Cell's host-bound actor as the only source of authority.
    pub fn with_signed_application_security_guc_compatibility(mut self) -> Self {
        self.signed_application_security_guc_compatibility = true;
        self
    }

    fn signed_mutation_grant(&self, table: &str) -> Option<MutationGrantId> {
        self.signed_mutation_grants
            .iter()
            .find_map(|(relation, grant)| relation.eq_ignore_ascii_case(table).then_some(*grant))
    }

    pub fn with_catalog_cache(mut self, cache: SqlSessionCatalogCache) -> Self {
        self.schema_cache_generation = cache.schema_cache_generation;
        self.schema_cache = cache.schema_cache;
        self.schema_list_cache = cache.schema_list_cache;
        self.parsed_statement_cache = cache.parsed_statement_cache;
        self
    }

    pub fn with_ddl_undo_log(mut self, log: SqlSessionDdlUndoLog) -> Self {
        self.ddl_undo = log.entries;
        self
    }

    pub fn take_ddl_undo_log(&mut self) -> SqlSessionDdlUndoLog {
        SqlSessionDdlUndoLog {
            entries: std::mem::take(&mut self.ddl_undo),
        }
    }

    pub fn rollback_ddl_undo_to_len(&mut self, len: usize) -> Result<()> {
        self.rollback_ddl_to_len(len)
    }

    pub fn into_catalog_cache(self) -> SqlSessionCatalogCache {
        SqlSessionCatalogCache {
            schema_cache_generation: self.schema_cache_generation,
            schema_cache: self.schema_cache,
            schema_list_cache: self.schema_list_cache,
            parsed_statement_cache: self.parsed_statement_cache,
        }
    }

    pub fn session_gucs(&self) -> &HashMap<String, String> {
        &self.session_gucs
    }

    pub fn session_guc_state(&self) -> SqlSessionGucState {
        SqlSessionGucState {
            settings: self.settings,
            session_gucs: self.session_gucs.clone(),
            transaction_start: self.guc_transaction_start.clone(),
            transaction_timestamp_seconds: self.transaction_timestamp_seconds,
            local_restore: self.guc_local_restore.clone(),
            local_settings_restore: self.guc_local_settings_restore.clone(),
            currvals: self.currvals.clone(),
        }
    }

    fn begin_guc_transaction(&mut self) {
        let mut state = self.take_session_guc_state();
        state.begin_transaction();
        self.install_session_guc_state(state);
    }

    fn with_implicit_guc_statement<T>(
        &mut self,
        execute: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let started_guc_transaction = self.guc_transaction_start.is_none();
        if started_guc_transaction {
            self.begin_guc_transaction();
        }
        let transaction_timestamp_seconds =
            self.transaction_timestamp_seconds.unwrap_or_else(unix_now);
        let timezone = self
            .session_gucs
            .get("timezone")
            .map(String::as_str)
            .or_else(|| default_session_guc("timezone"))
            .unwrap_or("UTC")
            .to_string();
        let interval_style = self
            .session_gucs
            .get("intervalstyle")
            .map(String::as_str)
            .or_else(|| default_session_guc("intervalstyle"))
            .unwrap_or("postgres")
            .to_string();
        let _temporal_scope =
            SqlTemporalScope::new(transaction_timestamp_seconds, timezone, interval_style);
        let result = execute(self);
        if started_guc_transaction {
            match &result {
                Ok(_) if self.tx.is_none() => self.commit_guc_transaction(),
                Ok(_) => {}
                Err(_) => self.rollback_guc_transaction(),
            }
        }
        result
    }

    fn commit_guc_transaction(&mut self) {
        let mut state = self.take_session_guc_state();
        state.commit_transaction();
        self.install_session_guc_state(state);
    }

    fn rollback_guc_transaction(&mut self) {
        let mut state = self.take_session_guc_state();
        state.rollback_transaction();
        self.install_session_guc_state(state);
    }

    fn reconcile_guc_assignments(
        &mut self,
        previous_gucs: Arc<HashMap<String, String>>,
        previous_settings: SqlSettings,
        local: bool,
    ) {
        let changed = previous_gucs
            .keys()
            .chain(self.session_gucs.keys())
            .filter(|key| previous_gucs.get(*key) != self.session_gucs.get(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in changed {
            if local {
                self.guc_local_restore
                    .entry(key.clone())
                    .or_insert_with(|| previous_gucs.get(&key).cloned());
            } else {
                self.guc_local_restore.remove(&key);
            }
        }
        if previous_settings.vector_search != self.settings.vector_search {
            if local {
                self.guc_local_settings_restore
                    .vector_search
                    .get_or_insert(previous_settings.vector_search);
            } else {
                self.guc_local_settings_restore.vector_search = None;
            }
        }
        if previous_settings.ef_search != self.settings.ef_search {
            if local {
                self.guc_local_settings_restore
                    .ef_search
                    .get_or_insert(previous_settings.ef_search);
            } else {
                self.guc_local_settings_restore.ef_search = None;
            }
        }
    }

    fn apply_scoped_guc_change(
        &mut self,
        local: bool,
        targets: GucAssignmentTargets,
        apply: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        let previous_gucs = self.session_gucs.clone();
        let previous_settings = self.settings;
        apply(self)?;
        self.reconcile_guc_assignments(previous_gucs, previous_settings, local);
        if !local {
            targets.clear_local_restore(self);
        }
        Ok(())
    }

    /// Keep a whole mutation statement, including authored triggers and their
    /// deferred effects, inside one transaction when the caller has no BEGIN.
    fn with_statement_transaction<T>(
        &mut self,
        execute: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if let Some(transaction) = self.tx.as_ref().map(Transaction::rollback_mark) {
            let ddl_undo_len = self.ddl_undo.len();
            let savepoint_len = self.savepoints.len();
            let mut gucs = self.session_guc_state();
            let result = execute(self);
            if result.is_err() {
                if let Some(tx) = self.tx.as_mut() {
                    tx.rollback_to_mark(transaction)?;
                }
                self.rollback_ddl_to_len(ddl_undo_len)?;
                self.savepoints.truncate(savepoint_len);
                // Sequence consumption and currval/lastval are not rolled back.
                gucs.currvals = std::mem::take(&mut self.currvals);
                let lastval = self.session_gucs.get(LASTVAL_SESSION_KEY).cloned();
                self.install_session_guc_state(gucs);
                if let Some(lastval) = lastval {
                    Arc::make_mut(&mut self.session_gucs)
                        .insert(LASTVAL_SESSION_KEY.into(), lastval);
                }
            }
            return result;
        }
        self.tx = Some(self.begin_session_transaction()?);
        let result = execute(self).and_then(|result| {
            self.fire_deferred_row_triggers()?;
            Ok(result)
        });
        match result {
            Ok(result) => {
                if !self.defer_commit {
                    self.commit()?;
                }
                Ok(result)
            }
            Err(error) => {
                self.rollback_current_transaction()?;
                Err(error)
            }
        }
    }

    pub fn copy_insert_rows(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize> {
        self.with_implicit_guc_statement(|session| {
            session.with_statement_transaction(|session| {
                session.copy_insert_rows_inner(table, columns, rows)
            })
        })
    }

    fn copy_insert_rows_inner(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize> {
        // A COPY target keeps its schema qualifier (pgwire no longer
        // collapses it), so apply the SAME schema->physical encoding
        // INSERT/SELECT use before resolving. Without this the qualifier
        // was dropped and `COPY carrier_private.context_nonces` loaded into
        // `public.context_nonces`.
        let qualified: Vec<String> = table.split('.').map(str::to_string).collect();
        let table = relation_name_from_parts(&qualified)?;
        let table = resolve_session_relation_name(self.db_ref(), &table)?;
        // COPY FROM is an INSERT with a different wire shape. It enforced RLS
        // but never the INSERT privilege, so any authenticated pgwire client
        // could bulk-load rows into any table that plain INSERT refused.
        self.require_table_privilege(&table, "INSERT")?;
        let schema = load_schema(self.db_ref(), &table)?;
        let columns = if columns.is_empty() {
            let Some(schema) = schema.as_ref() else {
                return Err(SqlError::Unsupported(
                    "COPY without a column list requires a table schema".to_string(),
                ));
            };
            schema
                .columns
                .iter()
                .filter(|column| !column.hidden && column.generated_expr.is_none())
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            columns.to_vec()
        };
        if let Some(schema) = schema.as_ref() {
            for column in &columns {
                ensure_schema_column(&table, schema, column)?;
            }
        }
        let defaults = self.bulk_missing_insert_defaults(schema.as_ref(), &columns, rows.len())?;
        let mut records = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            self.cancellation.check()?;
            if row.len() != columns.len() {
                return Err(SqlError::InvalidSql(format!(
                    "COPY expected {} columns, got {}",
                    columns.len(),
                    row.len()
                )));
            }
            let mut fields = BTreeMap::new();
            for (column, value) in columns.iter().zip(row) {
                fields.insert(
                    column.clone(),
                    value.map(SqlValue::String).unwrap_or(SqlValue::Null),
                );
            }
            self.apply_bulk_insert_defaults(schema.as_ref(), &defaults, idx, &mut fields)?;
            records.push(record_from_fields_with_db(
                self.db_ref(),
                &table,
                schema.as_ref(),
                fields,
            )?);
        }
        if let Some(schema) = schema.as_ref() {
            self.materialize_generated_columns(&table, schema, &mut records)?;
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                &table,
                schema,
                &records,
                false,
            )?;
        }
        let records = self.apply_before_insert_row_triggers(&table, schema.as_ref(), records)?;
        self.enforce_rls_checks(&table, schema.as_ref(), PolicyAction::Insert, &records)?;
        let count = records.len();
        self.insert_session_records(&table, records.clone())?;
        self.fire_after_insert_triggers(&table, schema.as_ref(), &records)?;
        Ok(count)
    }

    pub fn execute(&mut self, sql: &str) -> Result<SqlResult> {
        // Statement-scoped only: CREATE/DROP INDEX between statements must
        // not be answered from a stale arbiter.
        self.conflict_arbiter_memo.borrow_mut().take();
        self.with_implicit_guc_statement(|session| {
            let mut result = session.execute_profiled(sql)?;
            session.apply_bytea_output(&mut result);
            Ok(result)
        })
    }

    fn apply_bytea_output(&self, result: &mut SqlResult) {
        let output = self
            .session_gucs
            .get("bytea_output")
            .map(String::as_str)
            .or_else(|| default_session_guc("bytea_output"))
            .unwrap_or("hex");
        if output != "escape" {
            return;
        }
        for (index, pg_type) in result.column_types.iter().enumerate() {
            if pg_type.as_deref() != Some("bytea") {
                continue;
            }
            for row in &mut result.rows {
                let Some(SqlValue::String(value)) = row.get_mut(index) else {
                    continue;
                };
                if let Ok(bytes) = parse_bytea_text(value) {
                    *value = format_bytea_escape(&bytes);
                }
            }
        }
    }

    fn execute_profiled(&mut self, sql: &str) -> Result<SqlResult> {
        let mut profile = SqlProfileScope::new(sql);
        let schema_generation = self.db_ref().collection_generation(SCHEMA_COLLECTION);
        let mut schema_cache = SqlSchemaSessionCacheScope::new(
            schema_generation,
            &mut self.schema_cache_generation,
            &mut self.schema_cache,
            &mut self.schema_list_cache,
        );
        let result = self.execute_inner(sql);
        let schema_generation = self.db_ref().collection_generation(SCHEMA_COLLECTION);
        schema_cache.finish(
            schema_generation,
            &mut self.schema_cache_generation,
            &mut self.schema_cache,
            &mut self.schema_list_cache,
        );
        profile.finish(&result);
        result
    }

    /// Execute an already-parsed SQL statement through the same executor used by
    /// text SQL after parsing. This is primarily for embedded clients and
    /// diagnostics that want to preserve normal statement semantics without
    /// measuring parser or SQL-string construction cost.
    pub fn execute_statement_ast(&mut self, statement: &Statement) -> Result<SqlResult> {
        self.with_implicit_guc_statement(|session| {
            let mut result = session.execute_statement(statement)?;
            session.apply_bytea_output(&mut result);
            Ok(result)
        })
    }

    /// Execute a stored procedure directly from its already-typed arguments,
    /// bypassing SQL text parsing and `CALL` AST construction.
    pub fn call_procedure(&mut self, name: &str, args: &[SqlValue]) -> Result<SqlResult> {
        self.with_implicit_guc_statement(|session| {
            let name = name.to_ascii_lowercase();
            let routine = resolve_routine_cached(session.db_ref(), RoutineKind::Procedure, &name)?
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("procedure \"{name}\" does not exist"))
                })?;
            session.ensure_routine_execute_privilege(&name)?;
            session.execute_with_routine_security(&routine.schema, |session| {
                session.execute_plpgsql_procedure(&routine.ir, args)
            })
        })
    }

    pub(crate) fn execute_inner(&mut self, sql: &str) -> Result<SqlResult> {
        self.cancellation.check()?;
        if sql_has_no_statements(sql) {
            return Ok(SqlResult::empty(Vec::new()));
        }
        if leading_keyword_bypasses_raw_probes(sql) {
            return self.execute_parsed_sql(sql);
        }
        if let Some(result) = self.execute_raw_reset(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_comment(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_analyze(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_memory_table(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_memory_index(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_process_memory_jobs(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_similar_to_select(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_sequence(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_database_ddl(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_schema_owner(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_view_owner(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_pg_dump_range_type(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_extension_catalog(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_extension(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_sequence(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_identity_restart(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_add_identity(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_column_compression(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_table_set_schema(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_user_type(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_table_like(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_table_compression(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_partition_table(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_partition_ddl(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_exclusion_constraint_ddl(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_domain(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_composite_type(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_drop_domain(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_spatial_index(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_pack_spatial_index(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_vacuum(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_trim_audit_history(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_cube_ddl(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_projection_merge(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_projection_rollup(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_projection_admin(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_index_on_only(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_user(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_role_membership_ddl(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_create_trigger(sql)? {
            return Ok(result);
        }

        if let Some(ddl) = parse_raw_alter_default_privileges(sql)? {
            self.execute_raw_alter_default_privileges(ddl)?;
            return Ok(SqlResult::command("ALTER DEFAULT PRIVILEGES"));
        }

        if let Some(result) = self.execute_raw_type_privileges(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_function_privileges(sql)? {
            return Ok(result);
        }

        if let Some(revokes) = split_raw_multi_grantee_revoke(sql)? {
            for revoke in revokes {
                self.execute(&revoke)?;
            }
            return Ok(SqlResult::command("REVOKE"));
        }

        if let Some(result) = self.execute_raw_do_ddl(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.sql_engine().execute_builtin(sql)? {
            return Ok(result);
        }

        self.execute_parsed_sql(sql)
    }

    /// Parse and run the statements of `sql`; the raw-text handlers above
    /// have either declined or been bypassed.
    fn execute_parsed_sql(&mut self, sql: &str) -> Result<SqlResult> {
        if crate::statement_cache::statement_is_cacheable(sql) {
            if let Some(result) = self.execute_cached_statements(sql)? {
                return Ok(result);
            }
        }
        let statements = match parse_statements(sql) {
            Ok(statements) => statements,
            Err(error) => return self.execute_split_statements_with_raw_fallback(sql, error),
        };
        self.execute_statement_list(&statements)
    }

    fn execute_statement_list(&mut self, statements: &[Statement]) -> Result<SqlResult> {
        let mut last_result = None;
        for statement in statements {
            last_result = Some(self.execute_parsed_statement(statement)?);
        }
        last_result.ok_or_else(|| SqlError::InvalidSql("empty SQL statement".to_string()))
    }

    /// The statement list for `sql` from the template cache, with its
    /// literals bound; `None` when the text has nothing to lift or its
    /// template does not parse (the caller then parses the original text,
    /// which reports the real error).
    /// Run `sql` through the template cache: the parsed template is taken out
    /// of the cache, its literals bound in place, executed by reference and
    /// put back. `None` when the text has nothing to lift or its template does
    /// not parse (the caller then parses the original text, which reports the
    /// real error).
    fn execute_cached_statements(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some((template, literals)) = crate::statement_cache::templatize(sql) else {
            return Ok(None);
        };
        let mut cached = match self.parsed_statement_cache.remove(&template) {
            Some(cached) => cached,
            None => {
                let Ok(parsed) = parse_statements(&template) else {
                    return Ok(None);
                };
                if self.parsed_statement_cache.len() >= crate::statement_cache::MAX_CACHED_TEMPLATES
                {
                    self.parsed_statement_cache.clear();
                }
                crate::statement_cache::CachedTemplate::prepare(parsed, literals.len())?
            }
        };
        let result = cached
            .bind(&literals)
            .and_then(|()| self.execute_statement_list(&cached.statements));
        // Back into the cache whatever the outcome (a nested execution of the
        // same text may have inserted its own copy meanwhile; ours wins).
        self.parsed_statement_cache.insert(template, cached);
        result.map(Some)
    }

    pub(crate) fn execute_split_statements_with_raw_fallback(
        &mut self,
        sql: &str,
        parse_error: SqlError,
    ) -> Result<SqlResult> {
        let statements = split_sql_statements(sql);
        if statements.len() <= 1 {
            if let Some(result) = self.execute_raw_extension_catalog(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_create_function(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_create_procedure(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_create_trigger(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_table_set_schema(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_schema_owner(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_column_compression(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_user_type(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_type_privileges(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_domain(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_alter_composite_type(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_drop_domain(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_create_index_on_only(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_create_table_compression(sql)? {
                return Ok(result);
            }
            return Err(parse_error);
        }

        let mut last_result = None;
        for statement in statements {
            self.cancellation.check()?;
            if let Some(result) = self.execute_raw_extension_catalog(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_reset(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_comment(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_sequence(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_sequence(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_identity_restart(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_column_compression(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_table_set_schema(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_schema_owner(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_user_type(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_type_privileges(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_partition_table(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_table_compression(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_partition_ddl(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_exclusion_constraint_ddl(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_domain(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_composite_type(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_drop_domain(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_function(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_procedure(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_trigger(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_index_on_only(&statement)? {
                last_result = Some(result);
                continue;
            }
            last_result = Some(self.execute(&statement)?);
        }
        last_result.ok_or_else(|| SqlError::InvalidSql("empty SQL statement".to_string()))
    }

    pub(crate) fn execute_parsed_statement(&mut self, statement: &Statement) -> Result<SqlResult> {
        if self.trigger_depth > 0
            && matches!(
                statement,
                Statement::StartTransaction { .. }
                    | Statement::Commit { .. }
                    | Statement::Rollback { .. }
                    | Statement::Savepoint { .. }
                    | Statement::ReleaseSavepoint { .. }
            )
        {
            return Err(SqlError::data_exception(
                "2D000",
                "transaction control is not permitted in a trigger",
                None,
            ));
        }
        match statement {
            Statement::StartTransaction { modes, .. } => {
                validate_start_transaction_modes(modes)?;
                if self.tx.is_some() {
                    return Err(SqlError::Unsupported(
                        "nested transactions are not supported".to_string(),
                    ));
                }
                self.tx = Some(self.begin_session_transaction()?);
                Ok(SqlResult::command("BEGIN"))
            }
            Statement::Commit { .. } => self.commit(),
            Statement::Rollback { savepoint, .. } => {
                if let Some(savepoint) = savepoint {
                    return self.rollback_to_savepoint(savepoint);
                }
                self.rollback_current_transaction()
            }
            Statement::Savepoint { name } => self.create_savepoint(name),
            Statement::ReleaseSavepoint { name } => self.release_savepoint(name),
            Statement::Set(set) => self.execute_set(set),
            statement => self.execute_statement(statement),
        }
    }

    /// Internal rollback cleanup must run even if the statement was canceled.
    fn rollback_current_transaction(&mut self) -> Result<SqlResult> {
        // A rolled-back transaction's deferred constraint triggers
        // drop with the transaction itself.
        let tx_rollback = self
            .tx
            .take()
            .map(|tx| tx.rollback().map_err(SqlError::from))
            .unwrap_or(Ok(()));
        let ddl_rollback = self.rollback_ddl_to_len(0);
        self.savepoints.clear();
        self.rollback_guc_transaction();
        tx_rollback?;
        ddl_rollback?;
        Ok(SqlResult::command("ROLLBACK"))
    }

    pub(crate) fn commit(&mut self) -> Result<SqlResult> {
        // Deferred constraint triggers fire now, with the transaction still
        // open, so their checks see the complete write set and their failures
        // abort the commit like any failed statement.
        if self
            .tx
            .as_ref()
            .is_some_and(Transaction::has_deferred_hooks)
        {
            if let Err(error) = self.fire_deferred_row_triggers() {
                if let Some(tx) = self.tx.take() {
                    let _ = tx.rollback();
                }
                let _ddl_rollback = self.rollback_ddl_to_len(0);
                self.savepoints.clear();
                self.rollback_guc_transaction();
                return Err(error);
            }
        }
        if let Some(tx) = self.tx.take() {
            if let Err(error) = tx.commit() {
                let _ddl_rollback = self.rollback_ddl_to_len(0);
                self.savepoints.clear();
                self.rollback_guc_transaction();
                return Err(error.into());
            }
        }
        self.ddl_undo.clear();
        self.savepoints.clear();
        self.commit_guc_transaction();
        Ok(SqlResult::command("COMMIT"))
    }

    pub(crate) fn create_savepoint(&mut self, name: &Ident) -> Result<SqlResult> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(SqlError::InvalidTransactionState {
                message: "SAVEPOINT can only be used in transaction blocks".to_string(),
            });
        };
        self.savepoints.push(SavepointMark {
            name: savepoint_name(name),
            transaction: tx.rollback_mark(),
            ddl_undo_len: self.ddl_undo.len(),
            settings: self.settings,
            session_gucs: self.session_gucs.clone(),
            guc_local_restore: self.guc_local_restore.clone(),
            guc_local_settings_restore: self.guc_local_settings_restore.clone(),
        });
        Ok(SqlResult::command("SAVEPOINT"))
    }

    pub(crate) fn rollback_to_savepoint(&mut self, name: &Ident) -> Result<SqlResult> {
        let Some(tx) = self.tx.as_mut() else {
            return Err(SqlError::InvalidTransactionState {
                message: "ROLLBACK TO SAVEPOINT can only be used in transaction blocks".to_string(),
            });
        };
        let name = savepoint_name(name);
        let Some(position) = self.savepoints.iter().rposition(|mark| mark.name == name) else {
            return Err(SqlError::InvalidSavepoint { name });
        };
        let transaction = self.savepoints[position].transaction;
        let ddl_undo_len = self.savepoints[position].ddl_undo_len;
        let settings = self.savepoints[position].settings;
        let session_gucs = self.savepoints[position].session_gucs.clone();
        let guc_local_restore = self.savepoints[position].guc_local_restore.clone();
        let guc_local_settings_restore =
            self.savepoints[position].guc_local_settings_restore.clone();
        let write_rollback = tx.rollback_to_mark(transaction);
        let ddl_rollback = self.rollback_ddl_to_len(ddl_undo_len);
        let lastval = self.session_gucs.get(LASTVAL_SESSION_KEY).cloned();
        self.settings = settings;
        self.session_gucs = session_gucs;
        match lastval {
            Some(lastval) => {
                Arc::make_mut(&mut self.session_gucs)
                    .insert(LASTVAL_SESSION_KEY.to_string(), lastval);
            }
            None => {
                Arc::make_mut(&mut self.session_gucs).remove(LASTVAL_SESSION_KEY);
            }
        }
        self.guc_local_restore = guc_local_restore;
        self.guc_local_settings_restore = guc_local_settings_restore;
        self.savepoints.truncate(position + 1);
        write_rollback?;
        ddl_rollback?;
        Ok(SqlResult::command("ROLLBACK"))
    }

    pub(crate) fn release_savepoint(&mut self, name: &Ident) -> Result<SqlResult> {
        if self.tx.is_none() {
            return Err(SqlError::InvalidTransactionState {
                message: "RELEASE SAVEPOINT can only be used in transaction blocks".to_string(),
            });
        }
        let name = savepoint_name(name);
        let Some(position) = self.savepoints.iter().rposition(|mark| mark.name == name) else {
            return Err(SqlError::InvalidSavepoint { name });
        };
        self.savepoints.truncate(position);
        Ok(SqlResult::command("RELEASE"))
    }

    pub(crate) fn create_session_collection(&mut self, name: &str) -> Result<()> {
        let existed = self
            .db_ref()
            .collections()
            .iter()
            .any(|collection| collection.name.eq_ignore_ascii_case(name));
        self.db_mut()?.create_collection(name)?;
        if self.tx.is_some() && !existed {
            self.ddl_undo.push(DdlUndo::DropCollection {
                name: name.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn drop_session_collection(&mut self, name: &str) -> Result<bool> {
        let records = if self.tx.is_some()
            && self
                .db_ref()
                .collections()
                .iter()
                .any(|collection| collection.name.eq_ignore_ascii_case(name))
        {
            Some(self.db_ref().scan_collection(name)?)
        } else {
            None
        };
        let existed = self.db_mut()?.drop_collection(name)?;
        if let Some(records) = records {
            self.ddl_undo.push(DdlUndo::RestoreCollection {
                name: name.to_string(),
                records,
            });
        }
        Ok(existed)
    }

    pub(crate) fn create_session_index(&mut self, definition: IndexDefinition) -> Result<()> {
        let name = definition.name.clone();
        self.db_mut()?.create_index(definition)?;
        sql_index_definitions_cache_clear();
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::DropIndex { name });
        }
        Ok(())
    }

    pub(crate) fn drop_session_index(&mut self, name: &str) -> Result<bool> {
        let internal_names = list_schemas(self.db_ref())?
            .into_iter()
            .flat_map(|schema| schema.indexes)
            .find(|index| index.name.eq_ignore_ascii_case(name))
            .map(|index| index.internal_index_names)
            .unwrap_or_default();
        let executable_definitions = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .filter(|definition| {
                definition.name.eq_ignore_ascii_case(name)
                    || internal_names
                        .iter()
                        .any(|internal| internal.eq_ignore_ascii_case(&definition.name))
            })
            .collect::<Vec<_>>();
        let mut executable_existed = false;
        for definition in &executable_definitions {
            executable_existed |= self.db_mut()?.drop_index(&definition.name)?;
        }
        if !executable_existed {
            executable_existed = self
                .db_ref()
                .discard_full_text_build(name)
                .map_err(SqlError::from)?;
        }
        if executable_existed {
            sql_index_definitions_cache_clear();
        }
        if self.tx.is_some() && executable_existed {
            for definition in executable_definitions {
                self.ddl_undo.push(DdlUndo::RestoreIndex { definition });
            }
        }
        let metadata_existed = self.remove_session_index_from_schemas(name)?;
        Ok(executable_existed || metadata_existed)
    }

    pub(crate) fn rename_session_index(&mut self, old_name: &str, new_name: &str) -> Result<bool> {
        let executable_definition = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .find(|definition| definition.name.eq_ignore_ascii_case(old_name));
        let Some(definition) = executable_definition else {
            return Ok(false);
        };
        if definition.name.eq_ignore_ascii_case(new_name) {
            return Ok(true);
        }
        let renamed = self.db_mut()?.rename_index(&definition.name, new_name)?;
        if renamed {
            sql_index_definitions_cache_clear();
        }
        if self.tx.is_some() && renamed {
            self.ddl_undo.push(DdlUndo::RenameIndex {
                old_name: definition.name,
                new_name: new_name.to_string(),
            });
        }
        Ok(renamed)
    }

    pub(crate) fn remove_session_index_from_schemas(&mut self, index_name: &str) -> Result<bool> {
        let mut removed = false;
        for mut schema in list_schemas(self.db_ref())? {
            let before = schema.indexes.len();
            schema.indexes.retain(|index| index.name != index_name);
            if schema.indexes.len() != before {
                self.save_session_schema(&schema)?;
                removed = true;
            }
        }
        Ok(removed)
    }

    pub(crate) fn execute_raw_function_privileges(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(ddl) = parse_raw_function_privilege_ddl(self.db_ref(), sql)? else {
            return Ok(None);
        };
        let targets = ddl
            .routines
            .iter()
            .map(|routine| (PrivilegeObjectType::Function, routine.clone()))
            .collect::<Vec<_>>();
        self.require_grant_authority(&targets, if ddl.grant { "GRANT" } else { "REVOKE" })?;
        let known_roles = list_roles(self.db_ref())?;
        let grantees = ddl
            .grantees
            .into_iter()
            .map(|grantee| match grantee.as_str() {
                "CURRENT_USER" | "CURRENT_ROLE" => current_user_from_gucs(&self.session_gucs),
                "SESSION_USER" => session_user_from_gucs(&self.session_gucs),
                _ => grantee,
            })
            .collect::<Vec<_>>();
        // Owners and superusers already have implicit grant authority.
        // Accept their redundant grant option, but reject delegation to
        // other roles until dependent-grant tracking is implemented.
        if ddl.with_grant_option {
            for grantee in &grantees {
                let superuser = grantee == BOOTSTRAP_ROLE_NAME
                    || known_roles
                        .iter()
                        .any(|role| role.name == *grantee && role.superuser);
                for routine in &ddl.routines {
                    let owner = load_routine(self.db_ref(), RoutineKind::Function, routine)?
                        .map(|routine| routine.owner().to_owned())
                        .unwrap_or_else(current_role_name);
                    if !superuser
                        && (grantee == "public"
                            || !role_privilege_closure(self.db_ref(), grantee)?
                                .contains(&normalize_role_name(&owner)))
                    {
                        return Err(SqlError::Unsupported("function grant options for non-owner roles require dependent-grant tracking".into()));
                    }
                }
            }
        }
        for grantee in &grantees {
            if grantee != "public" && !known_roles.iter().any(|role| role.name == *grantee) {
                return Err(SqlError::UndefinedRole {
                    name: grantee.clone(),
                });
            }
        }
        for grantee in grantees {
            for routine in &ddl.routines {
                let privilege = PrivilegeGrant {
                    column: None,
                    object_type: PrivilegeObjectType::Function,
                    object_name: routine.clone(),
                    grantee: grantee.clone(),
                    privilege: "EXECUTE".to_string(),
                };
                if ddl.grant {
                    self.save_session_privilege(&privilege)?;
                } else {
                    self.delete_session_privilege(&privilege)?;
                }
            }
        }
        return Ok(Some(SqlResult::command(if ddl.grant {
            "GRANT"
        } else {
            "REVOKE"
        })));
    }

    pub(crate) fn save_session_schema(&mut self, schema: &TableSchema) -> Result<()> {
        let previous = load_schema(self.db_ref(), &schema.name)?;
        let created = previous.is_none();
        save_schema(self.db_mut()?, schema)?;
        if self.tx.is_some() {
            match previous.clone() {
                Some(schema) => self.ddl_undo.push(DdlUndo::RestoreSchema { schema }),
                None => self.ddl_undo.push(DdlUndo::DeleteSchema {
                    table: schema.name.clone(),
                }),
            }
        }
        if created {
            self.apply_default_privileges(
                schema.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME),
                &schema.schema_name,
                PrivilegeObjectType::Table,
                &schema.name,
            )?;
        }
        Ok(())
    }

    pub(crate) fn delete_session_schema(&mut self, table: &str) -> Result<()> {
        let previous = if self.tx.is_some() {
            load_schema(self.db_ref(), table)?
        } else {
            None
        };
        delete_schema(self.db_mut()?, table)?;
        for grant in list_privileges(self.db_ref())? {
            if grant.object_type == PrivilegeObjectType::Table && grant.object_name == table {
                self.delete_session_privilege(&grant)?;
            }
        }
        if let Some(schema) = previous {
            self.ddl_undo.push(DdlUndo::RestoreSchema { schema });
        }
        Ok(())
    }

    pub(crate) fn capture_table_state_undo(
        &mut self,
        table: &str,
        schema: &TableSchema,
    ) -> Result<()> {
        if self.tx.is_none() {
            return Ok(());
        }
        self.ddl_undo.push(DdlUndo::RestoreTableState {
            table: table.to_string(),
            schema: schema.clone(),
            records: self.db_ref().scan_collection(table)?,
        });
        Ok(())
    }

    pub(crate) fn create_session_namespace_if_missing(
        &mut self,
        namespace: NamespaceSchema,
        if_not_exists: bool,
    ) -> Result<()> {
        let existed = load_namespace(self.db_ref(), &namespace.name)?.is_some();
        save_namespace_if_missing(self.db_mut()?, namespace.clone(), if_not_exists)?;
        if self.tx.is_some() && !existed {
            self.ddl_undo.push(DdlUndo::DeleteNamespace {
                namespace: namespace.name,
            });
        }
        Ok(())
    }

    pub(crate) fn create_session_sequence_if_missing(
        &mut self,
        mut sequence: SequenceSchema,
        if_not_exists: bool,
    ) -> Result<()> {
        sequence.owner = current_user_from_gucs(&self.session_gucs);
        let existed = load_sequence(self.db_ref(), &sequence.name)?.is_some();
        create_sequence_if_missing(self.db_mut()?, sequence.clone(), if_not_exists)?;
        if self.tx.is_some() && !existed {
            self.ddl_undo.push(DdlUndo::DeleteSequence {
                sequence: sequence.name.clone(),
            });
        }
        if !existed {
            self.apply_default_privileges(
                &sequence.owner,
                "public",
                PrivilegeObjectType::Sequence,
                &sequence.name,
            )?;
        }
        Ok(())
    }

    pub(crate) fn save_session_sequence(&mut self, sequence: &SequenceSchema) -> Result<()> {
        let previous = load_sequence(self.db_ref(), &sequence.name)?;
        let created = previous.is_none();
        save_sequence(self.db_mut()?, sequence)?;
        if self.tx.is_some() {
            match previous.clone() {
                Some(sequence) => self.ddl_undo.push(DdlUndo::RestoreSequence { sequence }),
                None => self.ddl_undo.push(DdlUndo::DeleteSequence {
                    sequence: sequence.name.clone(),
                }),
            }
        }
        if created {
            self.apply_default_privileges(
                &sequence.owner,
                "public",
                PrivilegeObjectType::Sequence,
                &sequence.name,
            )?;
        }
        Ok(())
    }

    pub(crate) fn delete_session_sequence(&mut self, sequence: &str) -> Result<bool> {
        let previous = if self.tx.is_some() {
            load_sequence(self.db_ref(), sequence)?
        } else {
            None
        };
        let existed = delete_sequence(self.db_mut()?, sequence)?;
        if let Some(sequence) = previous {
            self.ddl_undo.push(DdlUndo::RestoreSequence { sequence });
        }
        Ok(existed)
    }

    pub(crate) fn drop_owned_sequences_for_table(&mut self, table: &str) -> Result<()> {
        let owned_sequences = list_sequences(self.db_ref())?
            .into_iter()
            .filter(|sequence| {
                sequence
                    .owned_by_table
                    .as_deref()
                    .is_some_and(|owned_table| owned_table.eq_ignore_ascii_case(table))
            })
            .map(|sequence| sequence.name)
            .collect::<Vec<_>>();
        for sequence in owned_sequences {
            self.delete_session_sequence(&sequence)?;
        }
        Ok(())
    }

    pub(crate) fn save_session_view(&mut self, view: &ViewSchema) -> Result<()> {
        let previous = load_view(self.db_ref(), &view.name)?;
        let created = previous.is_none();
        save_view(self.db_mut()?, view)?;
        if self.tx.is_some() {
            match previous.clone() {
                Some(view) => self.ddl_undo.push(DdlUndo::RestoreView { view }),
                None => self.ddl_undo.push(DdlUndo::DeleteView {
                    view: view.name.clone(),
                }),
            }
        }
        if created {
            self.apply_default_privileges(
                view.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME),
                "public",
                PrivilegeObjectType::Table,
                &view.name,
            )?;
        }
        Ok(())
    }

    pub(crate) fn delete_session_view(&mut self, view: &str) -> Result<bool> {
        let previous = if self.tx.is_some() {
            load_view(self.db_ref(), view)?
        } else {
            None
        };
        let existed = delete_view(self.db_mut()?, view)?;
        if let Some(view) = previous {
            self.ddl_undo.push(DdlUndo::RestoreView { view });
        }
        Ok(existed)
    }

    pub(crate) fn create_session_trigger_if_missing(
        &mut self,
        trigger: TriggerSchema,
        or_replace: bool,
    ) -> Result<()> {
        let previous = if self.tx.is_some() {
            find_trigger(self.db_ref(), &trigger.name, Some(&trigger.table_name))?
        } else {
            None
        };
        save_trigger_if_missing(self.db_mut()?, trigger.clone(), or_replace)?;
        if self.tx.is_some() {
            match previous {
                Some(previous) => {
                    if or_replace {
                        self.ddl_undo
                            .push(DdlUndo::RestoreTrigger { trigger: previous });
                    }
                }
                None => self.ddl_undo.push(DdlUndo::DeleteTrigger {
                    table: trigger.table_name,
                    trigger: trigger.name,
                }),
            }
        }
        Ok(())
    }

    pub(crate) fn save_session_trigger(&mut self, trigger: &TriggerSchema) -> Result<()> {
        let previous = if self.tx.is_some() {
            Some(find_trigger(
                self.db_ref(),
                &trigger.name,
                Some(&trigger.table_name),
            )?)
        } else {
            None
        };
        save_trigger(self.db_mut()?, trigger)?;
        if let Some(previous) = previous {
            match previous {
                Some(trigger) => self.ddl_undo.push(DdlUndo::RestoreTrigger { trigger }),
                None => self.ddl_undo.push(DdlUndo::DeleteTrigger {
                    table: trigger.table_name.clone(),
                    trigger: trigger.name.clone(),
                }),
            }
        }
        Ok(())
    }

    pub(crate) fn delete_session_trigger(
        &mut self,
        name: &str,
        table_name: Option<&str>,
    ) -> Result<bool> {
        let previous = if self.tx.is_some() {
            find_trigger(self.db_ref(), name, table_name)?
        } else {
            None
        };
        let existed = delete_trigger(self.db_mut()?, name, table_name)?;
        if let Some(trigger) = previous {
            self.ddl_undo.push(DdlUndo::RestoreTrigger { trigger });
        }
        Ok(existed)
    }

    pub(crate) fn rollback_ddl_to_len(&mut self, len: usize) -> Result<()> {
        while self.ddl_undo.len() > len {
            let undo = self
                .ddl_undo
                .pop()
                .expect("ddl undo length checked before pop");
            self.apply_ddl_undo(undo)?;
        }
        Ok(())
    }

    pub(crate) fn apply_ddl_undo(&mut self, undo: DdlUndo) -> Result<()> {
        match undo {
            DdlUndo::RestoreRole { name, previous } => {
                delete_role_record(self.db_mut()?, &name)?;
                if let Some(role) = previous {
                    create_role_record(self.db_mut()?, role, false)?;
                }
            }
            DdlUndo::RestoreMembership {
                role,
                member,
                previous,
            } => {
                delete_role_membership(self.db_mut()?, &role, &member)?;
                if let Some(membership) = previous {
                    save_role_membership(self.db_mut()?, &membership)?;
                }
            }
            DdlUndo::DropCollection { name } => {
                let _ = self.db_mut()?.drop_collection(&name)?;
            }
            DdlUndo::RestoreCollection { name, records } => {
                self.db_mut()?.create_collection(&name)?;
                if !records.is_empty() {
                    self.db_mut()?.batch_insert(&name, records)?;
                }
            }
            DdlUndo::RestoreTableState {
                table,
                schema,
                records,
            } => {
                restore_table_state(self.db_mut()?, &table, &schema, records)?;
            }
            DdlUndo::RestoreRenamedTable {
                old_table,
                new_table,
                records,
                indexes,
                schema,
                sequences,
            } => {
                restore_renamed_table(
                    self.db_mut()?,
                    &old_table,
                    &new_table,
                    records,
                    indexes,
                    schema,
                    sequences,
                )?;
            }
            DdlUndo::DropIndex { name } => {
                let _ = self.db_mut()?.drop_index(&name)?;
            }
            DdlUndo::RestoreIndex { definition } => {
                self.db_mut()?.create_index(definition)?;
            }
            DdlUndo::RenameIndex { old_name, new_name } => {
                let _ = self.db_mut()?.rename_index(&new_name, &old_name)?;
            }
            DdlUndo::DeleteSchema { table } => {
                delete_schema(self.db_mut()?, &table)?;
            }
            DdlUndo::RestoreSchema { schema } => {
                save_schema(self.db_mut()?, &schema)?;
            }
            DdlUndo::DeleteSequence { sequence } => {
                let _ = delete_sequence(self.db_mut()?, &sequence)?;
            }
            DdlUndo::RestoreSequence { sequence } => {
                save_sequence(self.db_mut()?, &sequence)?;
            }
            DdlUndo::DeleteView { view } => {
                let _ = delete_view(self.db_mut()?, &view)?;
            }
            DdlUndo::RestoreView { view } => {
                save_view(self.db_mut()?, &view)?;
            }
            DdlUndo::DeleteTrigger { table, trigger } => {
                let _ = delete_trigger(self.db_mut()?, &trigger, Some(&table))?;
            }
            DdlUndo::RestoreTrigger { trigger } => {
                save_trigger(self.db_mut()?, &trigger)?;
            }
            DdlUndo::DeleteUserType { schema_name, name } => {
                let _ = delete_user_type(self.db_mut()?, &schema_name, &name)?;
            }
            DdlUndo::RestoreUserType { user_type } => {
                save_user_type(self.db_mut()?, &user_type)?;
            }
            DdlUndo::DeletePrivilege { grant } => {
                delete_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::RestorePrivilege { grant } => {
                save_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::DeleteDefaultPrivilege { grant } => {
                delete_default_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::RestoreDefaultPrivilege { grant } => {
                save_default_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::DeleteRoutine { kind, name } => {
                delete_routine(self.db_mut()?, kind, &name)?;
            }
            DdlUndo::RestoreRoutine { routine } => {
                save_routine(self.db_mut()?, &routine)?;
            }
            DdlUndo::DeleteNamespace { namespace } => {
                delete_namespace(self.db_mut()?, &namespace)?;
            }
            DdlUndo::RestoreNamespace { namespace } => {
                save_namespace(self.db_mut()?, namespace)?;
            }
            DdlUndo::DeleteExtensionInstallation { name } => {
                let _ = delete_extension(self.db_mut()?, &name, true)?;
            }
            DdlUndo::DeleteLegacyExtension { name } => {
                let _ = self.db_mut()?.delete(EXTENSION_COLLECTION, &name)?;
            }
            DdlUndo::RestoreExtensionInstallation { installation } => {
                save_extension(self.db_mut()?, &installation)?;
            }
            DdlUndo::RestoreLegacyExtension { extension } => {
                match self.db_mut()?.delete(EXTENSION_COLLECTION, &extension.name) {
                    Ok(_) | Err(BicDbError::CollectionNotFound(_)) => {}
                    Err(error) => return Err(error.into()),
                }
                save_extension_if_missing(self.db_mut()?, extension, false)?;
            }
            DdlUndo::DeleteExtensionResource { name } => {
                let _ = delete_rest_resource(self.db_mut()?, &name)?;
            }
            DdlUndo::RestoreExtensionResource { resource } => {
                let _ = delete_rest_resource(self.db_mut()?, &resource.name)?;
                restore_rest_resource(self.db_mut()?, resource)?;
            }
            DdlUndo::DeleteExtensionEventBinding { name } => {
                let _ = delete_event_binding(self.db_mut()?, &name)?;
            }
            DdlUndo::RestoreExtensionEventBinding { binding } => {
                let _ = delete_event_binding(self.db_mut()?, &binding.name)?;
                restore_event_binding(self.db_mut()?, binding)?;
            }
            DdlUndo::DeleteExtensionWebsite { name } => {
                let _ = delete_website(self.db_mut()?, &name, true)?;
            }
            DdlUndo::RestoreExtensionWebsite { website } => {
                restore_website(self.db_mut()?, website)?;
            }
            DdlUndo::DeleteExtensionWebsiteRelease { website, version } => {
                let _ = delete_website_release(self.db_mut()?, &website, &version)?;
            }
            DdlUndo::RestoreExtensionWebsiteRelease { release } => {
                let _ = delete_website_release(self.db_mut()?, &release.website, &release.version)?;
                restore_website_release(self.db_mut()?, release)?;
            }
        }
        Ok(())
    }

    pub(crate) fn execute_set(&mut self, set: &Set) -> Result<SqlResult> {
        match set {
            Set::SetTransaction {
                modes, snapshot, ..
            } => {
                if snapshot.is_some() {
                    return Err(SqlError::Unsupported(
                        "transaction snapshots are not supported".to_string(),
                    ));
                }
                validate_set_transaction_modes(modes)?;
            }
            Set::SingleAssignment {
                scope,
                variable,
                values,
                ..
            } => {
                let local = matches!(scope, Some(sqlparser::ast::ContextModifier::Local));
                let setting = object_name(variable)?.to_ascii_lowercase();
                let targets = GucAssignmentTargets::setting(&setting);
                if setting == "search_path" {
                    self.apply_scoped_guc_change(local, targets, |session| {
                        session.apply_search_path_setting(values)
                    })?;
                } else {
                    let [value] = values.as_slice() else {
                        return Err(SqlError::Unsupported(
                            "SET supports one value per BicDB setting".to_string(),
                        ));
                    };
                    self.apply_scoped_guc_change(local, targets, |session| {
                        session.apply_setting(variable, value)
                    })?;
                }
            }
            Set::MultipleAssignments { assignments } => {
                for assignment in assignments {
                    let local = matches!(
                        assignment.scope,
                        Some(sqlparser::ast::ContextModifier::Local)
                    );
                    let setting = object_name(&assignment.name)?.to_ascii_lowercase();
                    let targets = GucAssignmentTargets::setting(&setting);
                    self.apply_scoped_guc_change(local, targets, |session| {
                        session.apply_setting(&assignment.name, &assignment.value)
                    })?;
                }
            }
            Set::SetTimeZone { local, value } => {
                self.apply_scoped_guc_change(
                    *local,
                    GucAssignmentTargets::setting("timezone"),
                    |session| session.apply_timezone_setting(value),
                )?;
            }
            Set::SetRole {
                context_modifier,
                role_name,
            } => {
                let local = matches!(
                    context_modifier,
                    Some(sqlparser::ast::ContextModifier::Local)
                );
                self.apply_scoped_guc_change(
                    local,
                    GucAssignmentTargets::setting(CURRENT_ROLE_GUC),
                    |session| match role_name {
                        Some(role) => session.apply_set_role(&ident_value(role)),
                        None => session.apply_set_role("none"),
                    },
                )?;
            }
            Set::SetSessionAuthorization(param) => {
                let local = param.scope == sqlparser::ast::ContextModifier::Local;
                self.apply_scoped_guc_change(
                    local,
                    GucAssignmentTargets::setting(SESSION_AUTHORIZATION_GUC),
                    |session| match &param.kind {
                        SetSessionAuthorizationParamKind::Default => {
                            session.apply_set_session_authorization(None)
                        }
                        SetSessionAuthorizationParamKind::User(user) => {
                            session.apply_set_session_authorization(Some(&ident_value(user)))
                        }
                    },
                )?;
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported SET statement {other}"
                )));
            }
        }
        Ok(SqlResult::command("SET"))
    }

    fn session_user_is_superuser(&self) -> Result<bool> {
        let session_user = session_user_from_gucs(&self.session_gucs);
        if session_user == BOOTSTRAP_ROLE_NAME {
            return Ok(true);
        }
        Ok(load_role_schema(self.db_ref(), &session_user)?.is_some_and(|role| role.superuser))
    }

    /// Superuser status of the EFFECTIVE role.
    ///
    /// Authorization decisions belong here, not on `session_user`: `SET ROLE`
    /// must actually drop privilege, which is the whole point of the pattern
    /// where a pooler or application lowers its role before running
    /// generated or untrusted SQL. Checking `session_user` leaves those gates
    /// wide open for the entire life of a superuser-authenticated connection.
    ///
    /// The one legitimate exception is the `SET ROLE` check itself, which
    /// PostgreSQL evaluates against the session user's memberships — see
    /// `execute_set_role`.
    pub(crate) fn current_user_is_superuser(&self) -> Result<bool> {
        let current_user = current_user_from_gucs(&self.session_gucs);
        if current_user == BOOTSTRAP_ROLE_NAME {
            return Ok(true);
        }
        Ok(load_role_schema(self.db_ref(), &current_user)?.is_some_and(|role| role.superuser))
    }

    fn current_user_can_manage_roles(&self) -> Result<bool> {
        let current_user = current_user_from_gucs(&self.session_gucs);
        if current_user == BOOTSTRAP_ROLE_NAME {
            return Ok(true);
        }
        Ok(load_role_schema(self.db_ref(), &current_user)?
            .is_some_and(|role| role.superuser || role.create_role))
    }

    fn session_user_can_manage_roles(&self) -> Result<bool> {
        let session_user = session_user_from_gucs(&self.session_gucs);
        if session_user == BOOTSTRAP_ROLE_NAME {
            return Ok(true);
        }
        Ok(load_role_schema(self.db_ref(), &session_user)?
            .is_some_and(|role| role.superuser || role.create_role))
    }

    fn require_role_management_privilege(&self, action: &str) -> Result<()> {
        if self.current_user_can_manage_roles()? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied to {action}"
        ))))
    }

    /// The table an index belongs to, if it can be resolved.
    ///
    /// Index DDL is authority over the indexed relation, so the owner test
    /// has to be asked about the table rather than the index. `None` means
    /// the index is unknown, which the caller reports as its own error
    /// rather than as an authorization decision.
    pub(crate) fn table_owning_index(&self, index: &str) -> Result<Option<String>> {
        if let Some(schema) = list_schemas(self.db_ref())?.into_iter().find(|schema| {
            schema
                .indexes
                .iter()
                .any(|candidate| candidate.name.eq_ignore_ascii_case(index))
        }) {
            return Ok(Some(schema.name));
        }
        // Materialized-view indexes are recorded on the VIEW, not on a table
        // schema. Missing them here resolved the owning relation to `None`,
        // and the DROP INDEX gate skips its check when that happens — so a
        // matview index could be dropped by anyone.
        if let Some(view) = list_views(self.db_ref())?.into_iter().find(|view| {
            view.indexes
                .iter()
                .any(|candidate| candidate.name.eq_ignore_ascii_case(index))
        }) {
            return Ok(Some(view.name));
        }
        Ok(self
            .db_ref()
            .index_definitions()
            .into_iter()
            .find(|definition| definition.name.eq_ignore_ascii_case(index))
            .map(|definition| definition.collection))
    }

    /// The object-DDL authorization layer.
    ///
    /// GRANT/REVOKE, ALTER TABLE and policy DDL all mutate a table's security
    /// posture, and none of them checked anything: any authenticated user
    /// could grant themselves SELECT, `ALTER TABLE ... DISABLE ROW LEVEL
    /// SECURITY`, take ownership, or attach a permissive `USING (true)`
    /// policy — collapsing both the GRANT and the RLS boundary in one
    /// statement. PostgreSQL requires ownership (or superuser) for every one
    /// of these; this is that single missing check, in one place so the
    /// sites cannot drift apart again.
    ///
    /// Schemas persisted before ownership tracking carry no owner and are
    /// treated as bootstrap-owned, matching the documented convention on
    /// `TableSchema::owner` and the existing view/sequence/type checks.
    pub(crate) fn require_table_ownership(&self, table: &str, action: &str) -> Result<()> {
        // Evaluated against the EFFECTIVE role, like PostgreSQL: `SET ROLE`
        // must actually drop privilege, so checking `session_user` here would
        // leave every gate open for a superuser-launched session.
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        let Some(schema) = load_schema(self.db_ref(), table)? else {
            // A missing TABLE schema is not the same as a missing relation:
            // views live in their own store, so returning early here made
            // every gate built on this function fail OPEN for views. Resolve
            // the view before giving up.
            if let Some(view) = load_view(self.db_ref(), table)? {
                let owner = view.owner.unwrap_or_else(current_role_name);
                return self.require_object_ownership(&owner, &format!("relation {table}"), action);
            }
            // Genuinely nonexistent relations are the caller's error to
            // report, not an authorization decision.
            return Ok(());
        };
        let owner = schema
            .owner
            .clone()
            .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
        if self.current_user_holds_role(&owner)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of relation {table} to {action}"
        ))))
    }

    /// Instance-wide operations with no per-table scope: extension install and
    /// activation (executable WASM that registers HTTP routes, event handlers
    /// and queue consumers), audit-history trimming, and the store-wide
    /// vacuum. None of these can be authorized against a relation, and all of
    /// them are administrative, so they take the administrative gate.
    pub(crate) fn require_superuser_for_admin_operation(&self, operation: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be superuser to {operation}"
        ))))
    }

    /// PostgreSQL requires the REFERENCES privilege on the table a foreign
    /// key points AT, and this is why: an FK to a relation you cannot read is
    /// both a cross-tenant existence oracle (the insert succeeds only if the
    /// parent row exists, so the parent's keys can be enumerated without
    /// SELECT) and a lock-in — the parent's owner can no longer delete the
    /// referenced rows, and cannot drop the constraint because it lives on
    /// someone else's table. It is also what makes a referential CASCADE into
    /// an unowned child legitimate: the link can only exist if someone with
    /// authority over the parent allowed it.
    pub(crate) fn require_reference_privilege(
        &self,
        child_table: &str,
        constraints: &[ConstraintSchema],
    ) -> Result<()> {
        for constraint in constraints {
            let ConstraintSchema::ForeignKey { foreign_table, .. } = constraint else {
                continue;
            };
            // A self-reference needs no separate authority: the caller is
            // already creating or altering this very table.
            if foreign_table.eq_ignore_ascii_case(child_table) {
                continue;
            }
            let parent = resolve_session_relation_name(self.db_ref(), foreign_table)
                .unwrap_or_else(|_| foreign_table.clone());
            self.require_table_privilege(&parent, "REFERENCES")?;
        }
        Ok(())
    }

    /// Whether the effective role IS `role`, or holds it indirectly.
    /// PostgreSQL treats a member of the owning role as an owner, so this
    /// walks memberships transitively, guarding against membership cycles.
    /// The single definition of "holds this role's authority" — every
    /// ownership gate below is built on it so they cannot drift apart.
    pub(crate) fn current_user_holds_role(&self, role: &str) -> Result<bool> {
        let current_user = current_user_from_gucs(&self.session_gucs);
        if current_user.eq_ignore_ascii_case(role) {
            return Ok(true);
        }
        Ok(role_privilege_closure(self.db_ref(), &current_user)?
            .contains(&normalize_role_name(role)))
    }

    /// "must be owner of <object>" for every object kind that is not a table:
    /// views, sequences, schemas, databases. Tables keep their own entry
    /// point only because they resolve their owner from a `TableSchema`.
    pub(crate) fn require_object_ownership(
        &self,
        owner: &str,
        object: &str,
        action: &str,
    ) -> Result<()> {
        if self.current_user_is_superuser()? || self.current_user_holds_role(owner)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of {object} to {action}"
        ))))
    }

    /// The other half of every `ALTER ... OWNER TO` gate: you may only hand
    /// an object to a role you could `SET ROLE` to. Without this, ownership
    /// is a laundering primitive — a view handed to a privileged role keeps
    /// executing with that role's read authority (definer semantics), so the
    /// giver gains everything the receiver can read.
    pub(crate) fn require_settable_new_owner(&self, new_owner: &str, object: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        let current_user = current_user_from_gucs(&self.session_gucs);
        if settable_role_closure(self.db_ref(), &current_user)?
            .contains(&normalize_role_name(new_owner))
        {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied to change owner of {object} to \"{new_owner}\""
        ))))
    }

    /// Ownership of a relation that may be a table OR a view. Views live in
    /// their own store, so a table-only lookup silently returns "no schema"
    /// and every gate built on it fails OPEN for views.
    pub(crate) fn require_relation_ownership(&self, name: &str, action: &str) -> Result<()> {
        self.require_table_ownership(name, action)
    }

    fn require_superuser_for_privileged_role_attributes(&self, action: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be superuser to {action} SUPERUSER, BYPASSRLS, CREATEROLE, or REPLICATION"
        ))))
    }

    fn ensure_role_target_manageable(&self, role: &RoleSchema, action: &str) -> Result<()> {
        if self.current_user_is_superuser()?
            || (!role.superuser && !role.bypass_rls && !role.create_role && !role.replication)
        {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be superuser to {action} privileged role \"{}\"",
            role.name
        ))))
    }

    pub(crate) fn apply_set_role(&mut self, role: &str) -> Result<()> {
        let role = normalize_role_name(role);
        if role == "none" {
            Arc::make_mut(&mut self.session_gucs).remove(CURRENT_ROLE_GUC);
            return Ok(());
        }
        if !role_exists(self.db_ref(), &role)? {
            return Err(SqlError::UndefinedRole { name: role });
        }
        // Like PostgreSQL, the privilege check is against the session user's
        // memberships, not the current role's.
        let session_user = session_user_from_gucs(&self.session_gucs);
        let allowed = self.session_user_is_superuser()?
            || settable_role_closure(self.db_ref(), &session_user)?.contains(&role);
        if !allowed {
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied to set role \"{role}\""
            ))));
        }
        Arc::make_mut(&mut self.session_gucs).insert(CURRENT_ROLE_GUC.to_string(), role);
        Ok(())
    }

    pub(crate) fn apply_set_session_authorization(&mut self, user: Option<&str>) -> Result<()> {
        let initial = initial_session_user_from_gucs(&self.session_gucs);
        // Local/embedded sessions do not arrive through pgwire's startup
        // identity seeding. Capture their original bootstrap identity before
        // the first SET so RESET cannot mistake the assumed identity for the
        // authenticated one.
        Arc::make_mut(&mut self.session_gucs)
            .entry(INITIAL_SESSION_AUTHORIZATION_GUC.to_string())
            .or_insert_with(|| initial.clone());
        let target = match user {
            Some(user) => normalize_role_name(user),
            None => initial.clone(),
        };
        // Restoring the connect-time identity must always work, even when the
        // authenticated pgwire user has no SQL role record.
        if target != initial && !role_exists(self.db_ref(), &target)? {
            return Err(SqlError::UndefinedRole { name: target });
        }
        // Only sessions whose connect-time (authenticated) user is a
        // superuser may assume another identity; restoring the connect-time
        // identity is always allowed.
        let initial_is_superuser = initial == BOOTSTRAP_ROLE_NAME
            || load_role_schema(self.db_ref(), &initial)?.is_some_and(|role| role.superuser);
        if target != initial && !initial_is_superuser {
            return Err(SqlError::BicDb(BicDbError::Authorization(
                "permission denied to set session authorization".to_string(),
            )));
        }
        Arc::make_mut(&mut self.session_gucs).insert(SESSION_AUTHORIZATION_GUC.to_string(), target);
        Arc::make_mut(&mut self.session_gucs).remove(CURRENT_ROLE_GUC);
        Ok(())
    }

    pub(crate) fn execute_raw_reset(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        if !statements
            .iter()
            .all(|statement| normalize_sql(statement).starts_with("reset "))
        {
            return Ok(None);
        }
        for statement in statements {
            let previous_gucs = self.session_gucs.clone();
            let previous_settings = self.settings;
            let targets = self.reset_setting(&statement)?;
            self.reconcile_guc_assignments(previous_gucs, previous_settings, false);
            targets.clear_local_restore(self);
        }
        Ok(Some(SqlResult::command("RESET")))
    }

    fn reset_setting(&mut self, sql: &str) -> Result<GucAssignmentTargets> {
        let normalized = normalize_sql(sql);
        let setting = normalized
            .strip_prefix("reset ")
            .ok_or_else(|| SqlError::InvalidSql(format!("invalid RESET statement {sql}")))?
            .trim();
        if setting.eq_ignore_ascii_case("session authorization") {
            self.apply_set_session_authorization(None)?;
            return Ok(GucAssignmentTargets::setting(SESSION_AUTHORIZATION_GUC));
        }
        if setting.eq_ignore_ascii_case("all") {
            let targets = GucAssignmentTargets::reset_all(&self.guc_local_restore);
            // PostgreSQL's RESET ALL leaves the session identity in place.
            let preserved = [
                SESSION_AUTHORIZATION_GUC,
                CURRENT_ROLE_GUC,
                INITIAL_SESSION_AUTHORIZATION_GUC,
                "server_version",
                "server_version_num",
            ];
            Arc::make_mut(&mut self.session_gucs).retain(|key, _| {
                preserved.contains(&key.as_str())
                    || key == POSTGRES_VERSION_BANNER_GUC
                    || is_protected_security_setting(key)
            });
            self.settings = SqlSettings::default();
            return Ok(targets);
        }
        let raw = setting.split_whitespace().next().unwrap_or(setting);
        // Keep dotted GUC names whole; normalize_object_name would strip the
        // qualifier ("app.tenant" -> "tenant").
        let setting = if raw.contains('.') {
            raw.trim_matches('"').to_ascii_lowercase()
        } else {
            normalize_object_name(raw)
        };
        let targets = GucAssignmentTargets::setting(&setting);
        if setting == POSTGRES_VERSION_BANNER_GUC {
            return Err(protected_security_setting_error(&setting));
        }
        if is_protected_security_setting(&setting)
            && !self.postgres_compatibility_security_gucs_enabled()
        {
            return Err(protected_security_setting_error(&setting));
        }
        match setting.as_str() {
            "application_name"
            | "client_encoding"
            | "client_min_messages"
            | "check_function_bodies"
            | "datestyle"
            | "default_table_access_method"
            | "default_tablespace"
            | "extra_float_digits"
            | "bytea_output"
            | "standard_conforming_strings"
            | "intervalstyle"
            | "xmloption"
            | "jit"
            | "statement_timeout"
            | "lock_timeout"
            | "idle_in_transaction_session_timeout"
            | "idle_session_timeout"
            | "transaction_timeout"
            | "restrict_nonsystem_relation_kind"
            | "row_security"
            | "search_path"
            | "synchronize_seqscans"
            | "timezone" => {
                Arc::make_mut(&mut self.session_gucs).remove(&setting);
                Ok(())
            }
            "bicdb.vector_search" => {
                self.settings.vector_search = SqlSettings::default().vector_search;
                Ok(())
            }
            "bicdb.ef_search" => {
                self.settings.ef_search = SqlSettings::default().ef_search;
                Ok(())
            }
            "role" => self.apply_set_role("none"),
            "session_authorization" => self.apply_set_session_authorization(None),
            INITIAL_SESSION_AUTHORIZATION_GUC | RLS_CHECK_AS_GUC => Err(SqlError::BicDb(
                BicDbError::Authorization(format!("parameter \"{setting}\" cannot be changed")),
            )),
            other if other.contains('.') => {
                Arc::make_mut(&mut self.session_gucs).remove(other);
                Ok(())
            }
            other => Err(SqlError::Unsupported(format!(
                "RESET {other} is not supported"
            ))),
        }?;
        Ok(targets)
    }

    fn discard_all(&mut self) -> Result<()> {
        if self.in_transaction() {
            return Err(SqlError::InvalidSql(
                "DISCARD ALL cannot run inside a transaction block".to_string(),
            ));
        }
        let database = self.session_gucs.get("database").cloned();
        let initial_user = self
            .session_gucs
            .get(INITIAL_SESSION_AUTHORIZATION_GUC)
            .cloned();
        let postgres_version_banner = self.session_gucs.get(POSTGRES_VERSION_BANNER_GUC).cloned();
        let server_version = self.session_gucs.get("server_version").cloned();
        let server_version_num = self.session_gucs.get("server_version_num").cloned();
        self.settings = SqlSettings::default();
        Arc::make_mut(&mut self.session_gucs).clear();
        if let Some(database) = database {
            Arc::make_mut(&mut self.session_gucs).insert("database".to_string(), database);
        }
        if let Some(initial_user) = initial_user {
            Arc::make_mut(&mut self.session_gucs).insert(
                INITIAL_SESSION_AUTHORIZATION_GUC.to_string(),
                initial_user.clone(),
            );
            Arc::make_mut(&mut self.session_gucs)
                .insert(SESSION_AUTHORIZATION_GUC.to_string(), initial_user);
        }
        if let Some(postgres_version_banner) = postgres_version_banner {
            Arc::make_mut(&mut self.session_gucs).insert(
                POSTGRES_VERSION_BANNER_GUC.to_string(),
                postgres_version_banner,
            );
        }
        if let Some(server_version) = server_version {
            Arc::make_mut(&mut self.session_gucs)
                .insert("server_version".to_string(), server_version);
        }
        if let Some(server_version_num) = server_version_num {
            Arc::make_mut(&mut self.session_gucs)
                .insert("server_version_num".to_string(), server_version_num);
        }
        self.guc_transaction_start = None;
        self.transaction_timestamp_seconds = None;
        self.guc_local_restore.clear();
        self.guc_local_settings_restore = SqlSettingsRestore::default();
        self.currvals.clear();
        self.rebind_trusted_security_settings();
        Ok(())
    }

    pub(crate) fn execute_raw_comment(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Ok(None);
        }
        if !statements
            .iter()
            .all(|statement| normalize_sql(statement).starts_with("comment on "))
        {
            return Ok(None);
        }
        for statement in statements {
            let normalized = normalize_sql(&statement);
            if !normalized.starts_with("comment on type ")
                && !normalized.starts_with("comment on domain ")
            {
                // Comments on other object kinds are accepted as metadata-only
                // compatibility statements. In particular, sqlparser does not
                // accept PostgreSQL's function-signature syntax here.
                continue;
            }
            let parsed = parse_statements(&statement)?;
            let [Statement::Comment {
                object_type,
                object_name,
                comment,
                if_exists,
            }] = parsed.as_slice()
            else {
                continue;
            };
            if !matches!(object_type, CommentObject::Type | CommentObject::Domain) {
                continue;
            }
            let (schema_name, name) = user_type_identity(object_name)?;
            let Some(mut user_type) = load_user_type(self.db_ref(), &schema_name, &name)? else {
                if *if_exists {
                    continue;
                }
                return Err(SqlError::undefined_type(format!("{schema_name}.{name}")));
            };
            let is_domain = matches!(user_type.kind, UserTypeKind::Domain { .. });
            if matches!(object_type, CommentObject::Domain) != is_domain {
                return Err(SqlError::data_exception(
                    "42809",
                    format!("{schema_name}.{name} has the wrong object type"),
                    Some(name),
                ));
            }
            self.ensure_user_type_owner(&user_type)?;
            user_type.comment = comment.clone();
            self.save_session_user_type(&user_type)?;
        }
        Ok(Some(SqlResult::command("COMMENT")))
    }

    pub(crate) fn execute_raw_alter_user_type(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(ddl) = parse_raw_alter_user_type(sql)? else {
            return Ok(None);
        };
        let (identity, domain) = match &ddl {
            RawAlterUserTypeDdl::Owner {
                identity, domain, ..
            }
            | RawAlterUserTypeDdl::SetSchema {
                identity, domain, ..
            } => (identity, *domain),
        };
        let mut user_type = load_user_type(self.db_ref(), &identity.schema_name, &identity.name)?
            .ok_or_else(|| {
            SqlError::undefined_type(format!("{}.{}", identity.schema_name, identity.name))
        })?;
        let is_domain = matches!(user_type.kind, UserTypeKind::Domain { .. });
        if domain != is_domain {
            return Err(SqlError::data_exception(
                "42809",
                format!(
                    "{}.{} has the wrong object type",
                    identity.schema_name, identity.name
                ),
                Some(identity.name.clone()),
            ));
        }
        self.ensure_user_type_owner(&user_type)?;
        match ddl {
            RawAlterUserTypeDdl::Owner { owner, .. } => {
                ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                let current_user = current_user_from_gucs(&self.session_gucs);
                let superuser = load_role_schema(self.db_ref(), &current_user)?
                    .is_some_and(|role| role.superuser);
                if !superuser
                    && !settable_role_closure(self.db_ref(), &current_user)?.contains(&owner)
                {
                    return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                        "permission denied to change type owner to \"{owner}\""
                    ))));
                }
                let previous_owner = user_type.owner.clone();
                if user_type.acl_explicit {
                    let previous_owner_grant = PrivilegeGrant {
                        column: None,
                        object_type: PrivilegeObjectType::Type,
                        object_name: user_type_privilege_name(
                            &user_type.schema_name,
                            &user_type.name,
                        ),
                        grantee: previous_owner,
                        privilege: "USAGE".to_string(),
                    };
                    let had_owner_grant = list_privileges(self.db_ref())?
                        .into_iter()
                        .any(|grant| grant == previous_owner_grant);
                    if had_owner_grant {
                        self.delete_session_privilege(&previous_owner_grant)?;
                        let mut new_owner_grant = previous_owner_grant;
                        new_owner_grant.grantee = owner.clone();
                        self.save_session_privilege(&new_owner_grant)?;
                    }
                }
                user_type.owner = owner;
                self.save_session_user_type(&user_type)?;
            }
            RawAlterUserTypeDdl::SetSchema { schema_name, .. } => {
                if !is_known_schema(&schema_name)
                    && load_namespace(self.db_ref(), &schema_name)?.is_none()
                {
                    return Err(SqlError::InvalidCollection(schema_name));
                }
                if schema_name.eq_ignore_ascii_case(&user_type.schema_name) {
                    return Ok(Some(SqlResult::command(if domain {
                        "ALTER DOMAIN"
                    } else {
                        "ALTER TYPE"
                    })));
                }
                if load_user_type(self.db_ref(), &schema_name, &user_type.name)?.is_some() {
                    return Err(SqlError::data_exception(
                        "42710",
                        format!("type \"{}\" already exists", user_type.name),
                        Some(user_type.name.clone()),
                    ));
                }
                let previous = user_type.clone();
                user_type.schema_name = schema_name;
                self.relocate_user_type(&previous, &user_type)?;
            }
        }
        Ok(Some(SqlResult::command(if domain {
            "ALTER DOMAIN"
        } else {
            "ALTER TYPE"
        })))
    }

    pub(crate) fn execute_raw_type_privileges(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(ddl) = parse_raw_type_privilege_ddl(sql)? else {
            return Ok(None);
        };
        let roles = list_roles(self.db_ref())?;
        let mut user_types = Vec::with_capacity(ddl.types.len());
        for identity in &ddl.types {
            let mut user_type = load_user_type(
                self.db_ref(),
                &identity.schema_name,
                &identity.name,
            )?
            .ok_or_else(|| {
                SqlError::undefined_type(format!("{}.{}", identity.schema_name, identity.name))
            })?;
            self.ensure_user_type_owner(&user_type)?;
            if !user_type.acl_explicit {
                let object_name = user_type_privilege_name(&user_type.schema_name, &user_type.name);
                for grantee in ["public", user_type.owner.as_str()] {
                    self.save_session_privilege(&PrivilegeGrant {
                        column: None,
                        object_type: PrivilegeObjectType::Type,
                        object_name: object_name.clone(),
                        grantee: grantee.to_string(),
                        privilege: "USAGE".to_string(),
                    })?;
                }
                user_type.acl_explicit = true;
                self.save_session_user_type(&user_type)?;
            }
            user_types.push(user_type);
        }
        for grantee in ddl.grantees {
            if grantee != "public" && !roles.iter().any(|role| role.name == grantee) {
                return Err(SqlError::UndefinedRole { name: grantee });
            }
            for user_type in &user_types {
                let grant = PrivilegeGrant {
                    column: None,
                    object_type: PrivilegeObjectType::Type,
                    object_name: user_type_privilege_name(&user_type.schema_name, &user_type.name),
                    grantee: grantee.clone(),
                    privilege: "USAGE".to_string(),
                };
                if ddl.grant {
                    self.save_session_privilege(&grant)?;
                } else {
                    self.delete_session_privilege(&grant)?;
                }
            }
        }
        Ok(Some(SqlResult::command(if ddl.grant {
            "GRANT"
        } else {
            "REVOKE"
        })))
    }

    fn ensure_user_type_owner(&self, user_type: &UserTypeSchema) -> Result<()> {
        let current_user = current_user_from_gucs(&self.session_gucs);
        if user_type.owner.eq_ignore_ascii_case(&current_user)
            || load_role_schema(self.db_ref(), &current_user)?.is_some_and(|role| role.superuser)
            || role_privilege_closure(self.db_ref(), &current_user)?
                .contains(&normalize_role_name(&user_type.owner))
        {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of type {}.{}",
            user_type.schema_name, user_type.name
        ))))
    }

    fn ensure_user_type_usage(&self, column_type: Option<&UserTypeColumnSchema>) -> Result<()> {
        let Some(column_type) = column_type else {
            return Ok(());
        };
        let Some(user_type) = load_user_type_by_oid(self.db_ref(), column_type.oid)? else {
            // Missing type metadata is not permission to use the type. Fail
            // closed: a column whose user type has vanished cannot be
            // authorized against anything.
            if self.current_user_is_superuser()? {
                return Ok(());
            }
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for type with oid {}",
                column_type.oid
            ))));
        };
        let role = current_user_from_gucs(&self.session_gucs);
        if role_can_use_user_type(self.db_ref(), &role, &user_type)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for type {}.{}",
            user_type.schema_name, user_type.name
        ))))
    }
}

fn index_expr_collation_oid(columns: &[ColumnSchema], expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Collate { expr, collation } => {
            if index_expr_collation_oid(columns, expr)? == 0 {
                return Err(SqlError::data_exception(
                    "42804",
                    format!("collations are not supported by type of expression {expr}"),
                    None,
                ));
            }
            let name = normalize_column_collation(collation)?;
            Ok(collation_oid(&name).unwrap_or(0))
        }
        Expr::Identifier(ident) => Ok(columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
            .filter(|column| pg_type_is_collatable(&column.pg_type))
            .map(ColumnSchema::collation_oid)
            .unwrap_or(0)),
        Expr::CompoundIdentifier(idents) => Ok(idents
            .last()
            .and_then(|ident| {
                columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
            })
            .filter(|column| pg_type_is_collatable(&column.pg_type))
            .map(ColumnSchema::collation_oid)
            .unwrap_or(0)),
        Expr::Nested(expr) => index_expr_collation_oid(columns, expr),
        Expr::Cast { data_type, .. } => {
            let (pg_type, _) = pg_type_from_data_type(data_type)?;
            Ok(if pg_type_is_collatable(&pg_type) {
                100
            } else {
                0
            })
        }
        Expr::Value(_) | Expr::TypedString(_) => Ok(100),
        _ => Ok(0),
    }
}

fn returning_projection_column_types(
    returning: &[SelectItem],
    schema: Option<&TableSchema>,
) -> Vec<Option<String>> {
    let mut types = Vec::new();
    for item in returning {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                types.extend(
                    FieldRef::wildcard(schema)
                        .iter()
                        .map(|field| field_pg_type(field, schema)),
                );
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                types.push(projected_expr_pg_type(expr, schema));
            }
            _ => {}
        }
    }
    types
}

fn returning_projection_column_metadata(
    db: &BicDb,
    table: &str,
    returning: &[SelectItem],
    schema: Option<&TableSchema>,
) -> Vec<SqlColumnMetadata> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    let table_oid = table_oids(db).get(table).copied().unwrap_or_default() as i32;
    let metadata_for_column = |column: &ColumnSchema| SqlColumnMetadata {
        table_oid,
        attribute_number: schema
            .columns
            .iter()
            .position(|candidate| candidate.name.eq_ignore_ascii_case(&column.name))
            .and_then(|index| i16::try_from(index + 1).ok())
            .unwrap_or_default(),
        type_modifier: column.catalog_typmod() as i32,
    };
    let direct_column = |expr: &Expr| {
        let name = match expr {
            Expr::Identifier(identifier) => Some(identifier.value.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.as_str()),
            Expr::Nested(inner) => match inner.as_ref() {
                Expr::Identifier(identifier) => Some(identifier.value.as_str()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.as_str()),
                _ => None,
            },
            _ => None,
        }?;
        schema
            .columns
            .iter()
            .find(|column| !column.hidden && column.name.eq_ignore_ascii_case(name))
            .map(&metadata_for_column)
    };
    let mut metadata = Vec::new();
    for item in returning {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => metadata.extend(
                schema
                    .columns
                    .iter()
                    .filter(|column| !column.hidden)
                    .map(&metadata_for_column),
            ),
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                metadata.push(direct_column(expr).unwrap_or_default());
            }
            _ => {}
        }
    }
    metadata
}

/// Split on commas at paren depth zero, so `COUNT(DISTINCT host)` survives.
fn split_top_level(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    for character in input.chars() {
        match character {
            '(' => {
                depth += 1;
                current.push(character);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(character);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(character),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// Contents of the parenthesised list following `keyword`, if present.
fn clause_list(body: &str, keyword: &str) -> Option<String> {
    let upper = body.to_ascii_uppercase();
    let at = upper.find(keyword)?;
    let rest = &body[at + keyword.len()..];
    let open = rest.find('(')?;
    let mut depth = 0usize;
    for (offset, character) in rest[open..].char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(rest[open + 1..open + offset].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// A parsed `CREATE CUBE`.
struct CubeDefinition {
    name: String,
    collection: String,
    dimensions: Vec<String>,
    measures: Vec<String>,
    sketches: Vec<bicdb_core::aggregate_projection::SketchSpec>,
    force: bool,
}

/// `CREATE CUBE <name> ON <collection> DIMENSIONS (...) [MEASURES (...)] [WITH FORCE]`
///
/// Measure forms map one-to-one onto what the engine can actually maintain:
/// `SUM(x)` / bare `x` are additive, `COUNT(DISTINCT x)` and `PERCENTILES(x)`
/// are sketches. Nothing here promises an aggregate the engine cannot retract
/// or merge.
fn parse_create_cube(sql: &str) -> Result<Option<CubeDefinition>> {
    use bicdb_core::aggregate_projection::{SketchKind, SketchSpec};
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(rest) = upper.strip_prefix("CREATE CUBE") else {
        return Ok(None);
    };
    let rest = &trimmed[trimmed.len() - rest.len()..];
    let upper_rest = rest.to_ascii_uppercase();
    let Some(on_at) = upper_rest.find(" ON ") else {
        return Err(SqlError::InvalidSql(
            "CREATE CUBE requires `ON <table>`".to_string(),
        ));
    };
    let name = rest[..on_at].trim().trim_matches('"').to_string();
    if name.is_empty() {
        return Err(SqlError::InvalidSql(
            "CREATE CUBE requires a cube name".to_string(),
        ));
    }
    let body = &rest[on_at + 4..];
    let upper_body = body.to_ascii_uppercase();
    let collection_end = upper_body
        .find("DIMENSIONS")
        .ok_or_else(|| SqlError::InvalidSql("CREATE CUBE requires DIMENSIONS (...)".to_string()))?;
    let collection = body[..collection_end].trim().trim_matches('"').to_string();
    if collection.is_empty() {
        return Err(SqlError::InvalidSql(
            "CREATE CUBE requires a table to aggregate".to_string(),
        ));
    }

    let dimensions: Vec<String> = clause_list(body, "DIMENSIONS")
        .map(|list| {
            split_top_level(&list)
                .into_iter()
                .map(|item| item.trim_matches('"').to_string())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if dimensions.is_empty() {
        return Err(SqlError::InvalidSql(
            "CREATE CUBE requires at least one dimension".to_string(),
        ));
    }

    let mut measures = Vec::new();
    let mut sketches = Vec::new();
    for item in clause_list(body, "MEASURES")
        .map(|list| split_top_level(&list))
        .unwrap_or_default()
    {
        let upper_item = item.to_ascii_uppercase();
        let inner = |item: &str| -> String {
            item[item.find('(').map(|at| at + 1).unwrap_or(0)..]
                .trim_end_matches(')')
                .trim()
                .trim_matches('"')
                .to_string()
        };
        if upper_item.starts_with("COUNT(DISTINCT") || upper_item.starts_with("COUNT (DISTINCT") {
            let path = inner(&item)
                .trim_start_matches(|c: char| c.is_alphabetic() || c.is_whitespace())
                .to_string();
            // `DISTINCT host` -> `host`
            let path = if path.is_empty() {
                inner(&item)
                    .split_whitespace()
                    .last()
                    .unwrap_or_default()
                    .to_string()
            } else {
                path
            };
            sketches.push(SketchSpec {
                name: path.clone(),
                path,
                kind: SketchKind::DistinctCount,
            });
        } else if upper_item.starts_with("PERCENTILES") || upper_item.starts_with("QUANTILES") {
            let path = inner(&item);
            sketches.push(SketchSpec {
                name: path.clone(),
                path,
                kind: SketchKind::Quantile,
            });
        } else if upper_item.starts_with("SUM") || upper_item.starts_with("AVG") {
            measures.push(inner(&item));
        } else {
            measures.push(item.trim_matches('"').to_string());
        }
    }

    Ok(Some(CubeDefinition {
        name,
        collection,
        dimensions,
        measures,
        sketches,
        force: upper_body.contains("WITH FORCE"),
    }))
}

/// Render aggregate cells as a result set. Shared by ROLLUP (G8) and MERGE
/// (G9) so the two cannot drift into presenting the same numbers differently.
fn render_aggregate_cells(
    dimension_names: &[String],
    measure_names: &[String],
    sketch_specs: &[bicdb_core::aggregate_projection::SketchSpec],
    cells: impl IntoIterator<
        Item = (
            bicdb_core::aggregate_projection::CellKey,
            bicdb_core::aggregate_projection::CellState,
            Vec<bicdb_core::aggregate_projection::SketchState>,
        ),
    >,
) -> SqlResult {
    use bicdb_core::aggregate_projection::{DimensionValue, SketchKind};
    let mut columns: Vec<String> = dimension_names.to_vec();
    columns.push("count".to_string());
    for measure in measure_names {
        columns.push(format!("sum_{measure}"));
        columns.push(format!("avg_{measure}"));
    }
    for spec in sketch_specs {
        match spec.kind {
            SketchKind::DistinctCount => columns.push(format!("distinct_{}", spec.name)),
            SketchKind::Quantile => {
                columns.push(format!("p50_{}", spec.name));
                columns.push(format!("p95_{}", spec.name));
            }
        }
    }

    let mut rows = Vec::new();
    for (key, state, sketches) in cells {
        let mut row: Vec<SqlValue> = key
            .iter()
            .map(|value| match value {
                DimensionValue::Text(text) => SqlValue::String(text.clone()),
                DimensionValue::Int(number) => SqlValue::Int(*number),
                DimensionValue::Bool(flag) => SqlValue::Bool(*flag),
                DimensionValue::Missing => SqlValue::Null,
            })
            .collect();
        row.push(SqlValue::Int(state.count));
        for index in 0..measure_names.len() {
            row.push(SqlValue::Float(state.sum(index)));
            row.push(
                state
                    .avg(index)
                    .map(SqlValue::Float)
                    .unwrap_or(SqlValue::Null),
            );
        }
        for (index, spec) in sketch_specs.iter().enumerate() {
            let sketch = sketches.get(index);
            match spec.kind {
                SketchKind::DistinctCount => row.push(
                    sketch
                        .and_then(|state| state.distinct_estimate())
                        .map(|value| SqlValue::Int(value.round() as i64))
                        .unwrap_or(SqlValue::Null),
                ),
                SketchKind::Quantile => {
                    for fraction in [0.5, 0.95] {
                        row.push(
                            sketch
                                .and_then(|state| state.percentile(fraction))
                                .map(SqlValue::Float)
                                .unwrap_or(SqlValue::Null),
                        );
                    }
                }
            }
        }
        rows.push(row);
    }
    SqlResult::new(columns, rows)
}

/// A requested level for one dimension.
enum RollupSpec {
    Level(bicdb_core::aggregate_projection::RollupLevel),
    /// H3 parent at a coarser resolution.
    H3(u8),
}

/// `(day PREFIX 7, category KEEP, cell H3 6)`.
fn parse_rollup_levels(spec: &str, dimensions: &[String]) -> Result<Vec<RollupSpec>> {
    use bicdb_core::aggregate_projection::RollupLevel;
    let mut levels: Vec<RollupSpec> = dimensions
        .iter()
        .map(|_| RollupSpec::Level(RollupLevel::Keep))
        .collect();
    let spec = spec
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    if spec.is_empty() {
        return Ok(levels);
    }
    for clause in spec.split(',') {
        let parts: Vec<&str> = clause.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(SqlError::InvalidSql(format!(
                "rollup level `{}` must be `<dimension> <level>`",
                clause.trim()
            )));
        }
        let dimension = parts[0].trim_matches('"');
        let index = dimensions
            .iter()
            .position(|name| name.eq_ignore_ascii_case(dimension))
            .ok_or_else(|| {
                SqlError::InvalidSql(format!(
                    "`{dimension}` is not a dimension of this materialized aggregate"
                ))
            })?;
        let argument = |position: usize| -> Result<i64> {
            parts
                .get(position)
                .and_then(|value| {
                    value
                        .trim_matches(|c| c == '(' || c == ')')
                        .parse::<i64>()
                        .ok()
                })
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!(
                        "rollup level `{}` needs a numeric argument",
                        clause.trim()
                    ))
                })
        };
        levels[index] = match parts[1].to_ascii_uppercase().as_str() {
            "KEEP" => RollupSpec::Level(RollupLevel::Keep),
            "WHOLE" | "ALL" => RollupSpec::Level(RollupLevel::Whole),
            "PREFIX" => RollupSpec::Level(RollupLevel::Prefix(argument(2)?.max(0) as usize)),
            "BUCKET" => RollupSpec::Level(RollupLevel::Bucket(argument(2)?)),
            "SEGMENT" => {
                // `SEGMENT / 2` — separator then depth.
                let separator = parts
                    .get(2)
                    .and_then(|value| value.trim_matches('\'').chars().next())
                    .ok_or_else(|| SqlError::InvalidSql("SEGMENT needs a separator".to_string()))?;
                RollupSpec::Level(RollupLevel::Segment {
                    separator,
                    depth: argument(3)?.max(0) as usize,
                })
            }
            "H3" => RollupSpec::H3(argument(2)?.clamp(0, 15) as u8),
            other => {
                return Err(SqlError::InvalidSql(format!(
                    "unknown rollup level `{other}`; expected KEEP, WHOLE, PREFIX, SEGMENT, BUCKET or H3"
                )));
            }
        };
    }
    Ok(levels)
}

/// Coarsen an H3 cell id to a parent resolution. A value that is not a valid
/// cell is passed through unchanged rather than silently becoming null — the
/// rollup should not invent data.
fn h3_parent(
    value: &bicdb_core::aggregate_projection::DimensionValue,
    resolution: u8,
) -> bicdb_core::aggregate_projection::DimensionValue {
    use bicdb_core::aggregate_projection::DimensionValue;
    let Ok(target) = h3o::Resolution::try_from(resolution) else {
        return value.clone();
    };
    let cell = match value {
        DimensionValue::Text(text) => text.parse::<h3o::CellIndex>().ok(),
        DimensionValue::Int(number) => h3o::CellIndex::try_from(*number as u64).ok(),
        _ => None,
    };
    match cell.and_then(|cell| cell.parent(target)) {
        Some(parent) => DimensionValue::Text(parent.to_string()),
        None => value.clone(),
    }
}
