//! SqlSession: the top-level SQL entry point (statement execution/dispatch, transactions and savepoints, DDL orchestration with undo, DML with FK cascades, RLS enforcement, routine invocation) plus DdlUndo and SavepointMark.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use session::*;`.

mod alter_column_identity;
pub(crate) use alter_column_identity::{parse_alter_sequence_parameters, SequenceAlterParameters};
mod collation_ddl;
mod create_schema_elements;
mod create_table_as;
pub(crate) mod deferred_constraints;
mod domain_types;
mod event_triggers;
pub(crate) use event_triggers::{
    event_trigger_function_error, pg_event_trigger_rows, EVENT_TRIGGER_COLLECTION,
    PG_EVENT_TRIGGER_COLUMNS,
};
pub(crate) mod extension_objects;
mod flashback;
mod foreign_data_ddl;
mod index_drop;
mod inheritance_dml;
pub(crate) mod insert_overriding;
mod instead_of_triggers;
pub(crate) mod materialized_views;
pub(crate) mod merge;
pub(crate) mod not_null_constraints;
mod owned_objects;
mod partition_indexes;
mod table_access_method;
pub(crate) mod text_search_ddl;
mod text_search_dictionaries;
#[cfg(test)]
mod timestamp_day_indexes;
mod typed_tables;
pub(crate) mod view_options;
pub(crate) use merge::UpdateMatchPolicy;
pub(crate) mod recycle_bin;
mod recycle_bin_journal;
mod rename_dependencies;
mod rename_journal;
pub use rename_journal::initialize_rename_journal_identities;
mod rename_table;
mod rename_views;
mod view_dependencies;
pub use recycle_bin::finish_committed_recycle_bin_purges;
pub(crate) use recycle_bin::is_recycle_bin_name;
pub use recycle_bin::recover_interrupted_recycle_bin_renames;
pub use recycle_bin::RECYCLE_BIN_COLLECTION;
pub(crate) use recycle_bin_journal::is_catalog_journal_collection;
#[cfg(test)]
mod case_mapped_index_tests;
#[cfg(test)]
mod cast_lookup_tests;
#[cfg(test)]
mod catalog_join_tests;
mod conflict_scope;
#[cfg(test)]
mod driver_metadata_tests;
pub(crate) mod functions_insert;
mod join_lookup_tests;
mod operator_ddl;
pub(crate) mod partition_admission;
mod partition_exec;
mod partition_schema;
pub use partition_admission::{
    apply_partition_upgrade, check_sql_partition_admission, ensure_sql_partition_admission,
    inspect_partition_upgrade, PartitionInvalidRow, PartitionPolicyConflict,
    PartitionPolicyDecision, PartitionRowRelocation, PartitionUpgradeApplied,
    PartitionUpgradeInspection, PartitionUpgradePlan,
};
mod explain_statement;
mod exported_snapshots;
mod partition_triggers;
pub(crate) mod planner_gucs;
#[cfg(test)]
mod primary_key_range_tests;
mod primary_key_updates;
mod privilege_projection;
mod raw_ddl_analyze;
mod raw_index_fts;
mod read_only;
mod row_assignments;
mod row_triggers;
mod sql_cursors;
pub(crate) use row_triggers::current_trigger_depth;
pub use sql_cursors::SqlCursors;
pub(crate) use sql_cursors::{fetch_xml_cursor_rows, pg_cursors_rows, PG_CURSORS_COLUMNS};
mod set_functions;
#[cfg(test)]
mod subquery_scope_tests;
mod temp_tables;
pub(crate) mod temporal_keys;
mod transaction_defaults;
mod vacuum_analyze;
pub use read_only::parse_transaction_read_only;
pub use temp_tables::{
    drop_connection_temp_schema, drop_orphan_temp_schemas, drop_temp_on_commit_tables,
    repair_orphan_views, temp_on_commit_delete_pending, temp_on_commit_drop_pending,
    temp_on_commit_pending, temp_schema_exists,
};
pub(crate) mod txn_update;
pub(crate) mod updatable_views;
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

/// PostgreSQL's trusted contrib extensions (`trusted = true` in their control files).
pub(crate) fn is_trusted_postgres_extension(name: &str) -> bool {
    matches!(
        name.trim_matches('"').to_ascii_lowercase().as_str(),
        "btree_gin"
            | "btree_gist"
            | "citext"
            | "cube"
            | "dict_int"
            | "fuzzystrmatch"
            | "hstore"
            | "intarray"
            | "isn"
            | "lo"
            | "ltree"
            | "pgcrypto"
            | "pg_trgm"
            | "plpgsql"
            | "seg"
            | "tablefunc"
            | "tcn"
            | "tsm_system_rows"
            | "tsm_system_time"
            | "unaccent"
            | "uuid-ossp"
    )
}

/// The temporary-object schema of backend `pid`.
pub fn temp_schema_for_backend(pid: i32) -> String {
    format!("pg_temp_{pid}")
}

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

/// The default btree operator class and the subtype difference function of
/// a range subtype, with their PostgreSQL catalog OIDs: everything a range
/// type over that subtype records in `pg_range`. Bounds compare through the
/// subtype's own typed ordering.
struct RangeSubtypeCatalog {
    opclass: &'static str,
    opclass_oid: i32,
    subdiff: Option<(&'static str, i32)>,
}

fn range_subtype_catalog(subtype: &str) -> Option<RangeSubtypeCatalog> {
    let (opclass, opclass_oid, subdiff) = match subtype {
        "int4" => ("int4_ops", 1978, Some(("int4range_subdiff", 3922))),
        "int8" => ("int8_ops", 3124, Some(("int8range_subdiff", 3923))),
        "numeric" => ("numeric_ops", 3125, Some(("numrange_subdiff", 3924))),
        "date" => ("date_ops", 3122, Some(("daterange_subdiff", 3925))),
        "timestamp" => ("timestamp_ops", 3128, Some(("tsrange_subdiff", 3929))),
        "timestamptz" => ("timestamptz_ops", 3127, Some(("tstzrange_subdiff", 3930))),
        "int2" => ("int2_ops", 1979, None),
        "float4" => ("float4_ops", 10012, Some(("float4mi", 205))),
        "float8" => ("float8_ops", 3123, Some(("float8mi", 219))),
        "text" => ("text_ops", 3126, None),
        "time" => ("time_ops", 10038, Some(("time_mi_time", 1690))),
        "timetz" => ("timetz_ops", 10041, None),
        "interval" => ("interval_ops", 10022, Some(("interval_mi", 1170))),
        _ => return None,
    };
    Some(RangeSubtypeCatalog {
        opclass,
        opclass_oid,
        subdiff,
    })
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
            UserDefinedTypeSqlDefinitionOption::Default(expr) => set_base_type_option(
                &mut definition.default_expr,
                crate::sql_text::sql_text(expr),
                "DEFAULT",
            )?,
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

fn rewrite_routine_schema_type_identity(
    routine: &mut RoutineSchema,
    previous: &UserTypeSchema,
    updated: &UserTypeSchema,
) -> Result<bool> {
    let mut changed = rewrite_routine_type_identity(&mut routine.return_type, previous, updated);
    if let Some(declaration) = &mut routine.return_type_declaration {
        changed |= rewrite_routine_type_identity(declaration, previous, updated);
    }
    for index in 0..routine.arg_types.len() {
        if rewrite_routine_type_identity(&mut routine.arg_types[index].pg_type, previous, updated) {
            if let Some(argument) = routine.args.get_mut(index) {
                rewrite_routine_argument_type_identity(argument, previous, updated);
            }
            changed = true;
        }
    }
    changed |= rewrite_routine_definition_type_identity(routine, previous, updated)?;
    Ok(changed)
}

fn rewrite_routine_definition_type_identity(
    routine: &mut RoutineSchema,
    previous: &UserTypeSchema,
    updated: &UserTypeSchema,
) -> Result<bool> {
    fn relocate(
        data_type: &mut DataType,
        previous: &UserTypeSchema,
        updated: &UserTypeSchema,
    ) -> bool {
        use sqlparser::ast::ArrayElemTypeDef;
        if let DataType::Table(Some(columns)) | DataType::NamedTable { columns, .. } = data_type {
            return columns.iter_mut().fold(false, |changed, column| {
                relocate(&mut column.data_type, previous, updated) || changed
            });
        }
        if let DataType::Array(
            ArrayElemTypeDef::AngleBracket(element)
            | ArrayElemTypeDef::SquareBracket(element, _)
            | ArrayElemTypeDef::Parenthesis(element),
        ) = data_type
        {
            return relocate(element, previous, updated);
        }
        let DataType::Custom(name, _) = data_type else {
            return false;
        };
        let Some(parts) = name
            .0
            .iter()
            .map(|part| part.as_ident())
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        let normalized = |ident: &Ident| {
            if ident.quote_style.is_some() {
                ident.value.clone()
            } else {
                ident.value.to_ascii_lowercase()
            }
        };
        if !matches!(parts.len(), 1 | 2)
            || normalized(parts.last().unwrap()) != previous.name
            || (parts.len() == 2 && normalized(parts[0]) != previous.schema_name)
        {
            return false;
        }
        let quoted = parts.iter().any(|part| part.quote_style.is_some());
        let identifier = |value: &str| {
            if quoted
                || value != value.to_ascii_lowercase()
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
                || sqlparser::keywords::ALL_KEYWORDS.contains(&value.to_ascii_uppercase().as_str())
            {
                Ident::with_quote('"', value)
            } else {
                Ident::new(value)
            }
        };
        let mut target = Vec::new();
        if updated.schema_name != "public" {
            target.push(identifier(&updated.schema_name));
        }
        target.push(identifier(&updated.name));
        *name = ObjectName::from(target);
        true
    }

    fn relocate_default(
        default: &mut Option<Expr>,
        previous: &UserTypeSchema,
        updated: &UserTypeSchema,
    ) -> bool {
        let mut changed = false;
        let _ = sqlparser::ast::visit_expressions_mut(default, |expr| {
            match expr {
                Expr::Cast { data_type, .. } => changed |= relocate(data_type, previous, updated),
                Expr::TypedString(value) => {
                    changed |= relocate(&mut value.data_type, previous, updated)
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        changed
    }

    let (mut statements, procedure_proxy) =
        match Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition) {
            Ok(statements) => (statements, false),
            Err(_) if routine.kind == RoutineKind::Procedure => {
                let Some(header) = oracle::procedures::header(&routine.definition)? else {
                    return Ok(false);
                };
                (vec![Statement::CreateFunction(header)], true)
            }
            Err(_) => return Ok(false),
        };
    let [statement] = statements.as_mut_slice() else {
        return Ok(false);
    };
    let mut changed = false;
    match statement {
        Statement::CreateFunction(function) => {
            for (index, argument) in function.args.iter_mut().flatten().enumerate() {
                let type_changed = relocate(&mut argument.data_type, previous, updated);
                let default_changed =
                    relocate_default(&mut argument.default_expr, previous, updated);
                if type_changed || default_changed {
                    if let Some(declaration) = routine.args.get_mut(index) {
                        *declaration = crate::sql_text::sql_text(argument);
                    }
                    if let (Some(saved), Ok(rewritten)) = (
                        routine.arg_types.get_mut(index),
                        routine_type_schema(&argument.data_type),
                    ) {
                        *saved = rewritten;
                    }
                    changed = true;
                }
            }
            if let Some(
                FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type),
            ) = &mut function.return_type
            {
                if relocate(data_type, previous, updated) {
                    routine.return_type_declaration = Some(data_type.to_string());
                    if let Ok(rewritten) = routine_type_schema(data_type) {
                        routine.return_type = rewritten.pg_type;
                        routine.return_type_modifier = rewritten.type_modifier;
                    }
                    changed = true;
                }
            }
        }
        Statement::CreateProcedure { params, .. } => {
            for (index, argument) in params.iter_mut().flatten().enumerate() {
                let type_changed = relocate(&mut argument.data_type, previous, updated);
                let default_changed = relocate_default(&mut argument.default, previous, updated);
                if type_changed || default_changed {
                    if let Some(declaration) = routine.args.get_mut(index) {
                        *declaration = crate::sql_text::sql_text(argument);
                    }
                    if let (Some(saved), Ok(rewritten)) = (
                        routine.arg_types.get_mut(index),
                        routine_type_schema(&argument.data_type),
                    ) {
                        *saved = rewritten;
                    }
                    changed = true;
                }
            }
        }
        _ => return Ok(false),
    }
    if changed {
        // Loading routine metadata reparses this declaration. Keep its typed
        // header current while preserving the body as its original SQL value.
        let statement = statements.remove(0);
        routine.definition = if procedure_proxy {
            let Statement::CreateFunction(function) = statement else {
                unreachable!("procedure parsing proxy is a function header");
            };
            oracle::procedures::render(function)?
        } else {
            crate::sql_text::sql_text(&statement)
        };
    }
    Ok(changed)
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
struct CatalogPreflightScope {
    transaction: Rc<RefCell<Option<Transaction>>>,
}

impl Drop for CatalogPreflightScope {
    fn drop(&mut self) {
        // A writing routine transfers this same owner into its ordinary
        // transaction. Otherwise the read-only preflight owner ends here,
        // including early errors and unwinding, without a durable commit.
        self.transaction.borrow_mut().take();
    }
}

#[derive(Debug)]
pub struct SqlSession<'db> {
    /// Per-statement memo for the unique arbiters backing an ON CONFLICT
    /// target. Catalog resolution stays out of the per-row upsert path.
    conflict_arbiter_memo: std::cell::RefCell<
        Option<(
            String,
            records::ConflictColumnInference,
            Arc<Vec<records::UniqueConflictArbiter>>,
        )>,
    >,
    /// Identity of the compiled routine IR whose statements are executing
    /// (a unique per-compilation token), or None outside routine bodies. Keys the
    /// per-node expression-type memo: IR nodes are immutable and live as long
    /// as the routine cache holds the IR; tokens are not reused after eviction
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
    /// None is ordinary operator SQL; Some is an immutable signed application
    /// decision supplied by the host, never a user-settable SQL setting.
    pub(crate) app_exact_decimal_units: Option<bool>,
    pub(crate) runtime: Option<Arc<dyn SqlSessionRuntime>>,
    /// Temporary-schema backend number of a session without a connection
    /// runtime, assigned when it first creates a temporary table; its
    /// temporary objects are dropped when the session ends.
    pub(crate) embedded_temp_backend: Option<i32>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) fts_limits: bicdb_core::FtsQueryLimits,
    pub(crate) tx: Option<Transaction>,
    /// Metadata read while resolving an autocommit CALL belongs to the same
    /// owner later used for its buffered writes. This owner carries no SQL
    /// transaction semantics until begin_session_transaction consumes it.
    catalog_preflight_tx: Rc<RefCell<Option<Transaction>>>,
    /// True only for the child session that runs a PL/pgSQL function for one
    /// row of an engine-evaluated statement (`SqlSession::for_row_routine_call`).
    /// Such a session never begins or owns a transaction and refuses
    /// data-modifying statements.
    pub(crate) row_routine_call: bool,
    /// A function invoked by SQL keeps Oracle's SQL-call restrictions through
    /// nested procedural calls. Autonomous children establish a new context.
    pub(crate) in_oracle_sql_function: bool,
    /// PL/SQL expression calls have different character-result boundaries
    /// from calls made by SQL, including SQL inside a routine.
    pub(crate) in_routine_expression: bool,
    pub(crate) oracle_autonomous: bool,
    pub(crate) oracle_autonomous_active: bool,
    pub(crate) oracle_autonomous_epoch: u64,
    pub(crate) oracle_autonomous_lock_epoch: u64,
    pub(crate) oracle_suspension: Option<Arc<bicdb_core::TransactionSuspension>>,
    independent_commits_observed: bool,
    statement_error_recovered: bool,
    /// The base relation a view rewrite already authorized; consumed by the
    /// rewritten statement's first privilege check (see updatable_views).
    pub(crate) view_target_preauthorized: Option<String>,
    independent_execution_observed: bool,
    latest_independent_sequence: u64,
    /// For that child session, the caller's transaction (if any), read through
    /// `read_tx`.
    pub(crate) parent_tx: Option<&'db Transaction>,
    pub(crate) savepoints: Vec<SavepointMark>,
    pub(crate) ddl_undo: Vec<DdlUndo>,
    /// Relations this transaction created or truncated, in order (COPY
    /// FREEZE's condition). Carried with the DDL undo log.
    pub(crate) fresh_relations: Vec<String>,
    pub(crate) settings: SqlSettings,
    pub(crate) session_gucs: Arc<HashMap<String, String>>,
    pub(crate) guc_transaction_start: Option<SqlSessionGucSnapshot>,
    pub(crate) transaction_timestamp_micros: Option<i64>,
    pub(crate) guc_local_restore: HashMap<String, Option<String>>,
    pub(crate) guc_local_settings_restore: SqlSettingsRestore,
    pub(crate) routine_vars: Arc<BTreeMap<String, SqlValue>>,
    pub(crate) oracle_exception: Option<Arc<SqlError>>,
    pub(crate) oracle_error_tokens: Vec<std::sync::Weak<std::sync::atomic::AtomicBool>>,
    /// NOTIFY payloads raised by `pg_notify` AFTER INSERT triggers, for the
    /// protocol layer to deliver to listeners once the writes commit.
    pub(crate) pending_notifications: Vec<(String, String)>,
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
    pub(crate) sequence_values: Rc<RefCell<SequenceSessionValues>>,
    random_state: crate::pg_random::RandomState,
    pub(crate) sql_cursors: sql_cursors::SqlCursors,
    pub(crate) oracle_output: oracle::output::State,
    pub(crate) oracle_lobs: oracle::lobs::State,
    pub(crate) pg_large_objects: crate::pg_large_objects::DescriptorState,
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
    /// DDL commands running under event triggers on this session; nested
    /// commands of one DDL statement do not fire again.
    pub(crate) event_trigger_ddl_depth: usize,
    /// Last field: drops an embedded session's temporary schema after every
    /// other field (the transaction included) is gone.
    pub(crate) embedded_temp_guard: Option<temp_tables::EmbeddedTempGuard>,
}

#[derive(Clone, Debug, Default)]
pub struct SqlSessionGucState {
    settings: SqlSettings,
    session_gucs: Arc<HashMap<String, String>>,
    transaction_start: Option<SqlSessionGucSnapshot>,
    transaction_timestamp_micros: Option<i64>,
    local_restore: HashMap<String, Option<String>>,
    local_settings_restore: SqlSettingsRestore,
    sequence_values: SequenceSessionValues,
    random_state: crate::pg_random::RandomState,
    sql_cursors: sql_cursors::SqlCursors,
    oracle_output: oracle::output::State,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SequenceSessionValues {
    pub(crate) current: FxHashMap<String, i64>,
    pub(crate) last_sequence: Option<String>,
}

impl SequenceSessionValues {
    pub(crate) fn record_nextval(&mut self, sequence: &str, value: i64) {
        self.current.insert(sequence.to_owned(), value);
        self.last_sequence = Some(sequence.to_owned());
    }
}

impl SqlSessionGucState {
    /// A WITH HOLD cursor was declared in the open transaction block.
    pub fn has_transaction_hold_cursor(&self) -> bool {
        self.sql_cursors.has_uncommitted_holdable()
    }

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

    /// A connection's immutable settings for wire encoding and decoding.
    pub fn shared_session_gucs(&self) -> Arc<HashMap<String, String>> {
        self.session_gucs.clone()
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
            self.transaction_timestamp_micros = Some(unix_now_micros());
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
        self.transaction_timestamp_micros = None;
        self.sql_cursors.commit();
    }

    pub fn rollback_transaction(&mut self) {
        self.sql_cursors.rollback();
        if let Some(start) = self.transaction_start.take() {
            self.settings = start.settings;
            self.session_gucs = start.session_gucs;
        }
        self.local_restore.clear();
        self.local_settings_restore = SqlSettingsRestore::default();
        self.transaction_timestamp_micros = None;
    }

    pub fn restore_savepoint(&mut self, savepoint: &Self) {
        self.sql_cursors
            .rollback_to(savepoint.sql_cursors.copy_mark());
        self.settings = savepoint.settings;
        self.session_gucs = savepoint.session_gucs.clone();
        self.transaction_start = savepoint.transaction_start.clone();
        self.transaction_timestamp_micros = savepoint.transaction_timestamp_micros;
        self.local_restore = savepoint.local_restore.clone();
        self.local_settings_restore = savepoint.local_settings_restore.clone();
    }

    /// Conservatively estimates memory owned by this opaque state, including
    /// effective values, transaction-start snapshots, and local restore data.
    pub fn memory_estimate(&self) -> usize {
        self.memory_estimate_with(|gucs| string_map_memory_estimate(gucs))
    }

    /// Records the setting maps this state holds, which copies of it (such as
    /// savepoints) share until a setting changes.
    pub fn note_shared_maps(&self, counted: &mut FxHashSet<usize>) {
        counted.insert(Arc::as_ptr(&self.session_gucs) as usize);
        if let Some(start) = &self.transaction_start {
            counted.insert(Arc::as_ptr(&start.session_gucs) as usize);
        }
    }

    /// `memory_estimate` for a copy kept beside other states: a setting map
    /// already in `counted` is not counted again.
    pub fn memory_estimate_beyond(&self, counted: &mut FxHashSet<usize>) -> usize {
        self.memory_estimate_with(|gucs| {
            if counted.insert(Arc::as_ptr(gucs) as usize) {
                string_map_memory_estimate(gucs)
            } else {
                0
            }
        })
    }

    fn memory_estimate_with(
        &self,
        mut map_estimate: impl FnMut(&Arc<HashMap<String, String>>) -> usize,
    ) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(map_estimate(&self.session_gucs))
            .saturating_add(
                self.transaction_start
                    .as_ref()
                    .map(|start| {
                        std::mem::size_of::<SqlSessionGucSnapshot>()
                            .saturating_add(map_estimate(&start.session_gucs))
                    })
                    .unwrap_or_default(),
            )
            .saturating_add(optional_string_map_memory_estimate(&self.local_restore))
            .saturating_add(sequence_value_map_memory_estimate(
                &self.sequence_values.current,
            ))
            .saturating_add(
                self.sequence_values
                    .last_sequence
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(self.oracle_output.memory_estimate())
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
                .map(SqlValue::json);
            }
            match SqlValue::into_json_value(value) {
                JsonValue::Array(values) => values
                    .into_iter()
                    .map(|value| {
                        rewrite_composite_value(
                            SqlValue::Json(value.into()),
                            target_oid,
                            target_name,
                            mutation,
                        )
                        .map(|value| match value {
                            SqlValue::Json(value) => SqlValue::into_json_value(value),
                            value => sql_value_to_json(value),
                        })
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(JsonValue::Array)
                    .map(SqlValue::json),
                JsonValue::Object(values) => values
                    .into_iter()
                    .map(|(key, value)| {
                        rewrite_composite_value(
                            SqlValue::Json(value.into()),
                            target_oid,
                            target_name,
                            mutation,
                        )
                        .map(|value| {
                            (
                                key,
                                match value {
                                    SqlValue::Json(value) => SqlValue::into_json_value(value),
                                    value => sql_value_to_json(value),
                                },
                            )
                        })
                    })
                    .collect::<Result<serde_json::Map<_, _>>>()
                    .map(JsonValue::Object)
                    .map(SqlValue::json),
                value => Ok(SqlValue::json(value)),
            }
        }
        value => Ok(value),
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
    fn is_current_transaction_mode(&self) -> bool {
        self.gucs.len() == 1
            && self.gucs.iter().any(|key| {
                matches!(
                    key.as_str(),
                    "transaction_isolation" | "transaction_read_only" | "transaction_deferrable"
                )
            })
    }

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
            "transaction_isolation",
            "transaction_deferrable",
            "transaction_read_only",
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
    index_generation: u64,
    index_commit_seq: u64,
    index_lookup_cache: IndexLookupCache,
    rowid_index_lookup_cache: RowIdIndexLookupCache,
}

#[derive(Clone, Debug, Default)]
pub struct SqlSessionCatalogCache {
    pub(crate) oracle_lobs: oracle::lobs::State,
    pub(crate) pg_large_objects: crate::pg_large_objects::DescriptorState,
    pub(crate) schema_cache_generation: u64,
    pub(crate) schema_cache: SqlSchemaCacheMap,
    pub(crate) schema_list_cache: SqlSchemaListCacheMap,
    /// Parsed statements keyed on their literal-normalized template. pgwire
    /// builds a session per statement, so the cache lives here, per
    /// connection, or it would be empty on every CALL.
    pub(crate) parsed_statement_cache: FxHashMap<String, crate::statement_cache::CachedTemplate>,
}

impl SqlSessionCatalogCache {
    /// Reuse this connection's schema metadata during read-only preparation.
    /// The closure must use `db`; schema changes invalidate cached metadata.
    pub fn with_schema_cache<T>(&mut self, db: &BicDb, prepare: impl FnOnce() -> T) -> T {
        // A cache belongs to one connection/database. Switching databases must
        // discard earlier generations, even when their counters happen to match.
        let instance = db.instance_id();
        self.schema_cache.retain(|id, _| *id == instance);
        self.schema_list_cache.retain(|id, _| *id == instance);
        let mut scope = SqlSchemaSessionCacheScope::new(
            db.collection_generation(SCHEMA_COLLECTION),
            &mut self.schema_cache_generation,
            &mut self.schema_cache,
            &mut self.schema_list_cache,
        );
        let result = prepare();
        scope.finish(
            db.collection_generation(SCHEMA_COLLECTION),
            &mut self.schema_cache_generation,
            &mut self.schema_cache,
            &mut self.schema_list_cache,
        );
        result
    }

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
    fresh_relations: Vec<String>,
}

impl SqlSessionDdlUndoLog {
    /// How many relations the transaction has created or truncated so far:
    /// a savepoint remembers it, and rolling back to the savepoint forgets
    /// the later ones with [`Self::truncate_fresh_relations`].
    pub fn fresh_relation_count(&self) -> usize {
        self.fresh_relations.len()
    }

    pub fn truncate_fresh_relations(&mut self, len: usize) {
        self.fresh_relations.truncate(len);
    }

    /// Whether `relation` (a physical name) was created or truncated after
    /// the first `from` such events: in the current subtransaction when
    /// `from` is the count the innermost savepoint remembered.
    pub fn relation_fresh_since(&self, relation: &str, from: usize) -> bool {
        self.fresh_relations
            .get(from..)
            .is_some_and(|fresh| fresh.iter().any(|name| name.eq_ignore_ascii_case(relation)))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Native hosts that commit outside SqlSession must release only rename
    /// scopes whose durable business decision consumed the journal header.
    pub fn finish_committed_catalog_scopes(&self, db: &BicDb) -> Result<()> {
        finish_committed_catalog_scopes(db, &self.entries)
    }
}

fn finish_committed_catalog_scopes(db: &BicDb, entries: &[DdlUndo]) -> Result<()> {
    for entry in entries {
        if let DdlUndo::RestoreRenameJournal { id, .. } = entry {
            rename_journal::finish_committed(db, id)?;
        }
    }
    Ok(())
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
        creation_intent: Option<String>,
        creation_receipt: Option<bicdb_core::CatalogCreatedCollectionUndo>,
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
    RestoreRenameJournal {
        id: String,
        old_table: String,
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
    RestorePartitionCatalog {
        entries: Vec<partition_triggers::CatalogUndo>,
    },
    RestorePartitionTableStates {
        states: Vec<partition_schema::TableState>,
        auxiliary: Vec<partition_schema::AuxiliaryState>,
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
    /// Undo of a DROP TABLE that moved the table into the recycle bin.
    RestoreFromRecycleBin {
        bin_name: String,
        target: String,
    },
    /// Undo of FLASHBACK TABLE ... TO BEFORE DROP.
    ReturnToRecycleBin {
        intent_id: String,
    },
    DeleteDefaultPrivilege {
        grant: DefaultPrivilegeGrant,
    },
    RestoreDefaultPrivilege {
        grant: DefaultPrivilegeGrant,
    },
    DeleteRoutine {
        routine: RoutineSchema,
    },
    RestoreRoutine {
        routine: RoutineSchema,
    },
    RestoreRule {
        key: String,
        previous: Option<crate::query_rules::RuleSchema>,
    },
    RestoreOperator {
        id: crate::pg_operators::OperatorRef,
        previous: Option<Box<crate::pg_operators::OperatorSchema>>,
    },
    RestoreCollation {
        schema_name: String,
        name: String,
        previous: Option<Box<crate::collations::CollationSchema>>,
    },
    RestoreTextSearchConfiguration {
        oid: u32,
        previous: Option<Box<crate::fts::authored::AuthoredConfig>>,
    },
    RestoreTextSearchDictionary {
        oid: u32,
        previous: Option<Box<crate::fts::authored::AuthoredDictionary>>,
    },
    RestoreEventTrigger {
        oid: i64,
        previous: Option<Box<event_triggers::EventTriggerSchema>>,
    },
    RestoreForeignCatalog {
        previous: Box<crate::foreign_data::ForeignCatalog>,
    },
    RestoreUserCast {
        source: String,
        target: String,
        previous: Option<Box<crate::user_casts::UserCast>>,
    },
    RestoreStatistics {
        schema: String,
        name: String,
        previous: Option<Box<crate::stats_ext::StatsObject>>,
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
    pub(crate) pg_large_objects: SqlLargeObjectSavepoint,
    pub(crate) ddl_undo_len: usize,
    pub(crate) settings: SqlSettings,
    pub(crate) session_gucs: Arc<HashMap<String, String>>,
    pub(crate) guc_local_restore: HashMap<String, Option<String>>,
    pub(crate) guc_local_settings_restore: SqlSettingsRestore,
    pub(crate) cursor_mark: u64,
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
        SqlValue::Json(value) => rename_json(std::sync::Arc::make_mut(value), from, to),
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
    let source_column_is_variable_length = source.pg_type == "varchar";
    let metadata_only = match (&source.type_modifier, &target.type_modifier) {
        (
            Some(PgTypeModifier::Character { length: source }),
            Some(PgTypeModifier::Character { length: target }),
        ) => target >= source && source_column_is_variable_length,
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
            event_trigger_ddl_depth: 0,
            db,
            exclusive,
            defer_commit: false,
            snapshot_floor: 0,
            _marker: PhantomData,
            security_context,
            app_exact_decimal_units: None,
            runtime: None,
            embedded_temp_backend: None,
            embedded_temp_guard: None,
            cancellation: CancellationToken::uncancelable(),
            fts_limits: bicdb_core::FtsQueryLimits::UNLIMITED,
            tx: None,
            catalog_preflight_tx: Rc::new(RefCell::new(None)),
            row_routine_call: false,
            in_oracle_sql_function: false,
            in_routine_expression: false,
            oracle_autonomous: false,
            oracle_autonomous_active: false,
            oracle_autonomous_epoch: 0,
            oracle_autonomous_lock_epoch: 0,
            oracle_suspension: None,
            independent_commits_observed: false,
            statement_error_recovered: false,
            view_target_preauthorized: None,
            independent_execution_observed: false,
            latest_independent_sequence: 0,
            parent_tx: None,
            savepoints: Vec::new(),
            ddl_undo: Vec::new(),
            fresh_relations: Vec::new(),
            settings: SqlSettings::default(),
            session_gucs: Arc::new(HashMap::new()),
            guc_transaction_start: None,
            transaction_timestamp_micros: None,
            guc_local_restore: HashMap::new(),
            guc_local_settings_restore: SqlSettingsRestore::default(),
            routine_vars: Arc::new(BTreeMap::new()),
            oracle_exception: None,
            oracle_error_tokens: Vec::new(),
            pending_notifications: Vec::new(),
            signed_mutation_grants: BTreeMap::new(),
            signed_application_security_guc_compatibility: false,
            sequence_values: Rc::new(RefCell::new(SequenceSessionValues::default())),
            random_state: crate::pg_random::RandomState::default(),
            sql_cursors: sql_cursors::SqlCursors::default(),
            oracle_output: oracle::output::State::default(),
            oracle_lobs: oracle::lobs::State::default(),
            pg_large_objects: crate::pg_large_objects::DescriptorState::default(),
            schema_cache_generation,
            schema_cache: SqlSchemaCacheMap::default(),
            schema_list_cache: BTreeMap::new(),
            parsed_statement_cache: FxHashMap::default(),
            txn_lookup_caches: std::cell::RefCell::new(None),
        };
        session.rebind_trusted_security_settings();
        session.apply_database_setting_defaults();
        session
    }

    /// `ALTER DATABASE name SET setting = value` defaults: any setting this session has not set itself
    /// starts at the database's value, and `RESET` returns to it (PostgreSQL's per-database defaults).
    pub(crate) fn apply_database_setting_defaults(&mut self) {
        let name = current_database_from_gucs(&self.session_gucs);
        let Ok(settings) = database_settings(self.db_ref(), &name) else {
            return;
        };
        if settings.is_empty() {
            return;
        }
        let gucs = Arc::make_mut(&mut self.session_gucs);
        for (setting, value) in settings {
            if !is_protected_security_setting(&setting)
                && !matches!(
                    setting.as_str(),
                    SESSION_CLIENT_ADDRESS_GUC | SESSION_CLIENT_HOST_GUC
                )
            {
                gucs.entry(setting).or_insert(value);
            }
        }
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

    /// Exclusive database access for row-rewriting DDL, together with the
    /// session's open transaction so rows it owns are rewritten through it.
    pub(crate) fn ddl_rows(&mut self) -> Result<DdlRows<'_>> {
        if !self.exclusive {
            return Err(SqlError::Unsupported(
                "operation requires exclusive database access".to_string(),
            ));
        }
        // Safety: as in `db_mut`; the transaction is a disjoint field.
        let db = unsafe { self.db.as_mut() };
        Ok(DdlRows {
            db,
            tx: self.tx.as_mut(),
        })
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

    /// The session's temporary-object schema, `pg_temp_<backend pid>`, for a
    /// session served by a connection. Embedded sessions have none.
    pub(crate) fn temp_schema_name(&self) -> Option<String> {
        match &self.runtime {
            Some(runtime) => Some(temp_schema_for_backend(runtime.backend_pid())),
            None => self.embedded_temp_backend.map(temp_schema_for_backend),
        }
    }

    /// The temporary schema for a new temporary object: a session without a
    /// connection takes a process-unique backend number on first use.
    pub(crate) fn temp_schema_for_creation(&mut self) -> String {
        if self.runtime.is_none() && self.embedded_temp_backend.is_none() {
            let backend = temp_tables::next_embedded_temp_backend();
            self.embedded_temp_backend = Some(backend);
            if self.exclusive {
                self.embedded_temp_guard =
                    Some(temp_tables::EmbeddedTempGuard::new(self.db, backend));
            }
        }
        self.temp_schema_name()
            .expect("a temporary schema exists for a connection or an embedded session")
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

    /// Bind the current signed application's declared feature. This Rust-only
    /// allowance is independent of SQL roles, GUCs and transaction ownership.
    pub fn with_application_exact_decimal_units(mut self, allowed: bool) -> Self {
        self.app_exact_decimal_units = Some(allowed);
        self
    }

    pub fn with_session_gucs(mut self, session_gucs: HashMap<String, String>) -> Self {
        self.session_gucs = Arc::new(session_gucs);
        self.rebind_trusted_security_settings();
        self.apply_database_setting_defaults();
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
        self.transaction_timestamp_micros = state.transaction_timestamp_micros;
        self.guc_local_restore = state.local_restore;
        self.guc_local_settings_restore = state.local_settings_restore;
        *self.sequence_values.borrow_mut() = state.sequence_values;
        self.random_state = state.random_state;
        self.sql_cursors = state.sql_cursors;
        self.oracle_output = state.oracle_output;
        self.rebind_trusted_security_settings();
        self.apply_database_setting_defaults();
    }

    fn take_session_guc_state(&mut self) -> SqlSessionGucState {
        let preserve_postgres_compatibility_gucs = self.security_context.is_none()
            && postgres_compatibility_gucs_enabled(&self.session_gucs);
        let mut state = SqlSessionGucState {
            settings: self.settings,
            session_gucs: std::mem::take(&mut self.session_gucs),
            transaction_start: self.guc_transaction_start.take(),
            transaction_timestamp_micros: self.transaction_timestamp_micros.take(),
            local_restore: std::mem::take(&mut self.guc_local_restore),
            local_settings_restore: std::mem::take(&mut self.guc_local_settings_restore),
            sequence_values: std::mem::take(&mut *self.sequence_values.borrow_mut()),
            random_state: self.random_state.clone(),
            sql_cursors: self.sql_cursors.snapshot(),
            oracle_output: std::mem::take(&mut self.oracle_output),
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
        if self.row_routine_call {
            return Err(row_routine_write_error());
        }
        let isolation = self.default_transaction_isolation()?;
        let floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        let preflight = self.catalog_preflight_tx.borrow_mut().take();
        let mut tx = match preflight {
            Some(mut tx) => {
                tx.refresh_read_committed_snapshot_after(floor)?;
                tx
            }
            None => self.db_ref().begin_transaction_after(floor)?,
        };
        tx.set_blocking_row_waits(self.blocking_row_wait_token());
        if matches!(isolation, "repeatable read" | "serializable") {
            tx.set_snapshot_isolation(true, floor)?;
            tx.set_statement_write_conflict_check(true)?;
        }
        tx.defer_statement_snapshot()?;
        Ok(tx)
    }

    /// DML waits for a row's owner (as SQL locking reads do) only on shared
    /// sessions: the owner commits through a shared guard, so a wait under an
    /// exclusive guard could never be released.
    pub(crate) fn blocking_row_wait_token(&self) -> Option<CancellationToken> {
        (!self.exclusive).then(|| self.cancellation.clone())
    }

    /// The transaction this session reads through: its own, or for a per-row
    /// routine call the caller's (see `parent_tx`).
    pub(crate) fn read_tx(&self) -> Option<&Transaction> {
        self.tx.as_ref().or(self.parent_tx)
    }

    /// The native transaction's trusted mutation actor identifies an owning
    /// application invocation. SQL roles, GUCs and routine/view execution cannot
    /// change it. Only the host may finish that transaction; ordinary operator
    /// SQL transactions retain their existing control semantics.
    pub(crate) fn require_local_transaction_control(&self) -> Result<()> {
        if self.has_host_owned_transaction() {
            return Err(SqlError::ApplicationTransactionControlDenied);
        }
        Ok(())
    }

    fn has_host_owned_transaction(&self) -> bool {
        // A routine's local slot must not hide its application-owned parent.
        self.tx
            .as_ref()
            .is_some_and(|tx| tx.mutation_actor().is_some())
            || self
                .parent_tx
                .is_some_and(|tx| tx.mutation_actor().is_some())
    }

    fn require_local_transaction_setting_control(&self, setting: &str) -> Result<()> {
        if matches!(
            setting,
            "transaction_isolation" | "transaction_read_only" | "transaction_deferrable"
        ) {
            self.require_local_transaction_control()?;
        }
        Ok(())
    }

    fn require_local_transaction_set_control(&self, set: &Set) -> Result<()> {
        if !self.has_host_owned_transaction() {
            return Ok(());
        }
        match set {
            Set::SetTransaction { session: false, .. } => self.require_local_transaction_control(),
            Set::SingleAssignment { variable, .. } => self
                .require_local_transaction_setting_control(
                    &object_name(variable)?.to_ascii_lowercase(),
                ),
            Set::MultipleAssignments { assignments } => {
                // Preflight every name before any assignment in the statement.
                for assignment in assignments {
                    self.require_local_transaction_setting_control(
                        &object_name(&assignment.name)?.to_ascii_lowercase(),
                    )?;
                }
                Ok(())
            }
            // Session defaults apply to future local transactions only.
            _ => Ok(()),
        }
    }

    /// Takes the pending (uncommitted) transaction produced by a deferred
    /// autocommit execution, if one is present.
    /// `(channel, payload)` notifications raised by this session's trigger
    /// writes since the last call. Deliver them when those writes commit.
    pub fn take_pending_notifications(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.pending_notifications)
    }

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
        self.oracle_lobs = cache.oracle_lobs;
        self.pg_large_objects = cache.pg_large_objects;
        self.schema_cache_generation = cache.schema_cache_generation;
        self.schema_cache = cache.schema_cache;
        self.schema_list_cache = cache.schema_list_cache;
        self.parsed_statement_cache = cache.parsed_statement_cache;
        self
    }

    pub fn with_ddl_undo_log(mut self, log: SqlSessionDdlUndoLog) -> Self {
        self.ddl_undo = log.entries;
        self.fresh_relations = log.fresh_relations;
        self
    }

    pub fn take_ddl_undo_log(&mut self) -> SqlSessionDdlUndoLog {
        SqlSessionDdlUndoLog {
            entries: std::mem::take(&mut self.ddl_undo),
            fresh_relations: std::mem::take(&mut self.fresh_relations),
        }
    }

    pub fn rollback_ddl_undo_to_len(&mut self, len: usize) -> Result<()> {
        self.rollback_ddl_to_len(len)
    }

    pub fn into_catalog_cache(self) -> SqlSessionCatalogCache {
        SqlSessionCatalogCache {
            oracle_lobs: self.oracle_lobs,
            pg_large_objects: self.pg_large_objects,
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
            transaction_timestamp_micros: self.transaction_timestamp_micros,
            local_restore: self.guc_local_restore.clone(),
            local_settings_restore: self.guc_local_settings_restore.clone(),
            sequence_values: self.sequence_values.borrow().clone(),
            random_state: self.random_state.clone(),
            sql_cursors: self.sql_cursors.snapshot(),
            oracle_output: self.oracle_output.clone(),
        }
    }

    pub(crate) fn begin_guc_transaction(&mut self) {
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
        let _temporal_scope = self.temporal_scope();
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

    fn temporal_scope(&self) -> SqlTemporalScope {
        let transaction_timestamp_micros = self
            .transaction_timestamp_micros
            .unwrap_or_else(unix_now_micros);
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
        SqlTemporalScope::new(transaction_timestamp_micros, timezone, interval_style)
            .with_datestyle(self.session_gucs.get("datestyle").map(String::as_str))
    }

    pub(crate) fn commit_guc_transaction(&mut self) {
        let mut state = self.take_session_guc_state();
        state.commit_transaction();
        self.install_session_guc_state(state);
    }

    pub(crate) fn rollback_guc_transaction(&mut self) {
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
        if targets.is_current_transaction_mode() {
            self.require_local_transaction_control()?;
        }
        let previous_gucs = self.session_gucs.clone();
        let previous_settings = self.settings;
        let local = local
            || (targets.is_current_transaction_mode()
                && !oracle::preview_routine_language_enabled(self.db_ref())?);
        apply(self)?;
        self.reconcile_guc_assignments(previous_gucs, previous_settings, local);
        if !local {
            targets.clear_local_restore(self);
        }
        Ok(())
    }

    /// Keep a whole mutation statement, including authored triggers and their
    /// deferred effects, inside one transaction when the caller has no BEGIN.
    pub(crate) fn with_statement_transaction<T>(
        &mut self,
        execute: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if let Some(transaction) = self.tx.as_ref().map(Transaction::rollback_mark) {
            let large_objects = self.pg_large_objects.savepoint()?;
            let ddl_undo_len = self.ddl_undo.len();
            let savepoint_len = self.savepoints.len();
            let mut gucs = self.session_guc_state();
            let result = execute(self);
            if result.is_err() {
                self.pg_large_objects.restore(large_objects)?;
                if let Some(tx) = self.tx.as_mut() {
                    tx.rollback_to_mark(transaction)?;
                }
                self.rollback_ddl_to_len(ddl_undo_len)?;
                self.savepoints.truncate(savepoint_len);
                // Sequence consumption and currval/lastval are not rolled back.
                gucs.sequence_values = std::mem::take(&mut *self.sequence_values.borrow_mut());
                self.install_session_guc_state(gucs);
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

    /// [`Self::copy_insert_rows`] that also returns the notices its triggers
    /// reported (RAISE NOTICE), which PostgreSQL sends before COPY's
    /// completion. A failure carries the notices reported before it.
    pub fn copy_insert_rows_reporting(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<(usize, Vec<SqlNotice>)> {
        let effects = crate::statement_context::StatementScope::enter();
        let result = self.copy_insert_rows(table, columns, rows);
        self.finish_reporting(&effects, result)
    }

    /// [`Self::fire_deferred_row_triggers`] that also returns the notices
    /// the triggers reported, which PostgreSQL sends before COMMIT completes.
    pub fn fire_deferred_row_triggers_reporting(&mut self) -> Result<Vec<SqlNotice>> {
        let effects = crate::statement_context::StatementScope::enter();
        let result = self.fire_deferred_row_triggers();
        self.finish_reporting(&effects, result)
            .map(|((), notices)| notices)
    }

    fn finish_reporting<T>(
        &self,
        effects: &crate::statement_context::StatementScope,
        result: Result<T>,
    ) -> Result<(T, Vec<SqlNotice>)> {
        let notices = effects.take_notices();
        match result {
            Ok(value) => Ok((value, notices)),
            Err(error) => {
                let error = match self.authorize_error_value_details(Err(error)) {
                    Err(error) => error,
                    Ok(_) => unreachable!("an error stays an error"),
                };
                if notices.is_empty() {
                    Err(error)
                } else {
                    Err(SqlError::WithNotices {
                        error: Box::new(error),
                        notices,
                    })
                }
            }
        }
    }

    /// The columns a COPY of `table` reads or writes, as PostgreSQL's
    /// `CopyGetAttnums` decides them, and the table's generated columns in
    /// column order. Without a column list: every column that is not
    /// generated (stored or virtual), which are computed on COPY FROM and
    /// left out of COPY TO. A generated column named in the list is refused
    /// with 42P10. `None` when the relation has no table schema.
    pub fn copy_target_columns(
        &self,
        table: &str,
        columns: &[String],
    ) -> Result<Option<(Vec<String>, Vec<String>)>> {
        self.with_relation_name_scopes(|| self.copy_target_columns_inner(table, columns))
    }

    /// What COPY FROM needs to know about its table beyond the columns: the
    /// physical name (COPY FREEZE's created-or-truncated check), whether it
    /// is partitioned, and which of `targets` have a default (the DEFAULT
    /// marker). `None` when the relation has no table schema.
    pub fn copy_relation_info(
        &self,
        table: &str,
        targets: &[String],
    ) -> Result<Option<CopyRelationInfo>> {
        self.with_relation_name_scopes(|| {
            let qualified = self.bind_copy_relation(table);
            let table = relation_name_from_parts(&qualified)?;
            let table = resolve_session_relation_name(self.db_ref(), &table)?;
            let Some(schema) = load_schema(self.db_ref(), &table)? else {
                return Ok(None);
            };
            let has_default = targets
                .iter()
                .map(|name| {
                    schema.column(name).is_some_and(|column| {
                        column.default_sequence.is_some()
                            || column.default_value.is_some()
                            || column.effective_default_expr().is_some()
                    })
                })
                .collect();
            Ok(Some(CopyRelationInfo {
                partitioned: schema.partitioning.is_some(),
                physical_name: table,
                has_default,
            }))
        })
    }

    /// Validate folded defaults that COPY may use, before the client starts
    /// sending rows. Expression defaults and sequence values are evaluated
    /// only if a row actually uses them.
    pub fn validate_copy_from_defaults(
        &self,
        table: &str,
        targets: &[String],
        default_marker: bool,
    ) -> Result<()> {
        self.with_relation_name_scopes(|| {
            let qualified = self.bind_copy_relation(table);
            let table = relation_name_from_parts(&qualified)?;
            let table = resolve_session_relation_name(self.db_ref(), &table)?;
            let Some(schema) = load_schema(self.db_ref(), &table)? else {
                return Ok(());
            };
            for column in schema.columns.iter().filter(|column| {
                !column.hidden && (default_marker || !targets.contains(&column.name))
            }) {
                if let Some(value) = &column.default_value {
                    cast_value_to_column_type(value.clone(), column)?;
                }
            }
            Ok(())
        })
    }

    /// Resolve relation names as a statement of this session does: its
    /// search_path and its temporary schema. `execute` enters these scopes
    /// itself; entry points that resolve names without it (COPY) need them
    /// too, or a temporary table is not found.
    fn with_relation_name_scopes<T>(&self, run: impl FnOnce() -> Result<T>) -> Result<T> {
        let temp_schema = self.temp_schema_name();
        let _temp_scope = crate::search_path::TempSchemaScope::enter(temp_schema.as_deref());
        let path = crate::search_path::binding_path_with_temp(
            self.db_ref(),
            &self.session_gucs,
            temp_schema.as_deref(),
        )?;
        let _path_scope = crate::search_path::PathScope::enter(path);
        let _catalog_scope = crate::search_path::CatalogScope::enter(&self.session_gucs);
        run()
    }

    /// The parts of a COPY target, an unqualified name bound through the
    /// active search_path (and so to the session's temporary schema) as a
    /// statement binds it. Each part is taken exactly as written.
    fn bind_copy_relation(&self, table: &str) -> Vec<String> {
        let mut name = ObjectName::from(
            table
                .split('.')
                .map(|part| Ident::with_quote('"', part))
                .collect::<Vec<_>>(),
        );
        crate::search_path::bind_raw_relation(self.db_ref(), &mut name);
        crate::eval::rewrite_helpers::object_name_parts(&name)
    }

    fn copy_target_columns_inner(
        &self,
        table: &str,
        columns: &[String],
    ) -> Result<Option<(Vec<String>, Vec<String>)>> {
        let display = table
            .rsplit('.')
            .next()
            .unwrap_or(table)
            .trim_matches('"')
            .to_string();
        let qualified = self.bind_copy_relation(table);
        let table = relation_name_from_parts(&qualified)?;
        let table = resolve_session_relation_name(self.db_ref(), &table)?;
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        let generated = schema
            .columns
            .iter()
            .filter(|column| !column.hidden && column.generated_expr.is_some())
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            let targets = schema
                .columns
                .iter()
                .filter(|column| !column.hidden && column.generated_expr.is_none())
                .map(|column| column.name.clone())
                .collect();
            return Ok(Some((targets, generated)));
        }
        // PostgreSQL's CopyGetAttnums: every named column must exist, once.
        for (index, column) in columns.iter().enumerate() {
            if !schema.column(column).is_some_and(|target| !target.hidden) {
                return Err(SqlError::coded(
                    "42703",
                    format!("column \"{column}\" of relation \"{display}\" does not exist"),
                    None,
                    None,
                ));
            }
            if columns[..index].contains(column) {
                return Err(SqlError::coded(
                    "42701",
                    format!("column \"{column}\" specified more than once"),
                    None,
                    None,
                ));
            }
        }
        for column in columns {
            if let Some(target) = schema
                .column(column)
                .filter(|target| !target.hidden && target.generated_expr.is_some())
            {
                return Err(SqlError::coded(
                    "42P10",
                    format!("column \"{}\" is a generated column", target.name),
                    Some("Generated columns cannot be used in COPY.".to_string()),
                    None,
                ));
            }
        }
        Ok(Some((columns.to_vec(), generated)))
    }

    pub fn copy_insert_rows(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize> {
        self.copy_insert_rows_at_lines(table, columns, rows, None, &[], None)
            .map(|outcome| outcome.inserted)
    }

    /// [`Self::copy_insert_rows_reporting`] for COPY FROM input whose rows
    /// came from these input line numbers (one per row): a value its column
    /// type refuses fails with PostgreSQL's CONTEXT line
    /// `COPY t, line N, column c: "value"`.
    pub fn copy_insert_rows_reporting_at_lines(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
        lines: &[u64],
    ) -> Result<(usize, Vec<SqlNotice>)> {
        let effects = crate::statement_context::StatementScope::enter();
        let result = self
            .copy_insert_rows_at_lines(table, columns, rows, Some(lines), &[], None)
            .map(|outcome| outcome.inserted);
        self.finish_reporting(&effects, result)
    }

    /// COPY FROM rows with the statement's rules: ON_ERROR IGNORE (with
    /// REJECT_LIMIT and LOG_VERBOSITY), DEFAULT markers (`default_marks`:
    /// per row, the indexes of the fields that were the marker) and a WHERE
    /// condition. Returns what was inserted and skipped, and the notices.
    pub fn copy_insert_rows_with_rules(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
        lines: &[u64],
        default_marks: &[Vec<usize>],
        rules: &CopyFromRules,
    ) -> Result<(CopyFromOutcome, Vec<SqlNotice>)> {
        let effects = crate::statement_context::StatementScope::enter();
        let result = self.copy_insert_rows_at_lines(
            table,
            columns,
            rows,
            Some(lines),
            default_marks,
            Some(rules),
        );
        self.finish_reporting(&effects, result)
    }

    fn copy_insert_rows_at_lines(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
        lines: Option<&[u64]>,
        default_marks: &[Vec<usize>],
        rules: Option<&CopyFromRules>,
    ) -> Result<CopyFromOutcome> {
        self.admit_partition_execution()?;
        let _case = oracle::simple_case::Scope::for_db(self.db_ref())?;
        self.require_transaction_write()?;
        self.with_implicit_guc_statement(|session| {
            session.with_statement_transaction(|session| {
                let temp_schema = session.temp_schema_name();
                let _temp_scope =
                    crate::search_path::TempSchemaScope::enter(temp_schema.as_deref());
                let path = crate::search_path::binding_path_with_temp(
                    session.db_ref(),
                    &session.session_gucs,
                    temp_schema.as_deref(),
                )?;
                let _path_scope = crate::search_path::PathScope::enter(path);
                let _catalog_scope = crate::search_path::CatalogScope::enter(&session.session_gucs);
                session.copy_insert_rows_inner(table, columns, rows, lines, default_marks, rules)
            })
        })
    }

    fn copy_insert_rows_inner(
        &mut self,
        table: &str,
        columns: &[String],
        rows: Vec<Vec<Option<String>>>,
        lines: Option<&[u64]>,
        default_marks: &[Vec<usize>],
        rules: Option<&CopyFromRules>,
    ) -> Result<CopyFromOutcome> {
        let _xml_settings = crate::xml::XmlSettingsScope::enter(&self.session_gucs);
        // PostgreSQL's CONTEXT names the relation without its schema.
        let display_relation = table
            .rsplit('.')
            .next()
            .unwrap_or(table)
            .trim_matches('"')
            .to_string();
        // A COPY target keeps its schema qualifier (pgwire no longer
        // collapses it), so apply the SAME schema->physical encoding
        // INSERT/SELECT use before resolving. Without this the qualifier
        // was dropped and `COPY carrier_private.context_nonces` loaded into
        // `public.context_nonces`.
        let qualified = self.bind_copy_relation(table);
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
        let on_error_ignore = rules.is_some_and(|rules| rules.on_error_ignore);
        let condition = rules
            .and_then(|rules| rules.where_clause.as_deref())
            .map(crate::records::parse_check_expression)
            .transpose()?;
        // A skipped or filtered row must not consume sequence values that a
        // bulk allocation would have taken for it: evaluate defaults row by
        // row then, as PostgreSQL does.
        let per_row_defaults = on_error_ignore
            || condition.is_some()
            || default_marks.iter().any(|marks| !marks.is_empty());
        let defaults = if per_row_defaults {
            Vec::new()
        } else {
            self.bulk_missing_insert_defaults(schema.as_ref(), &columns, rows.len())?
        };
        let mut skipped = 0_u64;
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
            let line = lines.and_then(|lines| lines.get(idx).copied());
            // Kept only to name the refused value in the error's CONTEXT.
            let original = (line.is_some() || on_error_ignore).then(|| row.clone());
            let marks = default_marks.get(idx).map(Vec::as_slice).unwrap_or(&[]);
            let converted = (|| -> Result<Record> {
                let mut fields = BTreeMap::new();
                for (field_index, (column, value)) in columns.iter().zip(row).enumerate() {
                    if marks.contains(&field_index) {
                        // The DEFAULT marker: the column default fills it below.
                        continue;
                    }
                    // COPY text goes through each column type's input function.
                    let value = match (
                        value,
                        schema.as_ref().and_then(|schema| schema.column(column)),
                    ) {
                        (Some(text), Some(target)) if copy_input_requires_special_type(target) => {
                            copy_text_input(&text, target).map_err(|error| match line {
                                Some(line) => copy_value_error(
                                    error,
                                    &display_relation,
                                    line,
                                    &target.name,
                                    &text,
                                ),
                                None => error,
                            })?
                        }
                        (value, _) => value.map(SqlValue::String).unwrap_or(SqlValue::Null),
                    };
                    fields.insert(column.clone(), value);
                }
                self.apply_bulk_insert_defaults(schema.as_ref(), &defaults, idx, &mut fields)?;
                record_from_insert_fields_with_db(self.db_ref(), &table, schema.as_ref(), fields)
                    .map_err(|error| match (line, original.as_deref()) {
                        (Some(line), Some(original)) => copy_row_conversion_error(
                            error,
                            &display_relation,
                            line,
                            schema.as_ref(),
                            &columns,
                            original,
                        ),
                        _ => error,
                    })
            })();
            let record = match converted {
                Ok(record) => record,
                Err(error) if on_error_ignore && is_soft_copy_input_error(&error) => {
                    // ON_ERROR IGNORE: a value its type's input refuses skips
                    // the row. Anything else still fails the COPY.
                    skipped += 1;
                    let rules = rules.expect("ON_ERROR IGNORE comes with rules");
                    if rules.verbose {
                        let (column, value) = copy_refused_field(
                            schema.as_ref(),
                            &columns,
                            original.as_deref().unwrap_or(&[]),
                        );
                        let line = line.unwrap_or_default();
                        let message = match value {
                            Some(value) => format!(
                                "skipping row due to data type incompatibility at line {line} for column \"{column}\": \"{}\"",
                                copy_context_printout(&value)
                            ),
                            None => format!(
                                "skipping row due to data type incompatibility at line {line} for column \"{column}\": null input"
                            ),
                        };
                        let _ = crate::statement_context::note_notice(SqlNotice {
                            sqlstate: "00000".to_string(),
                            message,
                            fields: vec![SqlErrorField::Context(format!(
                                "COPY {display_relation}"
                            ))],
                        });
                    }
                    if let Some(limit) = rules.reject_limit {
                        if rules.skipped_before + skipped > limit {
                            let error = SqlError::coded(
                                "22P02",
                                format!(
                                    "skipped more than REJECT_LIMIT ({limit}) rows due to data type incompatibility"
                                ),
                                None,
                                None,
                            );
                            // PostgreSQL's CONTEXT names the row and value.
                            let (column, value) = copy_refused_field(
                                schema.as_ref(),
                                &columns,
                                original.as_deref().unwrap_or(&[]),
                            );
                            return Err(match (line, value) {
                                (Some(line), Some(value)) => copy_value_error(
                                    error,
                                    &display_relation,
                                    line,
                                    &column,
                                    &value,
                                ),
                                _ => error,
                            });
                        }
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(condition) = &condition {
                let row = row_from_record(&table, &display_relation, schema.as_ref(), &record)?;
                if !matches!(
                    self.sql_engine().eval_row_truth(&row, condition)?,
                    Some(true)
                ) {
                    continue;
                }
            }
            records.push(record);
        }
        if let Some(parent) = schema
            .as_ref()
            .filter(|schema| schema.partitioning.is_some())
        {
            return Ok(CopyFromOutcome {
                inserted: self.insert_partition_rows(parent, records, false)?.len(),
                skipped,
            });
        }
        // BEFORE triggers can replace or suppress rows. Validate the rows
        // actually being stored, just as INSERT does, rather than allowing
        // COPY to bypass NOT NULL, CHECK and foreign-key constraints.
        let mut records =
            self.apply_before_insert_row_triggers(&table, schema.as_ref(), records)?;
        if let Some(schema) = schema.as_ref() {
            self.materialize_generated_columns(&table, schema, &mut records)?;
        }
        // As INSERT: the row policies first, so a refused key is not
        // revealed by a duplicate-key error; earlier rows keep their own
        // constraint errors.
        let rls_violation = self.first_rls_write_violation(
            &table,
            schema.as_ref(),
            PolicyAction::Insert,
            false,
            &records,
        )?;
        if let Some(schema) = schema.as_ref() {
            let checked = match &rls_violation {
                Some((index, _)) => &records[..*index],
                None => &records[..],
            };
            // Give CHECK typing the batch's declared schema, as query
            // materialization does. Callback queries keep their nested frames.
            let _row_types = crate::slot_types::push_frame();
            let declared_types = Arc::new(schema.clone());
            crate::slot_types::register_table(&table, &declared_types);
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                &table,
                schema,
                checked,
                false,
                Some(&self.sql_engine()),
            )?;
        }
        if let Some((_, error)) = rls_violation {
            return Err(error);
        }
        let count = records.len();
        self.insert_session_records(&table, records.clone())?;
        self.fire_after_insert_triggers(&table, schema.as_ref(), &records)?;
        Ok(CopyFromOutcome {
            inserted: count,
            skipped,
        })
    }

    /// CREATE/DROP AGGREGATE and CAST, ALTER/DROP ROUTINE, statistics
    /// objects and REINDEX. Outlined so the recursive `execute` frames stay
    /// small.
    #[inline(never)]
    fn execute_raw_catalog_compat(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if let Some(result) = self.execute_raw_text_search_ddl(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_event_trigger_ddl(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_create_aggregate(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_drop_aggregate(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_create_cast(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_routine_statement(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_statistics(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_reindex(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_drop_cast(sql)? {
            return Ok(Some(result));
        }
        if let Some(result) = self.execute_raw_foreign_data(sql)? {
            return Ok(Some(result));
        }
        Ok(None)
    }

    pub fn execute(&mut self, sql: &str) -> Result<SqlResult> {
        // String constants first take the one spelling every later stage
        // reads (PostgreSQL's decoding); see `string_constants`. A client
        // statement reads plain constants per standard_conforming_strings;
        // SQL that BicDB runs inside a statement is always standard.
        let (constants, lexer_warnings) = if crate::statement_context::in_statement() {
            (
                crate::string_constants::normalize_string_constants(
                    sql,
                    crate::string_constants::PlainStrings::Standard,
                )?,
                Vec::new(),
            )
        } else {
            // Float text and text search defaults follow the session.
            crate::set_extra_float_digits(crate::extra_float_digits_setting(&self.session_gucs));
            crate::set_default_text_search_config(&crate::default_text_search_config_setting(
                &self.session_gucs,
            ));
            crate::string_constants::normalize_client_string_constants_with_warnings(
                sql,
                &self.session_gucs,
            )?
        };
        let sql: &str = &constants;
        // PostgreSQL's lexer cuts identifiers to 63 bytes (and says so).
        let oracle = oracle::is_oracle_database(self.db_ref())?;
        let truncated = if oracle {
            None
        } else {
            crate::identifier_truncation::truncate_long_identifiers(sql)
        };
        let (sql, truncations) = match &truncated {
            Some((rewritten, truncations)) => (rewritten.as_str(), truncations.as_slice()),
            None => (sql, &[][..]),
        };
        let _cursor_scope = sql_cursors::CursorScope::enter(&self.sql_cursors);
        let postgres_batch = if oracle {
            None
        } else {
            crate::search_path::statements_bound_separately(sql, false, true)
        };
        let current_of = if postgres_batch.is_some() {
            None
        } else {
            self.rewrite_where_current_of(sql)?
        };
        let sql = current_of.as_deref().unwrap_or(sql);
        if postgres_batch.is_none() {
            if let Some(result) = self.execute_raw_cursor_statement(sql)? {
                return Ok(result);
            }
        }
        if !matches!(
            normalize_sql(sql).as_str(),
            "rollback" | "rollback work" | "rollback transaction"
        ) {
            self.admit_partition_execution()?;
        }
        let _preflight = self.prepare_catalog_preflight_text(sql)?;
        let preprocessing_reads = if !sql.contains(';') || split_sql_statements(sql).len() == 1 {
            self.enter_statement_catalog_reads(sql)?
        } else {
            None
        };
        let _case = oracle::simple_case::Scope::for_db(self.db_ref())?;
        self.statement_error_recovered = false;
        self.db_ref().check_recovery_state()?;
        // Unqualified relations resolve through the session's search_path;
        // routine bodies compiled during this statement bind the same way.
        let temp_schema = self.temp_schema_name();
        let _temp_scope = crate::search_path::TempSchemaScope::enter(temp_schema.as_deref());
        let path = crate::search_path::binding_path_with_temp(
            self.db_ref(),
            &self.session_gucs,
            temp_schema.as_deref(),
        )?;
        // A statement list binds each statement just before it runs, so a
        // statement sees the path and schemas earlier statements set up.
        let per_statement = if oracle {
            crate::search_path::statements_bound_separately(sql, path.is_some(), false)
        } else {
            postgres_batch
        };
        let path = if per_statement.is_some() { None } else { path };
        let bound = match path.as_deref() {
            Some(path) => crate::search_path::bind_sql_text(self.db_ref(), sql, path)?,
            None => None,
        };
        let _path_scope = crate::search_path::PathScope::enter(path);
        let _catalog_scope = crate::search_path::CatalogScope::enter(&self.session_gucs);
        let _stack_depth = crate::max_stack_depth::Scope::enter(&self.session_gucs);
        let sql = bound.as_deref().unwrap_or(sql);
        // A per-row routine call reaches raw SQL only through dynamic EXECUTE
        // fallbacks for statements sqlparser cannot represent (DDL, grants).
        if self.row_routine_call {
            return Err(row_routine_write_error());
        }
        // Statement-scoped only: CREATE/DROP INDEX between statements must
        // not be answered from a stale arbiter.
        self.conflict_arbiter_memo.borrow_mut().take();
        if let Some(tx) = self.tx.as_ref() {
            tx.set_blocking_row_waits(self.blocking_row_wait_token());
        }
        drop(preprocessing_reads);
        let effects = crate::statement_context::StatementScope::enter();
        // The lexer's warnings belong to this statement's diagnostics.
        for warning in lexer_warnings {
            crate::statement_context::note_notice(warning)?;
        }
        for truncation in truncations {
            crate::statement_context::report_notice(
                None,
                "42622",
                format!(
                    "identifier \"{}\" will be truncated to \"{}\"",
                    truncation.identifier, truncation.truncated
                ),
            );
        }
        let atomic = self.tx.is_some() && oracle::preview_routine_language_enabled(self.db_ref())?;
        let execute = |session: &mut Self| {
            session.with_implicit_guc_statement(|session| {
                let mut result = match &per_statement {
                    Some(statements) => {
                        let mut last = None;
                        for statement in statements {
                            let current_of = if oracle {
                                None
                            } else {
                                session.rewrite_where_current_of(statement)?
                            };
                            let statement = current_of.as_deref().unwrap_or(statement);
                            if !oracle {
                                if let Some(result) =
                                    session.execute_raw_cursor_statement(statement)?
                                {
                                    last = Some(result);
                                    continue;
                                }
                            }
                            let preprocessing_reads =
                                session.enter_statement_catalog_reads(statement)?;
                            let current_temp_schema = if oracle {
                                temp_schema.clone()
                            } else {
                                session.temp_schema_name()
                            };
                            let _temp_scope = (!oracle).then(|| {
                                crate::search_path::TempSchemaScope::enter(
                                    current_temp_schema.as_deref(),
                                )
                            });
                            let path = crate::search_path::binding_path_with_temp(
                                session.db_ref(),
                                &session.session_gucs,
                                current_temp_schema.as_deref(),
                            )?;
                            let bound = match path.as_deref() {
                                Some(path) => crate::search_path::bind_sql_text(
                                    session.db_ref(),
                                    statement,
                                    path,
                                )?,
                                None => None,
                            };
                            let _path_scope = crate::search_path::PathScope::enter(path);
                            session.conflict_arbiter_memo.borrow_mut().take();
                            drop(preprocessing_reads);
                            last =
                                Some(session.execute_text_statements(
                                    bound.as_deref().unwrap_or(statement),
                                )?);
                        }
                        last.ok_or_else(|| SqlError::InvalidSql("empty SQL statement".into()))?
                    }
                    None => session.execute_text_statements(sql)?,
                };
                session.apply_bytea_output(&mut result);
                Ok(result)
            })
        };
        let (mut result, recovered) = if atomic {
            self.with_oracle_statement_boundary(execute)
        } else {
            (execute(self), false)
        };
        // A statement outside a transaction block committed on its own: its
        // temporary tables' ON COMMIT DELETE ROWS applies now.
        // ON COMMIT DROP tables go after their transaction committed (a
        // table with buffered writes cannot be dropped inside it).
        if result.is_ok() && self.tx.is_none() && !self.in_transaction() {
            if let Some(temp) = self.temp_schema_name() {
                if temp_tables::temp_on_commit_delete_pending(&temp)
                    || temp_tables::temp_on_commit_drop_pending(&temp)
                {
                    if let Err(error) = self.apply_temp_on_commit_actions() {
                        result = Err(error);
                    }
                }
            }
        }
        self.independent_commits_observed = effects.independently_committed();
        self.independent_execution_observed = effects.independent_execution();
        self.latest_independent_sequence = effects.latest_sequence();
        self.observe_independent_commit()?;
        let result = match result {
            Ok(result) => Ok(result),
            Err(error) => {
                let error = oracle::errors::profile_constraint_error(self.db_ref(), error)?;
                self.statement_error_recovered = recovered;
                Err(error)
            }
        };
        effects.finish(self.authorize_error_value_details(result))
    }

    /// A constraint violation's DETAIL shows row values (`Key (a)=(1) already
    /// exists.`) only when the current role could read them, as PostgreSQL
    /// decides: never under row-level security that applies to the role,
    /// otherwise with SELECT on the relation. Other violations keep their
    /// value-free fallback line.
    pub(crate) fn authorize_error_value_details(
        &self,
        result: Result<SqlResult>,
    ) -> Result<SqlResult> {
        let Err(mut error) = result else {
            return result;
        };
        for detail in error.unauthorized_value_details() {
            if let Some(relation) = detail.values_of.clone() {
                detail.authorized = self.may_read_row_values(&relation);
            }
        }
        Err(error)
    }

    fn may_read_row_values(&self, relation: &str) -> bool {
        let db = self.db_ref();
        if session_role_is_superuser(db, &self.session_gucs) {
            return true;
        }
        let role = current_user_from_gucs(&self.session_gucs);
        let Ok(Some(schema)) = load_schema(db, relation) else {
            return false;
        };
        let owner = schema.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME);
        if schema.rls_enabled && (schema.rls_forced || normalize_role_name(owner) != role) {
            return false;
        }
        role_has_table_privilege(db, &role, &schema.name, "SELECT").unwrap_or(false)
    }

    /// Statement text through the profiled executor; a read-only transaction
    /// checks a single statement before it runs.
    fn execute_text_statements(&mut self, sql: &str) -> Result<SqlResult> {
        let _random = crate::pg_random::RandomScope::enter(&self.random_state);
        let _xml_settings = crate::xml::XmlSettingsScope::enter(&self.session_gucs);
        let _preflight = self.prepare_catalog_preflight_text(sql)?;
        let _catalog_reads = if !sql.contains(';') || split_sql_statements(sql).len() == 1 {
            self.enter_statement_catalog_reads(sql)?
        } else {
            None
        };
        self.acquire_postgres_statement_snapshot(|| {
            (!sql.contains(';') || split_sql_statements(sql).len() == 1)
                && postgres_execution_requires_snapshot(sql)
        })?;
        self.refresh_client_statement_snapshot()?;
        let _authored_text_search =
            crate::fts::authored::Scope::enter(self.db_ref(), &self.session_gucs)?;
        if !self.transaction_read_only() {
            return self.execute_profiled(sql);
        }
        let options = postgres_transaction_deferrability(sql)?;
        let parse_sql = options
            .as_ref()
            .map_or(sql, |(stripped, _)| stripped.as_str());
        let statements = parse_statements(parse_sql).map_err(|_| read_only::violation())?;
        if statements.len() > 1 {
            return self.execute_statement_list(&statements);
        }
        if let Some(statement) = statements.first() {
            self.ensure_read_only_statement(statement)?;
        }
        self.execute_profiled(sql)
    }

    /// True only after restoring a failed preview statement's pending work.
    /// Transport layers must not infer recoverability from an error code alone.
    pub fn last_statement_error_recovered(&self) -> bool {
        self.statement_error_recovered
    }

    fn with_oracle_statement_boundary<T>(
        &mut self,
        execute: impl FnOnce(&mut Self) -> Result<T>,
    ) -> (Result<T>, bool) {
        let tx = self.tx.as_ref().expect("pending Oracle statement");
        let id = tx.id();
        let mark = tx.rollback_mark();
        let large_objects = match self.pg_large_objects.savepoint() {
            Ok(mark) => mark,
            Err(error) => return (Err(error), false),
        };
        let ddl_len = self.ddl_undo.len();
        let savepoint_len = self.savepoints.len();
        let mut gucs = self.session_guc_state();
        let deferred = std::mem::replace(&mut self.defer_commit, true);
        let result = execute(self);
        self.defer_commit = deferred;
        if result.is_ok() {
            return (result, false);
        }
        let cleanup = (|| {
            match self.tx.as_mut() {
                Some(tx) if tx.id() == id => {
                    self.pg_large_objects.restore(large_objects)?;
                    tx.rollback_to_mark(mark)?;
                    self.rollback_ddl_to_len(ddl_len)?;
                    self.savepoints.truncate(savepoint_len);
                    gucs.sequence_values = std::mem::take(&mut *self.sequence_values.borrow_mut());
                    self.install_session_guc_state(gucs);
                }
                Some(_) => {
                    self.rollback_current_transaction()?;
                }
                // A COMMIT/ROLLBACK inside the call ended the original unit.
                // Its durable effects must not be undone with an old mark.
                None => {}
            }
            Ok(())
        })();
        match cleanup {
            Ok(()) => (result, true),
            Err(error) => (Err(error), false),
        }
    }

    /// Whether this statement crossed an independent durable commit boundary.
    /// A server must not automatically replay such a statement on fallback.
    pub fn last_statement_had_independent_commits(&self) -> bool {
        self.independent_commits_observed
    }
    /// Independent calls must not be automatically replayed, including when a
    /// commit returns a delivery error after its durability boundary.
    pub fn last_statement_had_independent_execution(&self) -> bool {
        self.independent_execution_observed
    }
    pub fn last_independent_commit_sequence(&self) -> u64 {
        self.latest_independent_sequence
    }

    fn observe_independent_commit(&mut self) -> Result<()> {
        self.snapshot_floor = self.snapshot_floor.max(self.latest_independent_sequence);
        if self.independent_commits_observed {
            if let Some(tx) = self.tx.as_mut() {
                tx.refresh_read_committed_snapshot_after(self.latest_independent_sequence)?;
            }
        }
        Ok(())
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
        let _random = crate::pg_random::RandomScope::enter(&self.random_state);
        if !matches!(statement, Statement::Rollback { .. }) {
            self.admit_partition_execution()?;
        }
        let _preflight = self.prepare_catalog_preflight(matches!(statement, Statement::Call(_)))?;
        let _catalog_reads = self.enter_parsed_catalog_reads(statement)?;
        let _authored_text_search =
            crate::fts::authored::Scope::enter(self.db_ref(), &self.session_gucs)?;
        let _case = oracle::simple_case::Scope::for_db(self.db_ref())?;
        self.statement_error_recovered = false;
        let _clock = oracle::current_time::StatementScope::new();
        self.db_ref().check_recovery_state()?;
        let effects = crate::statement_context::StatementScope::enter();
        let result = self.with_implicit_guc_statement(|session| {
            let _xml_settings = crate::xml::XmlSettingsScope::enter(&session.session_gucs);
            let mut result = session.execute_statement(statement)?;
            session.apply_bytea_output(&mut result);
            Ok(result)
        });
        self.independent_commits_observed = effects.independently_committed();
        self.independent_execution_observed = effects.independent_execution();
        self.latest_independent_sequence = effects.latest_sequence();
        self.observe_independent_commit()?;
        effects.finish(self.authorize_error_value_details(result))
    }

    /// Execute a stored procedure directly from its already-typed arguments,
    /// bypassing SQL text parsing and `CALL` AST construction.
    pub fn call_procedure(&mut self, name: &str, args: &[SqlValue]) -> Result<SqlResult> {
        let _random = crate::pg_random::RandomScope::enter(&self.random_state);
        self.admit_partition_execution()?;
        let _preflight = self.prepare_catalog_preflight(true)?;
        let _catalog_reads = self.enter_catalog_reads_requiring_snapshot(true)?;
        let _case = oracle::simple_case::Scope::for_db(self.db_ref())?;
        self.statement_error_recovered = false;
        self.db_ref().check_recovery_state()?;
        let effects = crate::statement_context::StatementScope::enter();
        let result = self.with_implicit_guc_statement(|session| {
            let name = name.to_ascii_lowercase();
            let routine = resolve_routine_cached(session.db_ref(), RoutineKind::Procedure, &name)?
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("procedure \"{name}\" does not exist"))
                })?;
            session.ensure_routine_execute_privilege(&name)?;
            session.execute_with_routine_security(&routine.schema, |session| {
                session.execute_plpgsql_procedure(&routine.ir, args)
            })
        });
        self.independent_commits_observed = effects.independently_committed();
        self.independent_execution_observed = effects.independent_execution();
        self.latest_independent_sequence = effects.latest_sequence();
        self.observe_independent_commit()?;
        effects.finish(self.authorize_error_value_details(result))
    }

    pub(crate) fn execute_inner(&mut self, sql: &str) -> Result<SqlResult> {
        if self.event_trigger_ddl_depth == 0
            && !event_triggers::list_event_triggers(self.db_ref())?.is_empty()
        {
            let tag = event_triggers::event_trigger_command_tag(sql);
            return self.with_ddl_event_triggers(tag, |session| session.execute_inner_body(sql));
        }
        self.execute_inner_body(sql)
    }

    fn execute_inner_body(&mut self, sql: &str) -> Result<SqlResult> {
        self.cancellation.check()?;
        let _statement_transaction = crate::statement_context::StatementTransactionScope::enter(
            self.tx.as_ref().or(self.parent_tx).map(|tx| tx.id().0),
        );
        if sql_has_no_statements(sql) {
            return Ok(SqlResult::empty(Vec::new()));
        }
        if let Some(command) = crate::two_phase::parse_two_phase_command(sql)? {
            return self.execute_two_phase(command);
        }
        if let Some(result) = self.execute_raw_insert_overriding(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_create_schema_elements(sql)? {
            return Ok(result);
        }
        if leading_keyword_bypasses_raw_probes(sql) {
            return self.execute_parsed_sql(sql);
        }
        if let Some(result) = self.execute_raw_oracle_collection(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_alter_role_settings(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_create_rule(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_drop_rule(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_reset(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_flashback(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_comment(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_catalog_compat(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_vacuum_analyze(sql)? {
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

        if let Some(function) = parse_raw_alter_procedure(sql)? {
            return self.execute_alter_routine(&function, RoutineKind::Procedure);
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

        if let Some(result) = self.execute_raw_not_null_constraints(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_column_identity(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_table_access_method(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_typed_table(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_view_options(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_owned_objects(sql)? {
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

        if let Some(result) = self.execute_raw_constraint_command(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_alter_table_set_schema(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_alter_user_type(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_relation_forms(sql)? {
            return Ok(result);
        }

        if let Some(result) = self.execute_raw_temporal_key_ddl(sql)? {
            return Ok(result);
        }
        // Temporal-key forms the DDL above does not take stay refused.
        self.refuse_temporal_keys(sql)?;

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

        if let Some(result) = self.execute_raw_maintain_privileges(sql)? {
            return Ok(result);
        }
        if let Some(result) = self.execute_raw_large_object_privileges(sql)? {
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
        if let Some((stripped, deferrable)) = postgres_transaction_deferrability(sql)? {
            if oracle::preview_routine_language_enabled(self.db_ref())? {
                return Err(SqlError::Unsupported(
                    "DEFERRABLE transaction mode requires the PostgreSQL profile".into(),
                ));
            }
            let statements = parse_statements(&stripped)?;
            if matches!(statements.as_slice(), [Statement::Set(_)]) {
                self.validate_transaction_deferrability()?;
            }
            let result = self.execute_statement_list(&statements)?;
            self.apply_transaction_deferrability(deferrable)?;
            return Ok(result);
        }
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
        self.execute_statement_list_checked(statements, true)
    }

    /// Executes `statements`; with `check_columns`, each statement's column
    /// references are resolved first (42703/42P01 for names no relation in
    /// scope provides) right before it runs, so earlier DDL in the batch is
    /// visible.
    fn execute_statement_list_checked(
        &mut self,
        statements: &[Statement],
        check_columns: bool,
    ) -> Result<SqlResult> {
        let mut last_result = None;
        for statement in statements {
            let _preflight =
                self.prepare_catalog_preflight(matches!(statement, Statement::Call(_)))?;
            let _catalog_reads = self.enter_parsed_catalog_reads(statement)?;
            if check_columns && self.column_references_checkable() {
                self.sql_engine()
                    .check_statement_column_references(statement)?;
            }
            last_result = Some(self.execute_parsed_statement(statement)?);
        }
        last_result.ok_or_else(|| SqlError::InvalidSql("empty SQL statement".to_string()))
    }

    fn enter_statement_catalog_reads(
        &mut self,
        sql: &str,
    ) -> Result<Option<bicdb_core::CatalogReadAccess>> {
        self.enter_catalog_reads_requiring_snapshot(postgres_execution_requires_snapshot(sql))
    }

    fn enter_parsed_catalog_reads(
        &mut self,
        statement: &Statement,
    ) -> Result<Option<bicdb_core::CatalogReadAccess>> {
        if self.read_tx().is_none() && self.catalog_preflight_tx.borrow().is_none() {
            return self
                .db_ref()
                .enter_catalog_read_access(None)
                .map_err(Into::into);
        }
        let needs_snapshot = match statement {
            Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_) => true,
            _ => postgres_execution_requires_snapshot(&statement.to_string()),
        };
        self.enter_catalog_reads_requiring_snapshot(needs_snapshot)
    }

    fn enter_catalog_reads_requiring_snapshot(
        &mut self,
        needs_snapshot: bool,
    ) -> Result<Option<bicdb_core::CatalogReadAccess>> {
        if needs_snapshot {
            self.acquire_postgres_statement_snapshot(|| true)?;
        }
        // DDL does not need a data snapshot, but its metadata reads still
        // belong to the pending transaction. An anonymous reader would be
        // refused by that transaction's own retained rename reservation.
        // Read identity grants no internal catalog write authority.
        self.enter_current_catalog_reads()
    }

    fn enter_current_catalog_reads(&self) -> Result<Option<bicdb_core::CatalogReadAccess>> {
        let preflight = self.catalog_preflight_tx.borrow();
        self.db_ref()
            .enter_catalog_read_access(self.read_tx().or(preflight.as_ref()))
            .map_err(Into::into)
    }

    fn prepare_catalog_preflight_text(&self, sql: &str) -> Result<Option<CatalogPreflightScope>> {
        let call =
            leading_sql_keyword(sql).is_some_and(|keyword| keyword.eq_ignore_ascii_case("call"));
        self.prepare_catalog_preflight(
            call && (!sql.contains(';') || split_sql_statements(sql).len() == 1),
        )
    }

    fn prepare_catalog_preflight(&self, call: bool) -> Result<Option<CatalogPreflightScope>> {
        if !call
            || self.read_tx().is_some()
            || self.catalog_preflight_tx.borrow().is_some()
            || self.row_routine_call
        {
            return Ok(None);
        }
        let floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        let mut tx = self.db_ref().begin_transaction_after(floor)?;
        tx.defer_statement_snapshot()?;
        *self.catalog_preflight_tx.borrow_mut() = Some(tx);
        Ok(Some(CatalogPreflightScope {
            transaction: Rc::clone(&self.catalog_preflight_tx),
        }))
    }

    /// Direct embedding sessions need the same READ COMMITTED statement
    /// boundary as pgwire. Native DDL copy/undo commits must be visible to the
    /// next client statement, including statements in one submitted SQL batch.
    /// Row/routine evaluators retain their existing enclosing-statement view;
    /// fixed-snapshot transactions are unchanged by the native refresh API.
    fn refresh_client_statement_snapshot(&mut self) -> Result<()> {
        if self.column_references_checkable() {
            let floor = self
                .snapshot_floor
                .max(crate::statement_context::latest_independent_commit());
            if let Some(tx) = self.tx.as_mut() {
                tx.refresh_read_committed_snapshot_after(floor)?;
            }
        }
        Ok(())
    }

    fn acquire_postgres_statement_snapshot(
        &mut self,
        requires_snapshot: impl FnOnce() -> bool,
    ) -> Result<()> {
        if !self.column_references_checkable()
            || !self
                .tx
                .as_ref()
                .is_some_and(|tx| !tx.statement_snapshot_acquired())
            || oracle::preview_routine_language_enabled(self.db_ref())?
            || !requires_snapshot()
        {
            return Ok(());
        }
        let floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        let serializable = self
            .session_gucs
            .get("transaction_isolation")
            .is_some_and(|level| level == "serializable");
        let deferrable = self
            .session_gucs
            .get("transaction_deferrable")
            .is_some_and(|value| value == "on");
        let read_only = self.transaction_read_only();
        let tx = self.tx.as_mut().unwrap();
        if serializable {
            tx.acquire_postgres_serializable_snapshot(
                floor,
                read_only,
                deferrable,
                &self.cancellation,
            )?;
        } else {
            tx.acquire_statement_snapshot(floor)?;
        }
        Ok(())
    }

    /// Column references are checked for client statements only: routine and
    /// trigger bodies resolve their variables through frames this check does
    /// not model.
    fn column_references_checkable(&self) -> bool {
        self.routine_vars.is_empty()
            && self.routine_slots.is_none()
            && self.trigger_depth == 0
            && !self.in_routine_expression
            && !self.row_routine_call
    }

    /// Catalog generations a column-reference check result depends on.
    fn column_check_stamp(&self) -> [u64; 3] {
        let db = self.db_ref();
        [
            db.collection_generation(SCHEMA_COLLECTION),
            db.collection_generation(VIEW_COLLECTION),
            db.collection_generation(ROUTINE_COLLECTION),
        ]
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
        // The template's column references are checked once per catalog
        // state, not on every execution of the statement.
        let stamp = self.column_check_stamp();
        let check_columns = cached.columns_checked != Some(stamp);
        let result = cached.bind(&literals).and_then(|()| {
            let _types = self.enter_cached_statement_expr_type_scope(&cached);
            self.execute_statement_list_checked(&cached.statements, check_columns)
        });
        if result.is_ok() && check_columns && self.column_references_checkable() {
            cached.columns_checked = Some(stamp);
        }
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
            if let Some(result) = self.execute_raw_oracle_collection(sql)? {
                return Ok(result);
            }
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
            if let Some(result) = self.execute_raw_constraint_command(sql)? {
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
            if let Some(result) = self.execute_raw_maintain_privileges(sql)? {
                return Ok(result);
            }
            if let Some(result) = self.execute_raw_large_object_privileges(sql)? {
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
            if let Some(result) = self.execute_raw_insert_overriding(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_create_schema_elements(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_not_null_constraints(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_alter_column_identity(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_table_access_method(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_typed_table(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_view_options(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_owned_objects(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_oracle_collection(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_extension_catalog(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_reset(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_cursor_statement(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_comment(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_catalog_compat(&statement)? {
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
            if let Some(result) = self.execute_raw_constraint_command(&statement)? {
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
            if let Some(result) = self.execute_raw_maintain_privileges(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_large_object_privileges(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_function_privileges(&statement)? {
                last_result = Some(result);
                continue;
            }
            if let Some(result) = self.execute_raw_temporal_key_ddl(&statement)? {
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
        let _preflight = self.prepare_catalog_preflight(matches!(statement, Statement::Call(_)))?;
        let _catalog_reads = self.enter_parsed_catalog_reads(statement)?;
        let _clock = oracle::current_time::StatementScope::new();
        oracle::lock_waits::validate(self.db_ref(), statement)?;
        let previous = std::mem::replace(&mut self.in_routine_expression, false);
        let result = self.execute_with_postgres_default_isolation(statement);
        self.in_routine_expression = previous;
        result
    }

    fn execute_with_postgres_default_isolation(
        &mut self,
        statement: &Statement,
    ) -> Result<SqlResult> {
        if self.tx.is_some()
            || self
                .session_gucs
                .get("default_transaction_isolation")
                .is_none_or(|level| level != "serializable")
            || !postgres_statement_has_parse_snapshot(statement)
            || oracle::preview_routine_language_enabled(self.db_ref())?
        {
            return self.execute_parsed_statement_in_sql(statement);
        }
        self.with_statement_transaction(|session| {
            session.set_transaction_isolation_label("serializable")?;
            session.execute_parsed_statement_in_sql(statement)
        })
    }

    fn execute_parsed_statement_in_sql(&mut self, statement: &Statement) -> Result<SqlResult> {
        if let Statement::Set(set) = statement {
            self.require_local_transaction_set_control(set)?;
        }
        if matches!(
            statement,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback {
                    savepoint: None,
                    ..
                }
        ) {
            // Refuse before snapshots, hooks, state changes or transaction
            // extraction. ROLLBACK TO/SAVEPOINT/RELEASE remain local operations.
            self.require_local_transaction_control()?;
        }
        self.ensure_read_only_statement(statement)?;
        self.acquire_postgres_statement_snapshot(|| {
            postgres_execution_requires_snapshot(&statement.to_string())
        })?;
        self.refresh_client_statement_snapshot()?;
        if let Some(result) = self.apply_instead_rule(statement)? {
            return Ok(result);
        }
        // Earlier statements in a batch can change temporal output settings.
        let _temporal_scope = self.temporal_scope();
        self.db_ref().check_recovery_state()?;
        if self.row_routine_call && !statement_is_plain_query(statement) {
            return Err(row_routine_write_error());
        }
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
        let result = match statement {
            Statement::StartTransaction { modes, .. } => {
                validate_profile_transaction_modes(
                    modes,
                    oracle::preview_routine_language_enabled(self.db_ref())?,
                )?;
                if self.tx.is_some() {
                    return Err(SqlError::Unsupported(
                        "nested transactions are not supported".to_string(),
                    ));
                }
                self.tx = Some(self.begin_session_transaction()?);
                self.set_transaction_isolation_label(self.default_transaction_isolation()?)?;
                let read_only = modes
                    .iter()
                    .find_map(|mode| match mode {
                        TransactionMode::AccessMode(TransactionAccessMode::ReadOnly) => Some(true),
                        TransactionMode::AccessMode(TransactionAccessMode::ReadWrite) => {
                            Some(false)
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| self.transaction_read_only());
                self.set_transaction_read_only_label(read_only)?;
                self.apply_transaction_modes(modes)?;
                Ok(SqlResult::command("BEGIN"))
            }
            Statement::Commit { chain: true, .. } => self.finish_chained_transaction(true),
            Statement::Commit { .. } => self.commit(),
            Statement::Rollback {
                savepoint, chain, ..
            } => {
                if let Some(savepoint) = savepoint {
                    return self.rollback_to_savepoint(savepoint);
                }
                if *chain {
                    self.finish_chained_transaction(false)
                } else {
                    self.rollback_current_transaction()
                }
            }
            Statement::Savepoint { name } => self.create_savepoint(name),
            Statement::ReleaseSavepoint { name } => self.release_savepoint(name),
            Statement::Set(set) => self.execute_set(set),
            Statement::Reset(_) => self
                .execute_raw_reset(&statement.to_string())?
                .ok_or_else(|| SqlError::InvalidSql("invalid RESET statement".into())),
            statement => self.execute_statement(statement),
        };
        if self.oracle_autonomous
            && result.is_ok()
            && matches!(
                statement,
                Statement::Insert(_)
                    | Statement::Merge(_)
                    | Statement::Update(_)
                    | Statement::Delete(_)
                    | Statement::Savepoint { .. }
            )
        {
            self.oracle_autonomous_active = true;
        }
        result
    }

    fn finish_chained_transaction(&mut self, commit: bool) -> Result<SqlResult> {
        self.require_local_transaction_control()?;
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| SqlError::InvalidTransactionState {
                message: "AND CHAIN requires an active transaction".into(),
            })?;
        let fixed = tx.isolation() != bicdb_core::TransactionIsolation::ReadCommitted;
        let read_only = self.transaction_read_only();
        let deferrable = self
            .session_gucs
            .get("transaction_deferrable")
            .is_some_and(|value| value == "on");
        let isolation = self
            .session_gucs
            .get("transaction_isolation")
            .cloned()
            .unwrap_or(self.default_transaction_isolation()?.into());
        let oracle = oracle::preview_routine_language_enabled(self.db_ref())?;
        let result = if commit {
            self.commit()?
        } else {
            self.rollback_current_transaction()?
        };

        let mut next = self.begin_session_transaction()?;
        let floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        if oracle {
            next.set_snapshot_isolation(fixed, floor)?;
        } else {
            next.set_deferred_snapshot_isolation(fixed, floor)?;
        }
        next.set_statement_write_conflict_check(fixed)?;
        self.set_transaction_read_only_label(read_only)?;
        self.set_transaction_isolation_label(&isolation)?;
        let mut state = self.take_session_guc_state();
        state.set_transaction_deferrable(deferrable);
        self.install_session_guc_state(state);
        self.tx = Some(next);
        Ok(result)
    }

    /// Internal rollback cleanup must run even if the statement was canceled.
    pub(crate) fn rollback_current_transaction(&mut self) -> Result<SqlResult> {
        self.pg_large_objects.clear();
        self.oracle_autonomous_active = false;
        self.oracle_autonomous_epoch += 1;
        // A rolled-back transaction's deferred constraint triggers
        // drop with the transaction itself.
        // Journal undo below reads as this owner whatever frame the running
        // statement entered: after a failed statement that frame has none.
        let owner_reads = self
            .tx
            .as_ref()
            .and_then(|tx| tx.enter_catalog_read_access().ok());
        let tx_rollback = self
            .tx
            .take()
            .map(|tx| tx.rollback().map_err(SqlError::from))
            .unwrap_or(Ok(()));
        let ddl_rollback = self.rollback_ddl_after_transaction_end(owner_reads);
        self.savepoints.clear();
        self.rollback_guc_transaction();
        tx_rollback?;
        ddl_rollback?;
        Ok(SqlResult::command("ROLLBACK"))
    }

    pub(crate) fn commit(&mut self) -> Result<SqlResult> {
        // Also guard internal commit entry points reached through nested CALL
        // and dynamic SQL, before deferred hooks or any durable effect.
        self.require_local_transaction_control()?;
        if self.oracle_autonomous && !self.pending_notifications.is_empty() {
            return Err(SqlError::Unsupported(
                "independent notification delivery is not implemented".into(),
            ));
        }
        // Deferred constraint triggers fire now, with the transaction still
        // open, so their checks see the complete write set and their failures
        // abort the commit like any failed statement.
        if self
            .tx
            .as_ref()
            .is_some_and(Transaction::has_deferred_hooks)
        {
            if let Err(error) = self.fire_deferred_row_triggers() {
                self.pg_large_objects.clear();
                let owner_reads = self
                    .tx
                    .as_ref()
                    .and_then(|tx| tx.enter_catalog_read_access().ok());
                if let Some(tx) = self.tx.take() {
                    let _ = tx.rollback();
                }
                let _ddl_rollback = self.rollback_ddl_after_transaction_end(owner_reads);
                self.savepoints.clear();
                self.rollback_guc_transaction();
                return Err(error);
            }
        }
        if self.tx.is_some() {
            self.run_temp_on_commit_actions()?;
        }
        self.pg_large_objects.clear();
        let mut committed_sequence = 0;
        if let Some(tx) = self.tx.take() {
            let owner_reads = tx.enter_catalog_read_access().ok();
            match tx.commit_with_sequence() {
                Ok(sequence) => committed_sequence = sequence,
                Err(error) => {
                    let _ddl_rollback = self.rollback_ddl_after_transaction_end(owner_reads);
                    self.savepoints.clear();
                    self.rollback_guc_transaction();
                    return Err(error.into());
                }
            }
        }
        if self.oracle_autonomous {
            self.oracle_autonomous_epoch += 1;
            self.oracle_autonomous_active = false;
            crate::statement_context::note_independent_commit(committed_sequence);
            self.snapshot_floor = self.snapshot_floor.max(committed_sequence);
        } else if self.defer_commit {
            crate::statement_context::note_independent_commit(committed_sequence);
            self.snapshot_floor = self.snapshot_floor.max(committed_sequence);
        }
        let committed_catalog_work = !self.ddl_undo.is_empty();
        finish_committed_catalog_scopes(self.db_ref(), &self.ddl_undo)?;
        self.ddl_undo.clear();
        self.savepoints.clear();
        self.commit_guc_transaction();
        recycle_bin_journal::stage("deferred_purge_decided");
        // The purge decision is already in the durable outer commit. A cleanup
        // error must preserve its marker for startup, never undo committed DDL.
        // Concurrent/shared COMMIT cannot mutate the collection catalog. Its
        // durable purge markers remain hidden until exclusive cleanup/startup.
        if self.exclusive {
            if committed_catalog_work {
                finish_committed_recycle_bin_purges(self.db_mut()?)?;
            } else {
                recycle_bin_journal::recover_purges(self.db_mut()?)?;
            }
        }
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
            pg_large_objects: self.pg_large_objects.savepoint()?,
            ddl_undo_len: self.ddl_undo.len(),
            settings: self.settings,
            session_gucs: self.session_gucs.clone(),
            guc_local_restore: self.guc_local_restore.clone(),
            guc_local_settings_restore: self.guc_local_settings_restore.clone(),
            cursor_mark: self.sql_cursors.mark(),
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
        self.sql_cursors
            .rollback_to(self.savepoints[position].cursor_mark);
        let descriptor_rollback = self
            .pg_large_objects
            .restore(self.savepoints[position].pg_large_objects);
        let write_rollback = tx.rollback_to_mark(transaction);
        let ddl_rollback = self.rollback_ddl_to_len(ddl_undo_len);
        self.settings = settings;
        self.session_gucs = session_gucs;
        self.guc_local_restore = guc_local_restore;
        self.guc_local_settings_restore = guc_local_settings_restore;
        self.savepoints.truncate(position + 1);
        descriptor_rollback?;
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
        require_user_relation_name(name)?;
        let existed = self
            .db_ref()
            .collections()
            .iter()
            .any(|collection| collection.name.eq_ignore_ascii_case(name));
        if !existed && recycle_bin_journal::create_replacement_collection(self, name)? {
            return Ok(());
        }
        self.db_mut()?.create_collection(name)?;
        if self.tx.is_some() && !existed {
            self.fresh_relations.push(name.to_string());
            self.ddl_undo.push(DdlUndo::DropCollection {
                name: name.to_string(),
                creation_intent: None,
                creation_receipt: None,
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
        self.create_session_index_inner(definition, false)
    }

    pub(crate) fn create_new_table_index(&mut self, definition: IndexDefinition) -> Result<()> {
        self.create_session_index_inner(definition, true)
    }

    fn create_session_index_inner(
        &mut self,
        mut definition: IndexDefinition,
        created_in_this_statement: bool,
    ) -> Result<()> {
        let logical_name = catalog_display_name(&definition.name);
        let schema = load_schema(self.db_ref(), &definition.collection)?;
        if let Some(schema) = &schema {
            definition.name = sql_index_storage_name(&schema.schema_name, &definition.name);
        }
        let name = definition.name.clone();
        let unique_shape = definition.unique.then(|| {
            (
                definition.collection.clone(),
                definition.fields.clone(),
                definition.nulls_not_distinct,
                definition.predicate.clone(),
            )
        });
        // A fresh CREATE TABLE has no corpus to rebuild, in autocommit as well
        // as an explicit transaction. For existing tables, the paged build
        // checks that the SQL transaction is the sole active transaction and
        // has acquired no write locks.
        let new_empty_table = self.db_ref().is_server_paged()
            && definition.kind == IndexKind::BTree
            && definition.exclusion.is_none()
            && self
                .tx
                .as_ref()
                .is_none_or(|tx| !tx.has_pending_writes(&definition.collection))
            && (created_in_this_statement
                || self.ddl_undo.iter().any(|undo| {
                    matches!(undo,
                DdlUndo::DropCollection { name, .. } if name == &definition.collection)
                }));
        let created = if new_empty_table {
            self.db_mut()?.create_empty_btree_index(definition)
        } else if let Some(owner) = self.tx.as_ref().map(|tx| tx.id()) {
            self.db_mut()?
                .create_index_for_sql_transaction(definition, owner)
        } else {
            self.db_mut()?.create_index(definition)
        };
        if let Err(error) = created {
            if matches!(error, BicDbError::UniqueIndexBuildConflict { .. }) {
                if let Some(schema) = &schema {
                    let mut error = unique_violation(schema, &logical_name);
                    if let SqlError::ConstraintViolation {
                        unique_origin,
                        message,
                        ..
                    } = &mut error
                    {
                        *unique_origin = Some(UniqueViolationOrigin::IndexBuild);
                        *message = format!("could not create unique index \"{logical_name}\"");
                    }
                    if let Some(detail) = unique_shape
                        .and_then(|shape| self.duplicated_index_key_detail(schema, shape))
                    {
                        error = error.with_error_detail(detail);
                    }
                    return Err(error);
                }
            }
            return Err(error.into());
        }
        sql_index_definitions_cache_clear();
        if self.tx.is_some() {
            self.ddl_undo.push(DdlUndo::DropIndex { name });
        }
        Ok(())
    }

    /// `Key (a)=(1) is duplicated.` of a unique index build that found duplicates.
    fn duplicated_index_key_detail(
        &self,
        schema: &TableSchema,
        (collection, fields, nulls_not_distinct, predicate): (
            String,
            Vec<IndexField>,
            bool,
            Option<bicdb_core::IndexPredicate>,
        ),
    ) -> Option<ErrorDetail> {
        let records = match self.tx.as_ref() {
            Some(tx) => tx.scan_collection_for_integrity_check(&collection),
            None => self.db_ref().scan_collection_unchecked(&collection),
        }
        .ok()?;
        let key = UniqueKey::IndexFields(fields);
        let values = crate::error_detail::smallest_duplicated_key(
            schema,
            &key,
            &records,
            nulls_not_distinct,
            predicate.as_ref(),
        )
        .ok()??;
        crate::error_detail::unique_key_detail(schema, &key, &values, "is duplicated.")
    }

    pub(crate) fn drop_session_index(&mut self, name: &str, table: Option<&str>) -> Result<bool> {
        let mut internal_names = Vec::new();
        for schema in list_schemas(self.db_ref())? {
            if table != Some(schema.name.as_str()) {
                continue;
            }
            // Includes implicit constraint indexes, which have no IndexSchema.
            internal_names.push(sql_index_storage_name(&schema.schema_name, name));
            for index in &schema.indexes {
                if index.name == name {
                    internal_names.extend(
                        index
                            .internal_index_names
                            .iter()
                            .map(|name| sql_index_storage_name(&schema.schema_name, name)),
                    );
                }
            }
        }
        for view in list_views(self.db_ref())? {
            if table != Some(view.name.as_str()) {
                continue;
            }
            let namespace = relation_catalog_namespace(self.db_ref(), &view.name)?;
            internal_names.push(sql_index_storage_name(&namespace, name));
            for index in &view.indexes {
                if index.name == name {
                    internal_names.extend(
                        index
                            .internal_index_names
                            .iter()
                            .map(|name| sql_index_storage_name(&namespace, name)),
                    );
                }
            }
        }
        let executable_definitions = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .filter(|definition| {
                table.is_none_or(|table| definition.collection == table)
                    && (definition.name == name
                        || internal_names
                            .iter()
                            .any(|internal| internal == &definition.name))
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
        let metadata_existed = self.remove_session_index_from_schemas(name, table)?;
        Ok(executable_existed || metadata_existed)
    }

    pub(crate) fn rename_session_index(
        &mut self,
        old_name: &str,
        new_name: &str,
        table: &str,
    ) -> Result<bool> {
        let schema = load_schema(self.db_ref(), table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.into()))?;
        let old_storage = sql_index_storage_name(&schema.schema_name, old_name);
        let new_storage = sql_index_storage_name(&schema.schema_name, new_name);
        let executable_definition =
            self.db_ref()
                .index_definitions()
                .into_iter()
                .find(|definition| {
                    definition.collection == table
                        && definition.name.eq_ignore_ascii_case(&old_storage)
                });
        let Some(definition) = executable_definition else {
            return Ok(false);
        };
        if definition.name.eq_ignore_ascii_case(&new_storage) {
            return Ok(true);
        }
        let renamed = self
            .db_mut()?
            .rename_index(&definition.name, &new_storage)?;
        if renamed {
            sql_index_definitions_cache_clear();
        }
        if self.tx.is_some() && renamed {
            self.ddl_undo.push(DdlUndo::RenameIndex {
                old_name: definition.name,
                new_name: new_storage,
            });
        }
        Ok(renamed)
    }

    pub(crate) fn remove_session_index_from_schemas(
        &mut self,
        index_name: &str,
        table: Option<&str>,
    ) -> Result<bool> {
        let mut removed = false;
        for mut schema in list_schemas(self.db_ref())? {
            if table.is_some_and(|table| table != schema.name) {
                continue;
            }
            let before = schema.indexes.len();
            schema.indexes.retain(|index| index.name != index_name);
            if schema.indexes.len() != before {
                self.save_session_schema(&schema)?;
                removed = true;
            }
        }
        for mut view in list_views(self.db_ref())? {
            if table.is_some_and(|table| table != view.name) {
                continue;
            }
            let before = view.indexes.len();
            view.indexes.retain(|index| index.name != index_name);
            if view.indexes.len() != before {
                self.save_session_view(&view)?;
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

    fn rename_journal_contexts(&self) -> Vec<String> {
        self.ddl_undo
            .iter()
            .filter_map(|undo| match undo {
                DdlUndo::RestoreRenameJournal { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    /// Scope only the actual native metadata writer. Preparatory work, default
    /// ACLs and unrelated metadata keep their own normal admission checks.
    fn catalog_record_mutation_access(
        &self,
        collection: &str,
        record: &str,
    ) -> Result<Option<bicdb_core::CatalogScopeAccess>> {
        let Some(owner) = self.tx.as_ref() else {
            return Ok(None);
        };
        rename_journal::record_mutation_access(
            self.db_ref(),
            Some(owner),
            &self.rename_journal_contexts(),
            collection,
            record,
        )
    }

    fn catalog_record_undo_access(
        &self,
        collection: &str,
        record: &str,
    ) -> Result<Option<bicdb_core::CatalogScopeAccess>> {
        rename_journal::record_mutation_access(
            self.db_ref(),
            self.tx.as_ref(),
            &self.rename_journal_contexts(),
            collection,
            record,
        )
    }

    fn catalog_schema_publication_access(
        &self,
        table: &str,
        certificate: bool,
        undo: bool,
    ) -> Result<Option<bicdb_core::CatalogScopeAccess>> {
        if !undo && self.tx.is_none() {
            return Ok(None);
        }
        let names = if certificate {
            vec![partition_admission::CERTIFICATE_COLLECTION.to_string()]
        } else {
            Vec::new()
        };
        rename_journal::record_mutation_access_with_publication_resources(
            self.db_ref(),
            self.tx.as_ref(),
            &self.rename_journal_contexts(),
            SCHEMA_COLLECTION,
            table,
            &names,
        )
    }

    fn catalog_partition_publication_access(
        &self,
        changes: &[(&'static str, String, Option<Record>)],
        undo: bool,
    ) -> Result<Option<bicdb_core::CatalogScopeAccess>> {
        if !undo && self.tx.is_none() {
            return Ok(None);
        }
        let names = [partition_admission::CERTIFICATE_COLLECTION.to_string()];
        let contexts = self.rename_journal_contexts();
        for (collection, id, _) in changes {
            if let Some(access) = rename_journal::record_mutation_access_with_publication_resources(
                self.db_ref(),
                self.tx.as_ref(),
                &contexts,
                collection,
                id,
                &names,
            )? {
                return Ok(Some(access));
            }
        }
        Ok(None)
    }

    pub(crate) fn save_session_schema(&mut self, schema: &TableSchema) -> Result<()> {
        let previous = load_schema(self.db_ref(), &schema.name)?;
        let created = previous.is_none();
        let certificate = schema.partitioning.is_some()
            || schema.partition_of.is_some()
            || previous
                .as_ref()
                .is_some_and(|old| old.partitioning.is_some() || old.partition_of.is_some());
        let saved = {
            let _access =
                self.catalog_schema_publication_access(&schema.name, certificate, false)?;
            let record = prepare_schema_record(self.db_mut()?, schema)?;
            let saved: TableSchema = serde_json::from_value(record.metadata.clone())?;
            partition_admission::publish_schema_change(
                self.db_mut()?,
                schema.name.clone(),
                Some(record),
            )?;
            saved
        };
        cache_saved_schema(self.db_ref(), &saved);
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
        {
            let certificate = previous
                .as_ref()
                .is_some_and(|old| old.partitioning.is_some() || old.partition_of.is_some());
            let _access = self.catalog_schema_publication_access(table, certificate, false)?;
            delete_schema(self.db_mut()?, table)?;
        }
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

    /// The physical name of a relation written as `parts` (`t` or `s.t`),
    /// an unqualified name bound through the active search_path (temporary
    /// schema first) as a statement binds it. `None` when it names nothing.
    pub(crate) fn resolve_written_relation(&self, parts: &[String]) -> Option<String> {
        let mut name = ObjectName::from(
            parts
                .iter()
                .map(|part| Ident::with_quote('"', part))
                .collect::<Vec<_>>(),
        );
        crate::search_path::bind_raw_relation(self.db_ref(), &mut name);
        let physical =
            relation_name_from_parts(&crate::eval::rewrite_helpers::object_name_parts(&name))
                .ok()?;
        resolve_relation_name_memoized(self.db_ref(), &physical)
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
            .filter(|sequence| sequence.owned_by_table.as_deref() == Some(table))
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
        if existed {
            self.drop_owned_sequences_for_table(view)?;
        }
        Ok(existed)
    }

    pub(crate) fn create_session_trigger_if_missing(
        &mut self,
        trigger: TriggerSchema,
        or_replace: bool,
    ) -> Result<()> {
        if let Some(previous) = load_trigger(self.db_ref(), &trigger.table_name, &trigger.name)? {
            if previous.partition_parent.is_some() {
                return Err(SqlError::data_exception(
                    "42710",
                    "cannot replace an inherited partition trigger directly",
                    None,
                ));
            }
        }
        if load_schema(self.db_ref(), &trigger.table_name)?
            .is_some_and(|schema| schema.partitioning.is_some())
            && trigger.for_each == "row"
        {
            return self.create_partition_trigger_tree(trigger, or_replace);
        }
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
        if self.has_partition_trigger_descendants(trigger)? {
            return self.save_partition_trigger_mode_tree(trigger);
        }
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
        self.delete_session_trigger_inner(name, table_name, false)
    }

    /// A trigger of a table being dropped: an inherited partition trigger
    /// goes with its table (DROP TABLE of a partition tree).
    pub(crate) fn delete_dropped_table_trigger(
        &mut self,
        name: &str,
        table_name: &str,
    ) -> Result<bool> {
        self.delete_session_trigger_inner(name, Some(table_name), true)
    }

    fn delete_session_trigger_inner(
        &mut self,
        name: &str,
        table_name: Option<&str>,
        table_dropped: bool,
    ) -> Result<bool> {
        if let Some(trigger) = find_trigger(self.db_ref(), name, table_name)? {
            if trigger.partition_parent.is_some() && !table_dropped {
                return Err(SqlError::data_exception(
                    "2BP01",
                    "cannot drop an inherited partition trigger directly",
                    None,
                ));
            }
            if self.has_partition_trigger_descendants(&trigger)? {
                return self.drop_partition_trigger_tree(&trigger);
            }
        }
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

    /// Undo every DDL effect once the owning transaction is gone. The running
    /// statement bound its metadata reads to that transaction. While a rename
    /// journal retains that owner's catalog scope, undo keeps reading as the
    /// owner, which its reservations require. Once no journal remains the
    /// owner is no longer registered, and the rest reads as the session's
    /// next statement would, with no owner.
    fn rollback_ddl_after_transaction_end(
        &mut self,
        owner_reads: Option<bicdb_core::CatalogReadAccess>,
    ) -> Result<()> {
        while self
            .ddl_undo
            .iter()
            .any(|undo| matches!(undo, DdlUndo::RestoreRenameJournal { .. }))
        {
            let len = self.ddl_undo.len() - 1;
            self.rollback_ddl_to_len(len)?;
        }
        drop(owner_reads);
        let _reads = self.db_ref().enter_catalog_read_access(None)?;
        self.rollback_ddl_to_len(0)
    }

    pub(crate) fn rollback_ddl_to_len(&mut self, len: usize) -> Result<()> {
        while self.ddl_undo.len() > len {
            if let Some(DdlUndo::RestorePartitionTableStates { states, auxiliary }) =
                self.ddl_undo.last().cloned()
            {
                self.restore_partition_table_states(states, auxiliary)?;
                self.ddl_undo.pop();
                continue;
            }
            if let Some(DdlUndo::RestorePartitionCatalog { entries }) =
                self.ddl_undo.last().cloned()
            {
                self.restore_partition_catalog(entries)?;
                self.ddl_undo.pop();
                continue;
            }
            // A live foreign owner can refuse journal recovery or an index
            // rebuild. Keep the matching undo until it succeeds so a later
            // full rollback can retry after that owner finishes. Clone only
            // small identities/definitions, not legacy row payloads.
            let retryable = match self.ddl_undo.last() {
                Some(DdlUndo::RestoreRenameJournal { id, old_table }) => {
                    Some(DdlUndo::RestoreRenameJournal {
                        id: id.clone(),
                        old_table: old_table.clone(),
                    })
                }
                Some(DdlUndo::DropCollection {
                    name,
                    creation_intent: Some(id),
                    creation_receipt,
                }) => Some(DdlUndo::DropCollection {
                    name: name.clone(),
                    creation_intent: Some(id.clone()),
                    creation_receipt: creation_receipt.clone(),
                }),
                Some(DdlUndo::RestoreIndex { definition }) => Some(DdlUndo::RestoreIndex {
                    definition: definition.clone(),
                }),
                _ => None,
            };
            if let Some(undo) = retryable {
                self.apply_ddl_undo(undo)?;
                self.ddl_undo.pop();
                continue;
            }
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
            DdlUndo::DropCollection {
                name,
                creation_intent,
                creation_receipt,
            } => {
                if let Some(id) = creation_intent {
                    recycle_bin_journal::undo_session_creation(
                        self,
                        &id,
                        creation_receipt.as_ref(),
                    )?;
                } else {
                    let _ = self.db_mut()?.drop_collection(&name)?;
                }
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
                // A rename this transaction left undecided still reserves the
                // relation; restore inside that journal's own scope rather
                // than as an unrelated writer it must refuse.
                let _access = self.catalog_record_undo_access(SCHEMA_COLLECTION, &table)?;
                restore_table_state(self.db_mut()?, &table, &schema, records)?;
            }
            DdlUndo::RestoreRenameJournal { id, .. } => {
                rename_journal::undo_session(self, &id)?;
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
                let _access = self.catalog_schema_publication_access(
                    &table,
                    self.db_ref().sql_partition_execution_feature_active(),
                    true,
                )?;
                delete_schema(self.db_mut()?, &table)?;
            }
            DdlUndo::RestoreSchema { schema } => {
                let saved = {
                    let _access = self.catalog_schema_publication_access(
                        &schema.name,
                        self.db_ref().sql_partition_execution_feature_active(),
                        true,
                    )?;
                    let record = prepare_schema_record(self.db_mut()?, &schema)?;
                    let saved: TableSchema = serde_json::from_value(record.metadata.clone())?;
                    partition_admission::publish_schema_change(
                        self.db_mut()?,
                        schema.name.clone(),
                        Some(record),
                    )?;
                    saved
                };
                cache_saved_schema(self.db_ref(), &saved);
            }
            DdlUndo::DeleteSequence { sequence } => {
                let _ = delete_sequence(self.db_mut()?, &sequence)?;
            }
            DdlUndo::RestoreSequence { sequence } => {
                save_sequence(self.db_mut()?, &sequence)?;
            }
            DdlUndo::RestoreRule { key, previous } => {
                crate::query_rules::restore_rule(self.db_mut()?, &key, previous)?;
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
            DdlUndo::RestorePartitionCatalog { entries } => {
                self.restore_partition_catalog(entries)?
            }
            DdlUndo::RestorePartitionTableStates { states, auxiliary } => {
                self.restore_partition_table_states(states, auxiliary)?
            }
            DdlUndo::DeleteUserType { schema_name, name } => {
                let _ = delete_user_type(self.db_mut()?, &schema_name, &name)?;
            }
            DdlUndo::RestoreUserType { user_type } => {
                save_user_type(self.db_mut()?, &user_type)?;
            }
            DdlUndo::DeletePrivilege { grant } => {
                let _access =
                    self.catalog_record_undo_access(PRIVILEGE_COLLECTION, &privilege_key(&grant))?;
                delete_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::RestorePrivilege { grant } => {
                let _access =
                    self.catalog_record_undo_access(PRIVILEGE_COLLECTION, &privilege_key(&grant))?;
                save_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::RestoreFromRecycleBin { bin_name, target } => {
                if let Some(entry) = recycle_bin::list_recycle_bin(self.db_ref())?
                    .into_iter()
                    .find(|entry| entry.bin_name == bin_name)
                {
                    self.restore_from_recycle_bin_undo(&entry, &target)?;
                }
            }
            DdlUndo::ReturnToRecycleBin { intent_id } => {
                recycle_bin_journal::undo_restore(self, &intent_id)?;
            }
            DdlUndo::DeleteDefaultPrivilege { grant } => {
                delete_default_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::RestoreDefaultPrivilege { grant } => {
                save_default_privilege(self.db_mut()?, &grant)?;
            }
            DdlUndo::DeleteRoutine { routine } => {
                delete_routine_schema(self.db_mut()?, &routine)?;
            }
            DdlUndo::RestoreRoutine { routine } => {
                save_routine(self.db_mut()?, &routine)?;
            }
            DdlUndo::RestoreCollation {
                schema_name,
                name,
                previous,
            } => match previous {
                Some(previous) => crate::collations::save_collation(self.db_mut()?, &previous)?,
                None => crate::collations::delete_collation(self.db_mut()?, &schema_name, &name)?,
            },
            DdlUndo::RestoreTextSearchConfiguration { oid, previous } => match previous {
                Some(previous) => crate::fts::authored::store(self.db_mut()?, &previous)?,
                None => crate::fts::authored::delete(self.db_mut()?, oid)?,
            },
            DdlUndo::RestoreTextSearchDictionary { oid, previous } => match previous {
                Some(previous) => {
                    crate::fts::authored::store_dictionary(self.db_mut()?, &previous)?
                }
                None => crate::fts::authored::delete_dictionary(self.db_mut()?, oid)?,
            },
            DdlUndo::RestoreEventTrigger { oid, previous } => {
                event_triggers::restore_event_trigger(self.db_mut()?, oid, previous.as_deref())?
            }
            DdlUndo::RestoreForeignCatalog { previous } => {
                crate::foreign_data::save_foreign_catalog(self.db_mut()?, &previous)?;
            }
            DdlUndo::RestoreOperator { id, previous } => match previous {
                Some(previous) => crate::pg_operators::save_operator(self.db_mut()?, &previous)?,
                None => crate::pg_operators::delete_operator(self.db_mut()?, &id)?,
            },
            DdlUndo::RestoreStatistics {
                schema,
                name,
                previous,
            } => crate::stats_ext::restore_stats(
                self.db_mut()?,
                previous.as_deref(),
                &schema,
                &name,
            )?,
            DdlUndo::RestoreUserCast {
                source,
                target,
                previous,
            } => match previous {
                Some(previous) => crate::user_casts::save_user_cast(self.db_mut()?, &previous)?,
                None => crate::user_casts::delete_user_cast(self.db_mut()?, &source, &target)?,
            },
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

    fn apply_transaction_modes(&mut self, modes: &[TransactionMode]) -> Result<()> {
        self.require_local_transaction_control()?;
        let oracle_preview = oracle::preview_routine_language_enabled(self.db_ref())?;
        let level = modes.iter().find_map(|mode| match mode {
            TransactionMode::IsolationLevel(level) => Some(level),
            _ => None,
        });
        let isolation_label = level.map(transaction_defaults::isolation_label);
        let same_isolation = isolation_label.is_some_and(|requested| {
            self.session_gucs
                .get("transaction_isolation")
                .is_some_and(|current| current == requested)
        });
        if oracle_preview {
            validate_profile_transaction_modes(modes, true)?;
        } else {
            validate_postgres_transaction_modes(modes, self.transaction_read_only())?;
            if isolation_label.is_some()
                && !same_isolation
                && self
                    .tx
                    .as_ref()
                    .is_some_and(Transaction::statement_snapshot_acquired)
            {
                return Err(SqlError::data_exception(
                    "25001",
                    "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                    None,
                ));
            }
        }
        if let Some(read_only) = modes.iter().find_map(|mode| match mode {
            TransactionMode::AccessMode(TransactionAccessMode::ReadOnly) => Some(true),
            TransactionMode::AccessMode(TransactionAccessMode::ReadWrite) => Some(false),
            _ => None,
        }) {
            if (self.tx.is_none() && !oracle_preview) || !self.savepoints.is_empty() {
                return Err(SqlError::data_exception(
                    "25001",
                    "transaction access mode requires an active top-level transaction",
                    None,
                ));
            }
            if !read_only && self.transaction_read_only() {
                return Err(SqlError::data_exception(
                    "25001",
                    "cannot relax an active read-only transaction",
                    None,
                ));
            }
            if self
                .tx
                .as_ref()
                .is_some_and(|tx| tx.write_len() != 0 || tx.lock_len() != 0)
            {
                return Err(SqlError::data_exception(
                    "25001",
                    "transaction access mode must be set before writes or row locks",
                    None,
                ));
            }
            self.set_transaction_read_only_label(read_only)?;
        }
        if !oracle_preview && same_isolation {
            return Ok(());
        }
        let Some(level) = level else {
            return Ok(());
        };
        let fixed = matches!(
            level,
            TransactionIsolationLevel::RepeatableRead | TransactionIsolationLevel::Serializable
        );
        if oracle_preview && self.tx.is_none() {
            self.tx = Some(self.begin_session_transaction()?);
        }
        let floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        if let Some(tx) = self.tx.as_mut() {
            if oracle_preview && (tx.write_len() != 0 || tx.lock_len() != 0) {
                return Err(oracle::errors::database_error(
                    -1453,
                    "25001",
                    "SET TRANSACTION must be first statement of transaction",
                ));
            }
            if oracle_preview {
                tx.set_snapshot_isolation(fixed, floor)?;
            } else {
                tx.set_deferred_snapshot_isolation(fixed, floor)?;
            }
            tx.set_statement_write_conflict_check(fixed)?;
            self.set_transaction_isolation_label(transaction_defaults::isolation_label(level))?;
        }
        Ok(())
    }

    pub(crate) fn execute_set(&mut self, set: &Set) -> Result<SqlResult> {
        self.require_local_transaction_set_control(set)?;
        match set {
            Set::SetTransaction {
                modes,
                snapshot,
                session,
            } => {
                if let Some(snapshot) = snapshot {
                    if *session || !modes.is_empty() {
                        return Err(SqlError::Unsupported(
                            "SET TRANSACTION SNAPSHOT takes no other transaction modes".to_string(),
                        ));
                    }
                    let id = match &snapshot.value {
                        sqlparser::ast::Value::SingleQuotedString(text) => text.clone(),
                        other => other.to_string(),
                    };
                    self.set_transaction_snapshot(&id)?;
                    return Ok(SqlResult::command("SET"));
                }
                if *session {
                    self.set_session_transaction_modes(modes)?;
                } else {
                    self.apply_transaction_modes(modes)?;
                }
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
                    // `SET datestyle = iso, ymd`: the list is one setting value.
                    let joined;
                    let values = if setting == "datestyle" && values.len() > 1 {
                        let text = values
                            .iter()
                            .map(|value| value.to_string().trim_matches('\'').to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        joined = [Expr::Value(
                            sqlparser::ast::Value::SingleQuotedString(text).into(),
                        )];
                        &joined[..]
                    } else {
                        values.as_slice()
                    };
                    let [value] = values else {
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

    pub(crate) fn require_role_management_privilege(&self, action: &str) -> Result<()> {
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
            schema.primary_key_constraint_name() == index
                || schema
                    .constraints
                    .iter()
                    .any(|constraint| constraint_name(constraint) == index)
                || schema
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
        require_user_relation_name(table)?;
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

    /// CREATE on the schema that will hold a new table, view, sequence, type,
    /// domain or routine. Before this, nothing consulted it, so
    /// `REVOKE CREATE ON SCHEMA public FROM PUBLIC` silently left every login
    /// able to create objects. A schema that does not exist yet (several
    /// CREATE paths create it on the fly) needs CREATE on the database, which
    /// is what `CREATE SCHEMA` itself requires.
    pub(crate) fn require_schema_create_privilege(&self, schema: &str) -> Result<()> {
        let schema = schema.to_string();
        if schema == "public" && load_namespace(self.db_ref(), &schema)?.is_none() {
            return Err(SqlError::data_exception_public(
                "3F000",
                "schema \"public\" does not exist",
                None,
            ));
        }
        let role = current_user_from_gucs(&self.session_gucs);
        let database = current_database_from_gucs(&self.session_gucs);
        match role_can_create_in_schema(self.db_ref(), &role, &schema, Some(&database))? {
            Some(true) => Ok(()),
            Some(false) => Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied for schema {schema}"
            )))),
            None => self.require_database_create_privilege(),
        }
    }

    /// CREATE on the current database, required by `CREATE SCHEMA`.
    pub(crate) fn require_database_create_privilege(&self) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        let database = current_database_from_gucs(&self.session_gucs);
        if role_can_create_in_database(self.db_ref(), &role, &database)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for database {database}"
        ))))
    }

    /// Instance-wide operations with no per-table scope: extension install and
    /// activation (executable WASM that registers HTTP routes, event handlers
    /// and queue consumers), audit-history trimming, and the store-wide
    /// vacuum. None of these can be authorized against a relation, and all of
    /// them are administrative, so they take the administrative gate.
    /// `CREATE EXTENSION`: PostgreSQL lets the database owner create *trusted* extensions (13+);
    /// every other extension needs a superuser.
    pub(crate) fn require_extension_creation_authority(&self, extension: &str) -> Result<()> {
        if self.current_user_is_superuser()? {
            return Ok(());
        }
        if is_trusted_postgres_extension(extension) {
            let database = current_database_from_gucs(&self.session_gucs);
            let owner = list_databases(self.db_ref())?
                .into_iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(&database))
                .map(|candidate| candidate.owner);
            let role = current_user_from_gucs(&self.session_gucs);
            if owner.is_some_and(|owner| owner.eq_ignore_ascii_case(&role)) {
                return Ok(());
            }
            return Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "permission denied to create extension \"{extension}\": must be the database owner or a superuser"
            ))));
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied to create extension \"{extension}\": must be superuser"
        ))))
    }

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
            self.apply_database_setting_defaults();
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
                SESSION_CLIENT_ADDRESS_GUC,
                SESSION_CLIENT_HOST_GUC,
                "server_version",
                "server_version_num",
                "bicdb.sql_profile",
                "transaction_isolation",
                "transaction_read_only",
                "transaction_deferrable",
            ];
            Arc::make_mut(&mut self.session_gucs).retain(|key, _| {
                preserved.contains(&key.as_str())
                    || key == POSTGRES_VERSION_BANNER_GUC
                    || key.starts_with(crate::datestyle_output::STARTUP_DEFAULT_PREFIX)
                    || is_protected_security_setting(key)
            });
            // Settings given at connection start reset to those values.
            let defaults = self
                .session_gucs
                .iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(crate::datestyle_output::STARTUP_DEFAULT_PREFIX)
                        .map(|name| (name.to_string(), value.clone()))
                })
                .collect::<Vec<_>>();
            for (name, value) in defaults {
                Arc::make_mut(&mut self.session_gucs).insert(name, value);
            }
            self.settings = SqlSettings::default();
            return Ok(targets);
        }
        // `RESET TIME ZONE` is RESET timezone.
        let setting = if setting.eq_ignore_ascii_case("time zone") {
            "timezone"
        } else {
            setting
        };
        let raw = setting.split_whitespace().next().unwrap_or(setting);
        // Keep dotted GUC names whole; normalize_object_name would strip the
        // qualifier ("app.tenant" -> "tenant").
        let setting = if raw.contains('.') {
            // A parsed statement can render app.option as "app"."option".
            // Decode identifier components before joining the GUC key; merely
            // trimming the outer quotes leaves embedded quote separators.
            sql_identifier_parts(raw)?.join(".").to_ascii_lowercase()
        } else {
            normalize_object_name(raw)
        };
        self.reset_named_setting(setting)
    }

    fn apply_setting_default(&mut self, setting: String) -> Result<()> {
        self.reset_named_setting(setting.clone())?;
        self.apply_database_setting_default(&setting);
        Ok(())
    }

    fn apply_database_setting_default(&mut self, setting: &str) {
        if self.session_gucs.contains_key(setting) || is_protected_security_setting(setting) {
            return;
        }
        let name = current_database_from_gucs(&self.session_gucs);
        if let Ok(mut defaults) = database_settings(self.db_ref(), &name) {
            if let Some(value) = defaults.remove(setting) {
                Arc::make_mut(&mut self.session_gucs).insert(setting.to_string(), value);
            }
        }
    }

    fn reset_named_setting(&mut self, setting: String) -> Result<GucAssignmentTargets> {
        let targets = GucAssignmentTargets::setting(&setting);
        self.require_local_transaction_setting_control(&setting)?;
        if setting == "server_encoding" {
            return Err(SqlError::data_exception_public(
                "55P02",
                "parameter \"server_encoding\" cannot be changed",
                None,
            ));
        }
        if targets.is_current_transaction_mode()
            && !oracle::preview_routine_language_enabled(self.db_ref())?
        {
            return Err(SqlError::Unsupported(format!(
                "parameter \"{setting}\" cannot be reset"
            )));
        }
        if setting == read_only::SETTING {
            return Err(SqlError::Unsupported(
                "transaction access mode must be changed with SET TRANSACTION".into(),
            ));
        }
        if setting == "bicdb.sql_profile" {
            return Err(SqlError::Unsupported(
                "bicdb.sql_profile is fixed at database open and connection startup".to_string(),
            ));
        }
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
            | "default_transaction_isolation"
            | "extra_float_digits"
            | "bytea_output"
            | "standard_conforming_strings"
            | "intervalstyle"
            | "xmloption"
            | "xmlbinary"
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
            | "default_text_search_config"
            | "escape_string_warning"
            | "max_stack_depth"
            | "timezone" => {
                // A setting given at connection start resets to that value.
                let startup = self
                    .session_gucs
                    .get(&format!(
                        "{}{setting}",
                        crate::datestyle_output::STARTUP_DEFAULT_PREFIX
                    ))
                    .cloned();
                match startup {
                    Some(value) => {
                        Arc::make_mut(&mut self.session_gucs).insert(setting.clone(), value);
                    }
                    None => {
                        Arc::make_mut(&mut self.session_gucs).remove(&setting);
                    }
                }
                // Output and text search read these per thread.
                crate::set_extra_float_digits(crate::extra_float_digits_setting(
                    &self.session_gucs,
                ));
                crate::set_default_text_search_config(&crate::default_text_search_config_setting(
                    &self.session_gucs,
                ));
                Ok(())
            }
            other if planner_gucs::is_planner_guc(other) => {
                Arc::make_mut(&mut self.session_gucs).remove(other);
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
            "max_prepared_transactions" => Err(restart_only_setting(&setting)),
            "session_authorization" => self.apply_set_session_authorization(None),
            INITIAL_SESSION_AUTHORIZATION_GUC
            | RLS_CHECK_AS_GUC
            | SESSION_CLIENT_ADDRESS_GUC
            | SESSION_CLIENT_HOST_GUC => Err(SqlError::BicDb(BicDbError::Authorization(format!(
                "parameter \"{setting}\" cannot be changed"
            )))),
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
        let transport: Vec<_> = [SESSION_CLIENT_ADDRESS_GUC, SESSION_CLIENT_HOST_GUC]
            .into_iter()
            .filter_map(|key| {
                self.session_gucs
                    .get(key)
                    .cloned()
                    .map(|value| (key.to_string(), value))
            })
            .collect();
        let database = self.session_gucs.get("database").cloned();
        let initial_user = self
            .session_gucs
            .get(INITIAL_SESSION_AUTHORIZATION_GUC)
            .cloned();
        let postgres_version_banner = self.session_gucs.get(POSTGRES_VERSION_BANNER_GUC).cloned();
        let server_version = self.session_gucs.get("server_version").cloned();
        let server_version_num = self.session_gucs.get("server_version_num").cloned();
        let sql_profile = self.session_gucs.get("bicdb.sql_profile").cloned();
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
        if let Some(sql_profile) = sql_profile {
            Arc::make_mut(&mut self.session_gucs)
                .insert("bicdb.sql_profile".to_string(), sql_profile);
        }
        Arc::make_mut(&mut self.session_gucs).extend(transport);
        self.guc_transaction_start = None;
        self.transaction_timestamp_micros = None;
        self.guc_local_restore.clear();
        self.guc_local_settings_restore = SqlSettingsRestore::default();
        *self.sequence_values.borrow_mut() = SequenceSessionValues::default();
        self.sql_cursors.close_all();
        self.oracle_output.reset()?;
        self.rebind_trusted_security_settings();
        Ok(())
    }

    pub(crate) fn execute_raw_comment(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        if let Some(result) = self.execute_raw_event_trigger_ddl(sql)? {
            return Ok(Some(result));
        }
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
            if normalized.starts_with("comment on table ")
                || normalized.starts_with("comment on column ")
            {
                self.execute_relation_comment(&statement)?;
                continue;
            }
            if self.execute_foreign_data_comment(&statement)?.is_some() {
                continue;
            }
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

    /// `COMMENT ON TABLE t` / `COMMENT ON COLUMN t.c`: kept on the table's
    /// schema so `obj_description`, `col_description` and `pg_description`
    /// report it. An empty comment removes it, as in PostgreSQL.
    fn execute_relation_comment(&mut self, statement: &str) -> Result<()> {
        let parsed = parse_statements(statement)?;
        let [Statement::Comment {
            object_type,
            object_name,
            comment,
            if_exists,
        }] = parsed.as_slice()
        else {
            return Err(SqlError::Unsupported(format!(
                "unsupported COMMENT statement: {statement}"
            )));
        };
        let (relation, column) = match object_type {
            CommentObject::Table => (object_name.clone(), None),
            CommentObject::Column => {
                let mut parts = object_name.0.clone();
                let column = parts.pop();
                if parts.is_empty() {
                    return Err(SqlError::InvalidSql(
                        "column name must be qualified".to_string(),
                    ));
                }
                let column = column
                    .map(|part| object_name_parts(&ObjectName(vec![part])).remove(0))
                    .ok_or_else(|| SqlError::InvalidSql("invalid column name".to_string()))?;
                (ObjectName(parts), Some(column))
            }
            _ => return Ok(()),
        };
        let display = object_name_parts(&relation).join(".");
        let mut table = relation_name(&relation)?;
        // An unqualified name resolves through search_path, as other DDL.
        if relation.0.len() == 1 {
            // The session's temporary schema first, as for any reference.
            if let Some(path) = crate::search_path::binding_path_with_temp(
                self.db_ref(),
                &self.session_gucs,
                self.temp_schema_name().as_deref(),
            )? {
                for namespace in path.iter() {
                    let mut parts = vec![sqlparser::ast::ObjectNamePart::Identifier(
                        Ident::with_quote('"', namespace.clone()),
                    )];
                    parts.extend(relation.0.iter().cloned());
                    let candidate = relation_name(&ObjectName(parts))?;
                    if load_schema(self.db_ref(), &candidate)?.is_some() {
                        table = candidate;
                        break;
                    }
                }
            }
        }
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let Some(mut schema) = load_schema(self.db_ref(), &table)? else {
            if *if_exists {
                return Ok(());
            }
            return Err(SqlError::data_exception_public(
                "42P01",
                format!("relation \"{display}\" does not exist"),
                None,
            ));
        };
        self.require_table_ownership(&table, "COMMENT")?;
        let comment = comment.clone().filter(|comment| !comment.is_empty());
        match column {
            None => schema.comment = comment,
            Some(column) => {
                let Some(target) = schema
                    .columns
                    .iter_mut()
                    .find(|candidate| !candidate.hidden && candidate.name == column)
                else {
                    return Err(SqlError::data_exception_public(
                        "42703",
                        format!("column \"{column}\" of relation \"{display}\" does not exist"),
                        None,
                    ));
                };
                target.comment = comment;
            }
        }
        self.save_session_schema(&schema)
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
                if !is_known_schema(self.db_ref(), &schema_name)?
                    && load_namespace(self.db_ref(), &schema_name)?.is_none()
                {
                    return Err(SqlError::InvalidCollection(schema_name));
                }
                // Moving an object into a schema creates it there.
                self.require_schema_create_privilege(&schema_name)?;
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
            let found = load_user_type(self.db_ref(), &identity.schema_name, &identity.name)?;
            let Some(mut user_type) = found else {
                // `_name` is the array type of a user type: PostgreSQL refuses
                // to set its privileges separately from the element type.
                if let Some(element) = identity.name.strip_prefix('_') {
                    if load_user_type(self.db_ref(), &identity.schema_name, element)?.is_some() {
                        return Err(SqlError::RaisedException {
                            sqlstate: "0LP01".into(),
                            message: "cannot set privileges of array types".into(),
                            detail: None,
                        });
                    }
                }
                return Err(SqlError::undefined_type(format!(
                    "{}.{}",
                    identity.schema_name, identity.name
                )));
            };
            if ddl.domain && !matches!(user_type.kind, UserTypeKind::Domain { .. }) {
                return Err(SqlError::data_exception(
                    "42809",
                    format!("\"{}\" is not a domain", user_type.name),
                    Some(user_type.name.clone()),
                ));
            }
            // Granting needs a grant option, which only the owner holds here;
            // PostgreSQL reports a role without one as having no privilege.
            if self.ensure_user_type_owner(&user_type).is_err() {
                return Err(type_usage_denied(&user_type));
            }
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
        Err(type_usage_denied(&user_type))
    }
}

/// PostgreSQL's aclcheck_error_type text: the type as format_type_be prints
/// it, schema-qualified only when not on the default search path.
pub(crate) fn type_usage_denied(user_type: &UserTypeSchema) -> SqlError {
    let name = if matches!(user_type.schema_name.as_str(), "public" | "pg_catalog") {
        pg_partition_key_identifier(&user_type.name)
    } else {
        format!(
            "{}.{}",
            pg_partition_key_identifier(&user_type.schema_name),
            pg_partition_key_identifier(&user_type.name)
        )
    };
    SqlError::BicDb(BicDbError::Authorization(format!(
        "permission denied for type {name}"
    )))
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

/// A PL/pgSQL function evaluated once per row of an engine-evaluated statement
/// reads under the caller's transaction but cannot write through it. Raised
/// before any write happens, as `Unsupported` so callers that can evaluate the
/// call in the session itself (a row-independent UPDATE assignment) still do.
pub(crate) fn row_routine_write_error() -> SqlError {
    SqlError::Unsupported(
        "a PL/pgSQL function called once per row of a query or UPDATE can only read data; \
         data-modifying statements in such a function are not supported"
            .to_string(),
    )
}

/// A query without data-modifying CTEs or SELECT INTO.
pub(crate) fn statement_is_plain_query(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    fn plain(query: &Query) -> bool {
        let ctes_plain = query
            .with
            .as_ref()
            .is_none_or(|with| with.cte_tables.iter().all(|cte| plain(&cte.query)));
        ctes_plain && body_plain(query.body.as_ref())
    }
    fn body_plain(body: &SetExpr) -> bool {
        match body {
            SetExpr::Select(select) => select.into.is_none(),
            SetExpr::Query(query) => plain(query),
            SetExpr::SetOperation { left, right, .. } => body_plain(left) && body_plain(right),
            SetExpr::Values(_) | SetExpr::Table(_) => true,
            _ => false,
        }
    }
    plain(query)
}

/// PostgreSQL's error for a postmaster-context setting such as
/// `max_prepared_transactions`.
pub(crate) fn restart_only_setting(setting: &str) -> SqlError {
    SqlError::data_exception(
        "55P02",
        format!("parameter \"{setting}\" cannot be changed without restarting the server"),
        None,
    )
}

/// PostgreSQL's `limit_printout_length`: COPY CONTEXT shows at most 100
/// bytes of a value or line, then `...`.
pub fn copy_context_printout(value: &str) -> String {
    const MAX: usize = 100;
    if value.len() <= MAX {
        return value.to_string();
    }
    let mut end = MAX;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
}

/// `error` (a column type refusing a COPY value) with PostgreSQL's CONTEXT.
fn copy_value_error(
    error: SqlError,
    relation: &str,
    line: u64,
    column: &str,
    value: &str,
) -> SqlError {
    SqlError::with_detail_fields(
        error,
        None,
        None,
        Some(format!(
            "COPY {relation}, line {line}, column {column}: \"{}\"",
            copy_context_printout(value)
        )),
    )
}

/// A COPY row whose conversion to a record failed: find the value its
/// column's input function refuses and report that error as PostgreSQL
/// does (its own message, with `COPY t, line N, column c: "value"`). Other
/// failures are returned unchanged.
fn copy_row_conversion_error(
    error: SqlError,
    relation: &str,
    line: u64,
    schema: Option<&TableSchema>,
    columns: &[String],
    row: &[Option<String>],
) -> SqlError {
    let Some(schema) = schema else {
        return error;
    };
    for (column, value) in columns.iter().zip(row) {
        let (Some(value), Some(target)) = (value, schema.column(column)) else {
            continue;
        };
        if let Err(refused) = cast_value_to_column_type(SqlValue::String(value.clone()), target) {
            // The record path wraps a bare input error in "column ... expects
            // type ..."; PostgreSQL reports the input function's own error.
            let base = match (&error, refused) {
                (
                    SqlError::TypeMismatch { .. },
                    refused @ (SqlError::ConstraintViolation { .. }
                    | SqlError::DataException { .. }),
                ) => refused,
                (SqlError::TypeMismatch { .. }, refused) => {
                    SqlError::coded("22P02", refused.to_string(), None, None)
                }
                _ => error,
            };
            return copy_value_error(base, relation, line, &target.name, value);
        }
    }
    error
}

/// COPY FROM's statement rules (see `SqlSession::copy_insert_rows_with_rules`).
#[derive(Clone, Debug, Default)]
pub struct CopyFromRules {
    /// ON_ERROR IGNORE: skip rows a column type's input refuses.
    pub on_error_ignore: bool,
    /// LOG_VERBOSITY VERBOSE: one NOTICE per skipped row.
    pub verbose: bool,
    /// REJECT_LIMIT, counted over the whole COPY.
    pub reject_limit: Option<u64>,
    /// Rows earlier batches of the same COPY skipped.
    pub skipped_before: u64,
    /// `WHERE <condition>` over the input row.
    pub where_clause: Option<String>,
}

/// What one batch of COPY FROM rows did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CopyFromOutcome {
    pub inserted: usize,
    pub skipped: u64,
}

/// See `SqlSession::copy_relation_info`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyRelationInfo {
    pub physical_name: String,
    pub partitioned: bool,
    pub has_default: Vec<bool>,
}

/// An error ON_ERROR IGNORE skips: a type's input function refusing a value
/// (PostgreSQL's soft errors), including a domain's constraints. Others
/// (missing defaults, internal failures) still fail the COPY.
fn is_soft_copy_input_error(error: &SqlError) -> bool {
    let sqlstate = error.sqlstate();
    sqlstate.starts_with("22") || matches!(sqlstate, "23502" | "23514")
}

fn copy_input_requires_special_type(target: &ColumnSchema) -> bool {
    target.user_type.is_none()
        && (matches!(target.pg_type.as_str(), "int2" | "int4" | "int8")
            || (target.pg_type == "xml" && crate::xml::xml_input_is_document()))
}

fn copy_text_input(text: &str, target: &ColumnSchema) -> Result<SqlValue> {
    if copy_input_requires_special_type(target) {
        if target.pg_type == "xml" {
            return crate::xml::xml_input_value(SqlValue::String(text.to_owned()));
        }
        return pg_integer_input(text, &target.pg_type).map(SqlValue::Int);
    }
    Ok(SqlValue::String(text.to_owned()))
}

/// The first field of a skipped row its column's input refuses, for the
/// verbose NOTICE: (column, the value or `None` for NULL input).
fn copy_refused_field(
    schema: Option<&TableSchema>,
    columns: &[String],
    row: &[Option<String>],
) -> (String, Option<String>) {
    if let Some(schema) = schema {
        for (column, value) in columns.iter().zip(row) {
            let Some(target) = schema.column(column) else {
                continue;
            };
            let input = match value {
                Some(text) => copy_text_input(text, target),
                None => Ok(SqlValue::Null),
            };
            let refused = match input {
                Err(_) => true,
                Ok(value) => cast_value_to_column_type(value, target).is_err(),
            };
            if refused {
                return (target.name.clone(), value.clone());
            }
        }
    }
    (
        columns.first().cloned().unwrap_or_default(),
        row.first().cloned().flatten(),
    )
}
