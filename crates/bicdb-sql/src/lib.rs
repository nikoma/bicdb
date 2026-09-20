use rustc_hash::FxHashMap;
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bicdb_core::{
    analyze_sample_limit, cosine_similarity, dot_product, estimate_distinct_from_sample,
    l2_distance, scale_sample_count, BicDb, BicDbError, BrokerCaller, CancellationToken, Geometry,
    IndexDefinition, IndexField, IndexKind, IndexValue, MemoryIndexMode, MutationGrantId,
    NetworkStatistics, RangeStatistics, Record, RecordSystemMetadata, RepairDelta, RepairPlan,
    RowId, SecurityContext, TableStatistics, Transaction, TypedColumnStatistics,
    TypedValueFrequency, ValueFrequency, VectorMetric,
};
use geo::{
    BooleanOps, BoundingRect, Centroid, Contains, ConvexHull, Destination, Distance, GeodesicArea,
    Geometry as GeoGeometry, Haversine, Intersects, Length, Relate, Simplify,
};
use md5::Md5;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlparser::ast::{
    visit_expressions_mut, AccessExpr, Action, AlterColumnOperation, AlterFunction,
    AlterFunctionAction, AlterFunctionKind, AlterFunctionOperation, AlterPolicy,
    AlterPolicyOperation, AlterRoleOperation, AlterTable, AlterTableOperation, ArrayElemTypeDef,
    Assignment, AssignmentTarget, BinaryOperator, CascadeOption, CharacterLength, ColumnDef,
    ColumnOption, CommentObject, ConditionalStatements, ConflictTarget, ConstraintCharacteristics,
    CreateExtension, CreateFunction, CreateFunctionBody, CreateIndex, CreatePolicy,
    CreatePolicyCommand, CreatePolicyType, CreateRole, CreateTable, CreateTableLike,
    CreateTableLikeDefaults, CreateTableLikeKind, CreateTableOptions, CreateTrigger, CreateView,
    DataType, DateTimeField, Delete, DiscardObject, DropFunction, DropPolicy, DropTrigger,
    DuplicateTreatment, ExactNumberInfo, Expr, FromTable, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, FunctionDesc, FunctionReturnType, FunctionSecurity, GeneratedAs, Grant,
    GrantObjects, Grantee, GranteeName, GranteesType, GroupByExpr, Ident, IndexColumn, IndexType,
    Insert, Interval, Join, JoinConstraint, JoinOperator, LimitClause, Lock, NamedWindowExpr,
    NullTreatment, ObjectName, ObjectType, OnConflictAction, OnInsert, OrderBy, OrderByKind,
    OrderByOptions, Owner, Privileges, ProcedureParam, Query, ReferentialAction,
    RenameTableNameKind, Revoke, RoleOption, SchemaName, Select, SelectFlavor, SelectItem,
    SelectItemQualifiedWildcardKind, SequenceOptions, Set, SetExpr, SetOperator, SetQuantifier,
    SetSessionAuthorizationParamKind, SqlOption, Statement, Subscript, TableAlias,
    TableAliasColumnDef, TableConstraint, TableFactor, TableFunctionArgs, TableObject,
    TableWithJoins, TimezoneInfo, TransactionAccessMode, TransactionIsolationLevel,
    TransactionMode, TriggerEvent, TriggerObject, TriggerObjectKind, TriggerPeriod, Truncate,
    TruncateIdentityOption, TruncateTableTarget, TypedString, UnaryOperator, UpdateTableFromKind,
    UserDefinedTypeRepresentation, UserDefinedTypeSqlDefinitionOption, Value, ValueWithSpan,
    WindowFrameBound, WindowFrameUnits, WindowSpec, WindowType, With,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, SqlError>;

mod identity;
pub use identity::{
    bicdb_version_banner, postgres_version_banner, BICDB_VERSION, POSTGRES_COMPATIBILITY_VERSION,
    POSTGRES_COMPATIBILITY_VERSION_NUM,
};
pub(crate) use identity::{postgres_compatibility_version_from_gucs, sql_version_banner};

/// BicDB application's generated PostgreSQL backend ranks hybrid-search text with an
/// ordered, A/B/C/D-weighted English document, `websearch_to_tsquery`, and
/// `ts_rank_cd`. The embedded application host uses this exact implementation
/// so zero-hop lowering does not substitute BM25 or a tokenizer approximation.
pub fn application_websearch_rank_cd_english(fields: &[String], query: &str) -> f32 {
    fts::application_websearch_rank_cd_english(fields, query)
}

/// Normalize an ordinary English web-search conjunction into the exact
/// lexemes used by `websearch_to_tsquery('english', ...)`. Returns `None` for
/// single-term, OR, NOT, phrase, prefix, or empty shapes so callers can retain
/// their compatibility path instead of silently changing query semantics.
pub fn english_websearch_positive_and_terms(query: &str) -> Option<Vec<String>> {
    fts::english_websearch_positive_and_terms(query)
}

/// Adaptive execution engine scaffold (signature / hotness / verification).
/// Observe-only and gated behind `BICDB_AEE`; see [`adaptive`] for details.
pub mod adaptive;
mod fts;
mod geometric;
mod pg_casts;
mod pg_opclasses;
mod trigram;
use trigram::*;
pub mod pg_types;
pub mod typed_value;
pub(crate) use fts::{
    eval_fts_db_function_value, eval_fts_function_value, fts_function_pg_type, fts_index_terms,
    FtsIndexCandidate,
};
/// TEST SHIM: the full `ts_rank` path (sparse tsvector + query walk) for a
/// single-term query over packed positions — exists solely so tests can pin
/// `bicdb_core::fts_rank_single_term` against it bit-for-bit.
pub fn ts_rank_with_scalars_for_tests(term: &str, packed: &[u16], weights: [f32; 4]) -> f32 {
    let query = fts::PgTsQuery {
        root: Some(fts::PgTsQueryNode::Operand(fts::PgTsQueryOperand {
            text: term.to_string(),
            weights: 0,
            prefix: false,
        })),
    };
    let sparse = fts::PgTsVector {
        lexemes: vec![fts::PgTsLexeme {
            text: term.to_string(),
            positions: packed.iter().map(|p| fts::unpack_ts_position(*p)).collect(),
        }],
    };
    fts::ts_rank_with_scalars(&sparse, &query, weights, 0, None)
}

/// TEST SHIM: full `ts_rank` for an AND-of-terms query over packed position
/// lists (`None` = term absent) — pins `bicdb_core::fts_rank_conjunctive`.
pub fn ts_rank_conjunctive_for_tests(terms: &[(&str, Option<&[u16]>)], weights: [f32; 4]) -> f32 {
    let mut root: Option<fts::PgTsQueryNode> = None;
    for (term, _) in terms {
        let operand = fts::PgTsQueryNode::Operand(fts::PgTsQueryOperand {
            text: (*term).to_string(),
            weights: 0,
            prefix: false,
        });
        root = Some(match root {
            None => operand,
            Some(left) => fts::PgTsQueryNode::And(Box::new(left), Box::new(operand)),
        });
    }
    let query = fts::PgTsQuery { root };
    let sparse = fts::PgTsVector {
        lexemes: terms
            .iter()
            .filter_map(|(term, positions)| {
                positions.map(|positions| fts::PgTsLexeme {
                    text: (*term).to_string(),
                    positions: positions
                        .iter()
                        .map(|p| fts::unpack_ts_position(*p))
                        .collect(),
                })
            })
            .collect(),
    };
    fts::ts_rank_with_scalars(&sparse, &query, weights, 0, None)
}

pub use fts::{
    PgTsLexeme, PgTsPosition, PgTsQuery, PgTsQueryNode, PgTsQueryOperand, PgTsVector, PgTsWeight,
};
pub(crate) use geometric::*;
pub(crate) use pg_casts::*;
pub(crate) use pg_opclasses::*;
pub use pg_types::{
    pg_array_element_oid, pg_array_element_spec_by_oid, pg_format_type, pg_internal_codec_type,
    pg_range_statistics_bounds_type, pg_type_delimiter, pg_type_delimiter_by_oid,
    pg_type_name_by_oid, pg_type_oid_by_name, pg_type_spec, pg_type_spec_by_oid, PgBinaryCodec,
    PgInternalCodecDirection, PgTextCodec, PgTypeRegistry, PgTypeSpec, PG_TYPE_REGISTRY,
    PG_TYPE_SPECS,
};
pub use typed_value::*;
mod type_codec;
pub(crate) use type_codec::{
    column_typed_compare, column_typed_index_key, column_typed_index_label,
    column_typed_not_distinct, column_typed_storage_key, pg_typed_compare_for_db,
    pg_typed_index_key_for_canonical, pg_typed_index_key_for_db, pg_typed_index_label,
    pg_typed_index_label_for_db,
};
pub use type_codec::{
    pg_scalar_codec, pg_typed_compare, pg_typed_hash_key, pg_typed_index_key,
    pg_typed_not_distinct, PgCanonicalTextCodec, PgCodecContext, PgScalarCodec,
};

// Phase-1 mechanical module split: each module below was extracted verbatim
// from this file. `pub use` glob re-exports keep every existing path resolving.
mod session;
pub use session::*;
mod runtime;
pub use runtime::*;
mod schema_meta;
mod statement_cache;
pub use schema_meta::*;
mod catalog_memo;
mod routines;
pub(crate) use routines::*;
mod ddl_alter;
pub use ddl_alter::*;
mod mvt;
mod records;
pub use records::*;
mod engine;
mod external_sort;
pub use engine::*;
pub mod extension_catalog;
pub use extension_catalog::*;
#[cfg(feature = "extension-host")]
pub mod extension_runtime;
#[cfg(feature = "extension-host")]
pub use extension_runtime::*;
mod planner;
pub(crate) use planner::*;
mod pg_dump_compat;
pub(crate) use pg_dump_compat::*;
mod select_exec;
pub(crate) use select_exec::*;
mod catalog_rows;
pub(crate) use catalog_rows::*;
mod virtual_tables;
pub(crate) use virtual_tables::*;
mod broker_fns;
mod eval;
mod pg_hash;
mod routine_outcome;
pub(crate) use broker_fns::{broker_function_pg_type, eval_broker_function_value};
pub(crate) use eval::*;
pub use eval::{
    oid_alias_numeric_value, oid_alias_type_from_oid, render_oid_alias_array_value,
    render_oid_alias_value,
};
pub use routine_outcome::{
    routine_outcome_counters_snapshot, routine_outcome_trace_enabled, RoutineOutcomeCounters,
};
mod jsonb;
pub(crate) use jsonb::{
    composite_to_json, eval_json_function_call_value, json_function_call_pg_type,
    json_object_aggregate_key, json_text_array_path, jsonb_index_key_token, jsonb_index_terms,
};
pub use jsonb::{postgres_jsonb_pretty_text, postgres_jsonb_text};
mod jsonpath;
pub(crate) use jsonpath::{
    eval_jsonpath_function_value, eval_jsonpath_operator_value, eval_jsonpath_query_values,
    jsonpath_function_pg_type, jsonpath_index_terms, normalize_jsonpath,
};
mod xml;
pub(crate) use xml::{
    eval_xml_function_value, finish_xml_agg, normalize_xml, xml_function_pg_type,
};

/// Bound-plan cache gate (on by default). Enables a fast path that memoizes the
/// bound single-table primary-key point-lookup plan (`SELECT cols FROM t WHERE
/// pk = <expr>`), skipping per-call planning. Modeled exactly on
/// [`adaptive::enabled`]: a single relaxed atomic read after a one-time
/// `BICDB_PLAN_CACHE` env probe. When disabled (`BICDB_PLAN_CACHE=0`) the path is
/// never entered and behavior is byte-identical to the fused execution.
/// Simple UPDATEs of resident rows splice the assigned columns into the old
/// row's JSON text and write the row in its stored form, never parsing it
/// (typed resident rows phase 3). `BICDB_STORED_UPDATE=0` disables it for A/B.
pub mod stored_update {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_STORED_UPDATE")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

pub mod plan_cache {
    use std::sync::atomic::{AtomicU8, Ordering};

    const STATE_UNINIT: u8 = 0;
    const STATE_OFF: u8 = 1;
    const STATE_ON: u8 = 2;

    static ENABLED: AtomicU8 = AtomicU8::new(STATE_UNINIT);

    #[cfg(test)]
    thread_local! {
        // Per-thread override so in-process A/B tests can flip the gate without
        // racing the process-global atomic against other parallel tests.
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }

    /// Whether the bound-plan cache fast path is active. Reads `BICDB_PLAN_CACHE`
    /// once, then is a single relaxed atomic load. When `false` the caller must
    /// leave the original execution path untouched.
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            STATE_ON => true,
            STATE_OFF => false,
            _ => {
                // On by default: the fast path reproduces the fused
                // execution exactly (see the differential tests) and measured
                // +2.3% on TPC-C, whose reads are almost all full-primary-key
                // point lookups. `BICDB_PLAN_CACHE=0` turns it off.
                let on = std::env::var("BICDB_PLAN_CACHE")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { STATE_ON } else { STATE_OFF }, Ordering::Relaxed);
                on
            }
        }
    }

    /// Force the gate on/off for the current thread only (tests/benches).
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
}

/// `BICDB_IR_PLAN_CACHE` env probe for the routine-IR-keyed point-lookup
/// SELECT plans. Off unless set to `1`/`on`/`true`/`yes`: measured slower
/// than the fused single-table path on TPC-C (Azure A/B −2.6%/−3.5%).
pub mod ir_plan_cache {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    /// Force the gate on/off for the current thread only (tests).
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_IR_PLAN_CACHE")
                    .map(|v| matches!(v.as_str(), "1" | "on" | "true" | "yes"))
                    .unwrap_or(false);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

/// `BICDB_IR_UPDATE_PLAN` env probe for the routine-IR-keyed UPDATE plans and
/// bound assignments (on unless set to `0`/`off`/`false`/`no`).
/// Routine-owned `INSERT ... VALUES` statements build their rows through the
/// per-IR-node insert template (`InsertValuesTemplate`). `BICDB_IR_INSERT_PLAN=0`
/// disables it for A/B and diagnosis.
/// The WHERE re-check over a materialized row set is bound once per statement
/// (`BoundExprScope::bind`) and evaluated per row through the bound
/// evaluator, the one the IR point lookups already filter with; expressions
/// the binder declines take the typed per-row evaluator as before.
/// `BICDB_BOUND_ROW_FILTER=0` disables it for A/B.
pub mod bound_row_filter {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_BOUND_ROW_FILTER")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

/// Routine-owned two-relation SELECTs whose WHERE/ON locates both rows by full
/// primary-key equalities run through the per-IR-node point-join template
/// (`PointJoinTemplate`). `BICDB_IR_JOIN_PLAN=0` disables it for A/B.
pub mod ir_join_plan {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_IR_JOIN_PLAN")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

pub mod ir_insert_plan {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_IR_INSERT_PLAN")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

/// A stored PL/pgSQL function whose body is exactly `RETURN <expr>` (with
/// only `ALIAS FOR $n` declarations) evaluates that bound expression straight
/// from its argument values — no call frame, no block interpreter. HammerDB's
/// `DBMS_RANDOM` has this shape and NEW_ORDER calls it ~40 times per order.
/// `BICDB_INLINE_RETURN_FNS=0` disables it for A/B.
pub mod inline_return_fns {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(debug_assertions)]
    static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    /// Calls served without a frame (debug builds only; tests pin the path).
    #[cfg(debug_assertions)]
    pub fn hits() -> u64 {
        HITS.load(Ordering::Relaxed)
    }
    #[inline]
    pub(crate) fn record_hit() {
        #[cfg(debug_assertions)]
        HITS.fetch_add(1, Ordering::Relaxed);
    }
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_INLINE_RETURN_FNS")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

pub mod ir_update_plan {
    use std::sync::atomic::{AtomicU8, Ordering};
    static ENABLED: AtomicU8 = AtomicU8::new(0);
    #[cfg(test)]
    thread_local! {
        static TEST_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    }
    #[cfg(test)]
    pub(crate) fn set_test_override(value: Option<bool>) {
        TEST_OVERRIDE.with(|cell| cell.set(value));
    }
    #[inline]
    pub fn enabled() -> bool {
        #[cfg(test)]
        {
            if let Some(value) = TEST_OVERRIDE.with(|cell| cell.get()) {
                return value;
            }
        }
        match ENABLED.load(Ordering::Relaxed) {
            2 => true,
            1 => false,
            _ => {
                let on = std::env::var("BICDB_IR_UPDATE_PLAN")
                    .map(|v| !matches!(v.as_str(), "0" | "off" | "false" | "no"))
                    .unwrap_or(true);
                ENABLED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
                on
            }
        }
    }
}

// Cached schemas are shared: a hit used to deep-clone the whole TableSchema
// (every column String), and hot paths ask for the same schema per row.
/// Per-db-instance table-schema cache: outer key is the `BicDb` address,
/// inner key the table name, so lookups borrow the name instead of allocating
/// a `(usize, String)` key per call.
type SqlSchemaCacheMap = FxHashMap<usize, FxHashMap<String, Option<Arc<TableSchema>>>>;
type SqlSchemaListCacheMap = BTreeMap<usize, Arc<Vec<TableSchema>>>;
type SqlIndexDefinitionsCacheMap = FxHashMap<usize, FxHashMap<String, Vec<IndexDefinition>>>;

const SYNTHETIC_PRIMARY_KEY: &str = "__bicdb_rowid";
const PG_PROC_CATALOG_OID: i64 = 1255;
const PG_CLASS_CATALOG_OID: i64 = 1259;
const PG_TYPE_CATALOG_OID: i64 = 1247;
const PG_CAST_CATALOG_OID: i64 = 2605;
const PG_NAMESPACE_CATALOG_OID: i64 = 2615;
const PG_CONSTRAINT_CATALOG_OID: i64 = 2606;
const PG_LANGUAGE_CATALOG_OID: i64 = 2612;
const PG_EXTENSION_CATALOG_OID: i64 = 3079;
const PLPGSQL_EXTENSION_OID: i64 = 14024;
const PLPGSQL_CALL_HANDLER_OID: i64 = 14025;
const PLPGSQL_INLINE_HANDLER_OID: i64 = 14026;
const PLPGSQL_VALIDATOR_OID: i64 = 14027;
const PLPGSQL_LANGUAGE_OID: i64 = 14028;
static SYNTHETIC_ROW_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static SQL_RANDOM_STATE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Debug, Default)]
struct SqlProfileStats {
    schema_loads: usize,
    schema_load_bytes: usize,
    schema_saves: usize,
    bytes_serialized: usize,
    rows_materialized: usize,
    join_intermediate_rows: usize,
    join_candidate_pairs: usize,
    join_steps: usize,
    peak_memory_bytes: usize,
    index_lookup_count: usize,
    index_catalog_entries_considered: usize,
    join_predicate_row_merges: usize,
    planner_record_count_scans: usize,
    record_id_prefix_scans: usize,
    scalar_subqueries: usize,
    scalar_subquery_elapsed_ms: f64,
    foreign_key_parent_delete_schema_scans: usize,
    foreign_key_child_scans: usize,
    write_elapsed_ms: f64,
    write_batches: usize,
    write_rows: usize,
    full_scan_count: usize,
}

thread_local! {
    static SQL_PROFILE_STACK: RefCell<Vec<SqlProfileStats>> = const { RefCell::new(Vec::new()) };
    static SQL_SCHEMA_CACHE_STACK: RefCell<Vec<SqlSchemaCacheMap>> = const { RefCell::new(Vec::new()) };
    static SQL_SCHEMA_LIST_CACHE_STACK: RefCell<Vec<SqlSchemaListCacheMap>> = const { RefCell::new(Vec::new()) };
    static SQL_INDEX_DEFINITIONS_CACHE_STACK: RefCell<Vec<SqlIndexDefinitionsCacheMap>> = const { RefCell::new(Vec::new()) };
}

static ROUTINE_EXCEPTION_40001: AtomicU64 = AtomicU64::new(0);
static ROUTINE_EXCEPTION_40P01: AtomicU64 = AtomicU64::new(0);
static ROUTINE_EXCEPTION_P0002: AtomicU64 = AtomicU64::new(0);
static ROUTINE_EXCEPTION_OTHER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoutineExceptionCounts {
    pub serialization_failure: u64,
    pub deadlock_detected: u64,
    pub no_data_found: u64,
    pub other: u64,
}

fn plpgsql_into_no_data_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_PLPGSQL_INTO_NO_DATA")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    })
}

pub fn routine_exception_counts_snapshot() -> RoutineExceptionCounts {
    RoutineExceptionCounts {
        serialization_failure: ROUTINE_EXCEPTION_40001.load(std::sync::atomic::Ordering::Relaxed),
        deadlock_detected: ROUTINE_EXCEPTION_40P01.load(std::sync::atomic::Ordering::Relaxed),
        no_data_found: ROUTINE_EXCEPTION_P0002.load(std::sync::atomic::Ordering::Relaxed),
        other: ROUTINE_EXCEPTION_OTHER.load(std::sync::atomic::Ordering::Relaxed),
    }
}

fn routine_exception_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BICDB_PROC_MIX_TRACE")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn record_handled_routine_exception(sqlstate: &str) {
    if !routine_exception_trace_enabled() {
        return;
    }
    match sqlstate {
        "40001" => &ROUTINE_EXCEPTION_40001,
        "40P01" => &ROUTINE_EXCEPTION_40P01,
        "P0002" => &ROUTINE_EXCEPTION_P0002,
        _ => &ROUTINE_EXCEPTION_OTHER,
    }
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
thread_local! {
    static SQL_SCHEMA_LIST_RAW_LOADS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_ROW_LOOKUP_COMPOUND_JOINS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_PARSE_STATEMENT_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_ROW_FROM_RECORD_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_MERGE_ROWS_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_SLOT_ROW_TO_MAP_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_INDEX_STORAGE_LOOKUP_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_RECORD_STORAGE_GET_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_RECORDS_FOR_IDS_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_CELL_ROWS_FAST: RefCell<usize> = const { RefCell::new(0) };
    /// Fields converted by `slot_rows_from_visible` (sum over rows): proves
    /// projection pushdown narrowed the rows a join converts.
    static SQL_SLOT_ROW_FIELDS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_CELL_ROWS_FALLBACK: RefCell<usize> = const { RefCell::new(0) };
    static SQL_INDEXED_SELECTION_PLAN_CALLS: RefCell<usize> = const { RefCell::new(0) };
    /// Locator strategies derived (the WHERE-walk the memo saves).
    static SQL_LOCATOR_STRATEGY_DERIVATIONS: RefCell<usize> = const { RefCell::new(0) };
    /// DELETEs served by the stored-form point path.
    static SQL_STORED_DELETE_HITS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_JOIN_CONSTRAINT_EVAL_CALLS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_JOIN_CONSTRAINT_BOUND_ONCE: RefCell<usize> = const { RefCell::new(0) };
    static SQL_JOIN_CONSTRAINT_PAIR_FALLBACKS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_PENDING_ID_MERGES: RefCell<usize> = const { RefCell::new(0) };
    static SQL_ROUTINE_FRAME_MAP_SYNCS: RefCell<usize> = const { RefCell::new(0) };
    static SQL_ROUTINE_SLOT_ARRAY_CLONES: RefCell<usize> = const { RefCell::new(0) };
    /// `bound_row_context` builds that snapshot the string-keyed variable
    /// map (the form a compiled routine's statements no longer take).
    static SQL_ROUTINE_VAR_SNAPSHOTS: RefCell<usize> = const { RefCell::new(0) };
}

struct SqlProfileScope {
    active: bool,
    started: Option<Instant>,
    sql_hash: u64,
    sql_len: usize,
    statement_kind: String,
}

struct SqlSchemaCacheScope {
    active: bool,
}

struct SqlSchemaSessionCacheScope {
    active: bool,
}

impl SqlSchemaCacheScope {
    fn new() -> Self {
        let active = SQL_SCHEMA_CACHE_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.is_empty() {
                stack.push(SqlSchemaCacheMap::default());
                SQL_SCHEMA_LIST_CACHE_STACK.with(|list_stack| {
                    list_stack.borrow_mut().push(BTreeMap::new());
                });
                SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|index_stack| {
                    index_stack
                        .borrow_mut()
                        .push(SqlIndexDefinitionsCacheMap::default());
                });
                true
            } else {
                false
            }
        });
        Self { active }
    }
}

impl SqlSchemaSessionCacheScope {
    fn new(
        schema_generation: u64,
        cached_generation: &mut u64,
        schema_cache: &mut SqlSchemaCacheMap,
        schema_list_cache: &mut SqlSchemaListCacheMap,
    ) -> Self {
        if *cached_generation != schema_generation {
            schema_cache.clear();
            schema_list_cache.clear();
            *cached_generation = schema_generation;
        }

        let active = SQL_SCHEMA_CACHE_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.is_empty() {
                stack.push(std::mem::take(schema_cache));
                SQL_SCHEMA_LIST_CACHE_STACK.with(|list_stack| {
                    list_stack
                        .borrow_mut()
                        .push(std::mem::take(schema_list_cache));
                });
                SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|index_stack| {
                    index_stack
                        .borrow_mut()
                        .push(SqlIndexDefinitionsCacheMap::default());
                });
                true
            } else {
                false
            }
        });
        Self { active }
    }

    fn finish(
        &mut self,
        schema_generation: u64,
        cached_generation: &mut u64,
        schema_cache: &mut SqlSchemaCacheMap,
        schema_list_cache: &mut SqlSchemaListCacheMap,
    ) {
        if !self.active {
            return;
        }
        *schema_cache =
            SQL_SCHEMA_CACHE_STACK.with(|stack| stack.borrow_mut().pop().unwrap_or_default());
        *schema_list_cache =
            SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| stack.borrow_mut().pop().unwrap_or_default());
        SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
        *cached_generation = schema_generation;
        self.active = false;
    }
}

impl Drop for SqlSchemaCacheScope {
    fn drop(&mut self) {
        if self.active {
            SQL_SCHEMA_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
            SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
            SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
}

impl Drop for SqlSchemaSessionCacheScope {
    fn drop(&mut self) {
        if self.active {
            SQL_SCHEMA_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
            SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
            SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
}

impl SqlProfileScope {
    fn new(sql: &str) -> Self {
        if !sql_profile_enabled() {
            return Self::inactive();
        }
        Self::new_with_parts(
            sql_profile_statement_kind(sql),
            sql_profile_hash(sql),
            sql.len(),
        )
    }

    fn new_routine_statement(statement: &RoutineStmt) -> Self {
        if !sql_profile_enabled() {
            return Self::inactive();
        }
        let detail = routine_statement_profile_signature(statement);
        Self::new_with_parts(
            routine_statement_profile_kind(statement),
            sql_profile_hash(&detail),
            detail.len(),
        )
    }

    fn new_with_parts(statement_kind: String, sql_hash: u64, sql_len: usize) -> Self {
        let active = sql_profile_enabled();
        if active {
            SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        }
        Self {
            active,
            started: active.then(Instant::now),
            sql_hash,
            sql_len,
            statement_kind,
        }
    }

    fn inactive() -> Self {
        Self {
            active: false,
            started: None,
            sql_hash: 0,
            sql_len: 0,
            statement_kind: String::new(),
        }
    }

    fn finish(&mut self, result: &Result<SqlResult>) {
        if !self.active {
            return;
        }
        let (ok, result_rows, result_memory) = match result {
            Ok(result) => (
                true,
                Some(result.rows.len()),
                sql_result_memory_estimate(result),
            ),
            Err(_) => (false, None, 0),
        };
        self.finish_parts(ok, result_rows, result_memory);
    }

    fn finish_control(&mut self, result: &Result<RoutineControl>) {
        if !self.active {
            return;
        }
        self.finish_parts(result.is_ok(), None, 0);
    }

    fn finish_parts(&mut self, ok: bool, result_rows: Option<usize>, result_memory: usize) {
        let mut stats = SQL_PROFILE_STACK
            .with(|stack| stack.borrow_mut().pop())
            .unwrap_or_default();
        if result_memory > 0 {
            sql_profile_observe_memory(&mut stats, result_memory);
        }
        let elapsed_ms = self
            .started
            .expect("active SQL profile scope has a start time")
            .elapsed()
            .as_secs_f64()
            * 1000.0;
        let flags = sql_trace_flags();
        let mut fields = vec![
            "bicdb_trace".to_string(),
            format!("stmt={}", self.statement_kind),
            format!("sql_hash={:016x}", self.sql_hash),
            format!("sql_len={}", self.sql_len),
            format!("elapsed_ms={elapsed_ms:.3}"),
            format!("ok={ok}"),
        ];
        if let Some(result_rows) = result_rows {
            fields.push(format!("result_rows={result_rows}"));
        }
        if flags.schema {
            fields.extend([
                format!("schema_loads={}", stats.schema_loads),
                format!("schema_saves={}", stats.schema_saves),
                format!("schema_bytes_read={}", stats.schema_load_bytes),
                format!("schema_bytes_written={}", stats.bytes_serialized),
            ]);
        }
        if flags.plan {
            fields.extend([
                format!("rows_materialized={}", stats.rows_materialized),
                format!("join_intermediate_rows={}", stats.join_intermediate_rows),
                format!("join_candidate_pairs={}", stats.join_candidate_pairs),
                format!("join_steps={}", stats.join_steps),
                format!("peak_memory_bytes={}", stats.peak_memory_bytes),
                format!("index_lookups={}", stats.index_lookup_count),
                format!(
                    "index_catalog_entries_considered={}",
                    stats.index_catalog_entries_considered
                ),
                format!(
                    "join_predicate_row_merges={}",
                    stats.join_predicate_row_merges
                ),
                format!(
                    "planner_record_count_scans={}",
                    stats.planner_record_count_scans
                ),
                format!("record_id_prefix_scans={}", stats.record_id_prefix_scans),
                format!("scalar_subqueries={}", stats.scalar_subqueries),
                format!(
                    "scalar_subquery_elapsed_ms={:.3}",
                    stats.scalar_subquery_elapsed_ms
                ),
                format!(
                    "foreign_key_parent_delete_schema_scans={}",
                    stats.foreign_key_parent_delete_schema_scans
                ),
                format!("foreign_key_child_scans={}", stats.foreign_key_child_scans),
                format!("write_elapsed_ms={:.3}", stats.write_elapsed_ms),
                format!("write_batches={}", stats.write_batches),
                format!("write_rows={}", stats.write_rows),
                format!("full_scans={}", stats.full_scan_count),
            ]);
        }
        if flags.exec {
            fields.push(format!("timestamp={}", unix_now()));
        }
        eprintln!("{}", fields.join(" "));
    }
}

#[derive(Clone, Copy, Debug)]
struct SqlTraceFlags {
    exec: bool,
    schema: bool,
    plan: bool,
}

fn sql_profile_enabled() -> bool {
    let flags = sql_trace_flags();
    flags.exec || flags.schema || flags.plan
}

fn sql_trace_flags() -> SqlTraceFlags {
    // Trace flags come from environment variables fixed at process start. Reading
    // them via getenv on every statement/plan shows up as locked __findenv calls
    // in profiles, so resolve them once.
    static FLAGS: std::sync::OnceLock<SqlTraceFlags> = std::sync::OnceLock::new();
    *FLAGS.get_or_init(|| SqlTraceFlags {
        exec: env_flag_enabled("BICDB_TRACE_EXEC"),
        schema: env_flag_enabled("BICDB_TRACE_SCHEMA"),
        plan: env_flag_enabled("BICDB_TRACE_PLAN"),
    })
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            let value = value.trim();
            !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false)
}

fn sql_profile_hash(sql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

fn sql_profile_statement_kind(sql: &str) -> String {
    let tokens = sql
        .split_whitespace()
        .take(3)
        .map(|token| {
            token
                .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .to_ascii_uppercase()
        })
        .collect::<Vec<_>>();
    match tokens.as_slice() {
        [first, ..] if first.is_empty() => "EMPTY".to_string(),
        [first] => first.clone(),
        [first, second, ..] if first == "CREATE" && second == "UNIQUE" => {
            "CREATE_INDEX".to_string()
        }
        [first, second, ..] if first == "CREATE" && second == "INDEX" => "CREATE_INDEX".to_string(),
        [first, second, ..] if first == "ALTER" && second == "TABLE" => "ALTER_TABLE".to_string(),
        [first, second, ..] if first == "CREATE" => format!("CREATE_{second}"),
        [first, second, ..] if first == "DROP" => format!("DROP_{second}"),
        [first, second, ..] if first == "TRUNCATE" => format!("TRUNCATE_{second}"),
        [first, ..] => first.clone(),
        [] => "EMPTY".to_string(),
    }
}

fn routine_statement_profile_kind(statement: &RoutineStmt) -> String {
    match statement {
        RoutineStmt::ContinueLoop => "ROUTINE_CONTINUE".to_string(),
        RoutineStmt::Null => "ROUTINE_NULL".to_string(),
        RoutineStmt::Assignment { .. } => "ROUTINE_ASSIGNMENT".to_string(),
        RoutineStmt::SelectInto { .. } => "ROUTINE_SELECT_INTO".to_string(),
        RoutineStmt::Perform { .. } => "ROUTINE_PERFORM".to_string(),
        RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
            format!(
                "ROUTINE_SQL_{}",
                sql_profile_statement_kind_from_ast(statement)
            )
        }
        RoutineStmt::DynamicExecute(_) => "ROUTINE_DYNAMIC_EXECUTE".to_string(),
        RoutineStmt::If { .. } => "ROUTINE_IF".to_string(),
        RoutineStmt::ForLoop { .. } => "ROUTINE_FOR_LOOP".to_string(),
        RoutineStmt::ForeachLoop { .. } => "ROUTINE_FOREACH_LOOP".to_string(),
        RoutineStmt::QueryForLoop { .. } => "ROUTINE_QUERY_FOR_LOOP".to_string(),
        RoutineStmt::OpenCursor { .. } => "ROUTINE_OPEN_CURSOR".to_string(),
        RoutineStmt::FetchCursor { .. } => "ROUTINE_FETCH_CURSOR".to_string(),
        RoutineStmt::CloseCursor { .. } => "ROUTINE_CLOSE_CURSOR".to_string(),
        RoutineStmt::RaiseException { .. } => "ROUTINE_RAISE_EXCEPTION".to_string(),
        RoutineStmt::ReturnQuery(_) => "ROUTINE_RETURN_QUERY".to_string(),
        RoutineStmt::Return(_) => "ROUTINE_RETURN".to_string(),
    }
}

fn routine_statement_profile_signature(statement: &RoutineStmt) -> String {
    match statement {
        RoutineStmt::ContinueLoop => "CONTINUE".to_string(),
        RoutineStmt::Null => "NULL".to_string(),
        RoutineStmt::Assignment { target, expr } => {
            format!(
                "ASSIGN {} := {expr}",
                routine_assignment_target_signature(target)
            )
        }
        RoutineStmt::SelectInto {
            query,
            targets,
            strict,
        } => {
            format!(
                "SELECT_INTO {} INTO {}{}",
                query,
                if *strict { "STRICT " } else { "" },
                targets.join(",")
            )
        }
        RoutineStmt::Perform { query } => {
            format!("PERFORM {query}")
        }
        RoutineStmt::Sql(statement) => {
            format!("SQL {}", sql_statement_profile_signature(statement))
        }
        RoutineStmt::SqlInto { statement, targets } => {
            format!(
                "SQL_INTO {} INTO {}",
                sql_statement_profile_signature(statement),
                targets.join(",")
            )
        }
        RoutineStmt::DynamicExecute(expr) => format!("EXECUTE {expr}"),
        RoutineStmt::If { condition, .. } => format!("IF {condition}"),
        RoutineStmt::ForLoop {
            iterator,
            lower,
            upper,
            ..
        } => format!("FOR {iterator} IN {lower}..{upper}"),
        RoutineStmt::ForeachLoop {
            target,
            slice,
            array,
            ..
        } => format!("FOREACH {target} SLICE {slice} IN ARRAY {array}"),
        RoutineStmt::QueryForLoop { target, query, .. } => format!("FOR {target} IN {query}"),
        RoutineStmt::OpenCursor { name } => format!("OPEN {name}"),
        RoutineStmt::FetchCursor { name, targets } => {
            format!("FETCH {name} INTO {}", targets.join(","))
        }
        RoutineStmt::CloseCursor { name } => format!("CLOSE {name}"),
        RoutineStmt::RaiseException {
            message, sqlstate, ..
        } => {
            format!("RAISE EXCEPTION {message:?} USING ERRCODE = {sqlstate:?}")
        }
        RoutineStmt::ReturnQuery(query) => format!("RETURN QUERY {query}"),
        RoutineStmt::Return(expr) => expr
            .as_ref()
            .map(|expr| format!("RETURN {expr}"))
            .unwrap_or_else(|| "RETURN".to_string()),
    }
}

fn routine_assignment_target_signature(target: &RoutineAssignmentTarget) -> String {
    match target {
        RoutineAssignmentTarget::Variable(name) => name.clone(),
        RoutineAssignmentTarget::ArrayElement { array_name, index } => {
            format!("{array_name}[{index}]")
        }
    }
}

fn sql_statement_profile_signature(statement: &Statement) -> String {
    match statement {
        Statement::Query(query) => query.to_string(),
        Statement::Insert(insert) => insert.to_string(),
        Statement::Update(update) => update.to_string(),
        Statement::Delete(delete) => delete.to_string(),
        Statement::Truncate(truncate) => truncate.to_string(),
        other => sql_profile_statement_kind_from_ast(other),
    }
}

fn sql_profile_statement_kind_from_ast(statement: &Statement) -> String {
    match statement {
        Statement::Query(_) => "SELECT".to_string(),
        Statement::Insert(_) => "INSERT".to_string(),
        Statement::Update(_) => "UPDATE".to_string(),
        Statement::Delete(_) => "DELETE".to_string(),
        Statement::Truncate(_) => "TRUNCATE_TABLE".to_string(),
        Statement::CreateTable(_) => "CREATE_TABLE".to_string(),
        Statement::CreateView(_) => "CREATE_VIEW".to_string(),
        Statement::CreateSequence { .. } => "CREATE_SEQUENCE".to_string(),
        Statement::CreateDomain(_) => "CREATE_DOMAIN".to_string(),
        Statement::CreateType { .. } => "CREATE_TYPE".to_string(),
        Statement::CreateSchema { .. } => "CREATE_SCHEMA".to_string(),
        Statement::CreateExtension(_) => "CREATE_EXTENSION".to_string(),
        Statement::CreateIndex(_) => "CREATE_INDEX".to_string(),
        Statement::CreateFunction(_) => "CREATE_FUNCTION".to_string(),
        Statement::CreateProcedure { .. } => "CREATE_PROCEDURE".to_string(),
        Statement::CreateTrigger(_) => "CREATE_TRIGGER".to_string(),
        Statement::CreateRole(_) => "CREATE_ROLE".to_string(),
        Statement::Drop { object_type, .. } => format!("DROP_{object_type:?}").to_ascii_uppercase(),
        Statement::DropFunction(_) => "DROP_FUNCTION".to_string(),
        Statement::DropProcedure { .. } => "DROP_PROCEDURE".to_string(),
        Statement::DropTrigger(_) => "DROP_TRIGGER".to_string(),
        Statement::AlterTable(_) => "ALTER_TABLE".to_string(),
        Statement::AlterFunction(_) => "ALTER_FUNCTION".to_string(),
        Statement::StartTransaction { .. } => "START_TRANSACTION".to_string(),
        Statement::Commit { .. } => "COMMIT".to_string(),
        Statement::Rollback { .. } => "ROLLBACK".to_string(),
        Statement::Set(_) => "SET".to_string(),
        Statement::Discard { .. } => "DISCARD".to_string(),
        other => sql_profile_statement_kind(&other.to_string()),
    }
}

fn sql_profile_active() -> bool {
    SQL_PROFILE_STACK.with(|stack| !stack.borrow().is_empty())
}

fn sql_profile_record(update: impl Fn(&mut SqlProfileStats)) {
    SQL_PROFILE_STACK.with(|stack| {
        for stats in stack.borrow_mut().iter_mut() {
            update(stats);
        }
    });
}

fn sql_profile_observe_memory(stats: &mut SqlProfileStats, bytes: usize) {
    stats.peak_memory_bytes = stats.peak_memory_bytes.max(bytes);
}

fn sql_profile_schema_load(bytes: usize) {
    sql_profile_record(|stats| {
        stats.schema_loads += 1;
        stats.schema_load_bytes += bytes;
    });
}

fn sql_profile_schema_save(bytes: usize) {
    sql_profile_record(|stats| {
        stats.schema_saves += 1;
        stats.bytes_serialized += bytes;
        sql_profile_observe_memory(stats, bytes);
    });
}

fn sql_db_cache_key(db: &BicDb) -> usize {
    db as *const BicDb as usize
}

fn sql_schema_cache_get(db: &BicDb, table: &str) -> Option<Option<Arc<TableSchema>>> {
    let key = sql_db_cache_key(db);
    SQL_SCHEMA_CACHE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|cache| cache.get(&key))
            .and_then(|tables| tables.get(table).cloned())
    })
}

fn sql_schema_cache_set(db: &BicDb, table: &str, schema: Option<Arc<TableSchema>>) {
    let key = sql_db_cache_key(db);
    SQL_SCHEMA_CACHE_STACK.with(|stack| {
        if let Some(cache) = stack.borrow_mut().last_mut() {
            cache
                .entry(key)
                .or_default()
                .insert(table.to_string(), schema);
        }
    });
}

fn sql_index_definitions_cache_clear() {
    SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
        if let Some(cache) = stack.borrow_mut().last_mut() {
            cache.clear();
        }
    });
}

thread_local! {
    /// `BicDb::index_definitions()` clones every definition (name, collection,
    /// field list, predicate) on each call, and the statement path asks for it
    /// several times per statement. One shared, name-sorted list per
    /// `index_generation`, per thread.
    static INDEX_DEFINITIONS_MEMO: RefCell<FxHashMap<u64, (u64, Arc<Vec<IndexDefinition>>)>> =
        RefCell::new(FxHashMap::default());
}

/// The database's index definitions, sorted by name, shared: a refcount bump
/// while the index generation is unchanged.
pub(crate) fn index_definitions_shared(db: &BicDb) -> Arc<Vec<IndexDefinition>> {
    // Keyed by the instance id, not the address: a fresh database allocated
    // where a dropped one lived starts at generation 0 too, and an
    // address-keyed memo would hand it the old database's definitions.
    let key = db.instance_id();
    let generation = db.index_generation();
    let hit = INDEX_DEFINITIONS_MEMO.with(|memo| {
        memo.borrow()
            .get(&key)
            .filter(|(cached, _)| *cached == generation)
            .map(|(_, definitions)| Arc::clone(definitions))
    });
    if let Some(hit) = hit {
        return hit;
    }
    let definitions = Arc::new(db.index_definitions());
    INDEX_DEFINITIONS_MEMO.with(|memo| {
        memo.borrow_mut()
            .insert(key, (generation, Arc::clone(&definitions)));
    });
    definitions
}

fn sql_index_definitions_for_collection(db: &BicDb, collection: &str) -> Vec<IndexDefinition> {
    let key = sql_db_cache_key(db);
    if let Some(indexes) = SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|cache| cache.get(&key))
            .and_then(|collections| collections.get(collection).cloned())
    }) {
        return indexes;
    }

    // Core B-tree keys are byte ordered. Keep locale-collated indexes durable so
    // they can enforce deterministic uniqueness, but do not use them for query
    // planning until the storage layer carries ICU sort keys.
    let locale_ordered_indexes = load_schema_shared(db, collection)
        .ok()
        .flatten()
        .map(|schema| {
            schema
                .indexes
                .iter()
                .filter(|index| {
                    index
                        .collations
                        .iter()
                        .any(|oid| Some(*oid) == collation_oid("en-x-icu"))
                })
                .map(|index| index.name.to_ascii_lowercase())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let indexes = index_definitions_shared(db)
        .iter()
        .filter(|index| {
            index.collection == collection
                && !locale_ordered_indexes.contains(&index.name.to_ascii_lowercase())
        })
        .cloned()
        .collect::<Vec<_>>();
    SQL_INDEX_DEFINITIONS_CACHE_STACK.with(|stack| {
        if let Some(cache) = stack.borrow_mut().last_mut() {
            cache
                .entry(key)
                .or_default()
                .insert(collection.to_string(), indexes.clone());
        }
    });
    indexes
}

fn sql_schema_list_cache_key(db: &BicDb) -> usize {
    db as *const BicDb as usize
}

fn sql_schema_list_cache_get(db: &BicDb) -> Option<Arc<Vec<TableSchema>>> {
    let key = sql_schema_list_cache_key(db);
    SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .and_then(|cache| cache.get(&key).cloned())
    })
}

fn sql_schema_list_cache_set(db: &BicDb, schemas: Arc<Vec<TableSchema>>) {
    let key = sql_schema_list_cache_key(db);
    SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| {
        if let Some(cache) = stack.borrow_mut().last_mut() {
            cache.insert(key, schemas);
        }
    });
}

fn sql_schema_list_cache_invalidate(db: &BicDb) {
    let key = sql_schema_list_cache_key(db);
    SQL_SCHEMA_LIST_CACHE_STACK.with(|stack| {
        if let Some(cache) = stack.borrow_mut().last_mut() {
            cache.remove(&key);
        }
    });
}

#[cfg(test)]
fn reset_sql_schema_list_raw_loads() {
    SQL_SCHEMA_LIST_RAW_LOADS.with(|loads| *loads.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_schema_list_raw_loads() -> usize {
    SQL_SCHEMA_LIST_RAW_LOADS.with(|loads| *loads.borrow())
}

#[cfg(test)]
fn reset_sql_row_lookup_compound_joins() {
    SQL_ROW_LOOKUP_COMPOUND_JOINS.with(|joins| *joins.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_row_lookup_compound_joins() -> usize {
    SQL_ROW_LOOKUP_COMPOUND_JOINS.with(|joins| *joins.borrow())
}

#[cfg(test)]
fn reset_sql_parse_statement_calls() {
    SQL_PARSE_STATEMENT_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_parse_statement_calls() -> usize {
    SQL_PARSE_STATEMENT_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_row_from_record_calls() {
    SQL_ROW_FROM_RECORD_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_row_from_record_calls() -> usize {
    SQL_ROW_FROM_RECORD_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_merge_rows_calls() {
    SQL_MERGE_ROWS_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_merge_rows_calls() -> usize {
    SQL_MERGE_ROWS_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_slot_row_to_map_calls() {
    SQL_SLOT_ROW_TO_MAP_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_slot_row_to_map_calls() -> usize {
    SQL_SLOT_ROW_TO_MAP_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_index_storage_lookup_calls() {
    SQL_INDEX_STORAGE_LOOKUP_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_index_storage_lookup_calls() -> usize {
    SQL_INDEX_STORAGE_LOOKUP_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_record_storage_get_calls() {
    SQL_RECORD_STORAGE_GET_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_record_storage_get_calls() -> usize {
    SQL_RECORD_STORAGE_GET_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_cell_row_counts() {
    SQL_CELL_ROWS_FAST.with(|calls| *calls.borrow_mut() = 0);
    SQL_CELL_ROWS_FALLBACK.with(|calls| *calls.borrow_mut() = 0);
}

/// `(rows built from cells, rows that fell back to the Record form)`.
#[cfg(test)]
fn sql_cell_row_counts() -> (usize, usize) {
    (
        SQL_CELL_ROWS_FAST.with(|calls| *calls.borrow()),
        SQL_CELL_ROWS_FALLBACK.with(|calls| *calls.borrow()),
    )
}

#[cfg(test)]
fn reset_sql_records_for_ids_calls() {
    SQL_RECORDS_FOR_IDS_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_records_for_ids_calls() -> usize {
    SQL_RECORDS_FOR_IDS_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_indexed_selection_plan_calls() {
    SQL_INDEXED_SELECTION_PLAN_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_indexed_selection_plan_calls() -> usize {
    SQL_INDEXED_SELECTION_PLAN_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_locator_strategy_derivations() {
    SQL_LOCATOR_STRATEGY_DERIVATIONS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_locator_strategy_derivations() -> usize {
    SQL_LOCATOR_STRATEGY_DERIVATIONS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_stored_delete_hits() {
    SQL_STORED_DELETE_HITS.with(|hits| *hits.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_stored_delete_hits() -> usize {
    SQL_STORED_DELETE_HITS.with(|hits| *hits.borrow())
}

#[cfg(test)]
fn reset_sql_join_constraint_eval_calls() {
    SQL_JOIN_CONSTRAINT_EVAL_CALLS.with(|calls| *calls.borrow_mut() = 0);
}

#[cfg(test)]
fn reset_sql_join_constraint_paths() {
    SQL_JOIN_CONSTRAINT_BOUND_ONCE.with(|calls| *calls.borrow_mut() = 0);
    SQL_JOIN_CONSTRAINT_PAIR_FALLBACKS.with(|calls| *calls.borrow_mut() = 0);
}

/// `(constraints bound once per join, per-pair generic evaluations)`.
#[cfg(test)]
fn sql_join_constraint_paths() -> (usize, usize) {
    (
        SQL_JOIN_CONSTRAINT_BOUND_ONCE.with(|calls| *calls.borrow()),
        SQL_JOIN_CONSTRAINT_PAIR_FALLBACKS.with(|calls| *calls.borrow()),
    )
}

#[cfg(test)]
fn sql_join_constraint_eval_calls() -> usize {
    SQL_JOIN_CONSTRAINT_EVAL_CALLS.with(|calls| *calls.borrow())
}

#[cfg(test)]
fn reset_sql_pending_id_merges() {
    SQL_PENDING_ID_MERGES.with(|merges| *merges.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_pending_id_merges() -> usize {
    SQL_PENDING_ID_MERGES.with(|merges| *merges.borrow())
}

#[cfg(test)]
fn reset_sql_routine_frame_map_syncs() {
    SQL_ROUTINE_FRAME_MAP_SYNCS.with(|syncs| *syncs.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_routine_frame_map_syncs() -> usize {
    SQL_ROUTINE_FRAME_MAP_SYNCS.with(|syncs| *syncs.borrow())
}

#[cfg(test)]
fn reset_sql_routine_slot_array_clones() {
    SQL_ROUTINE_SLOT_ARRAY_CLONES.with(|clones| *clones.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_routine_slot_array_clones() -> usize {
    SQL_ROUTINE_SLOT_ARRAY_CLONES.with(|clones| *clones.borrow())
}

#[cfg(test)]
fn reset_sql_routine_var_snapshots() {
    SQL_ROUTINE_VAR_SNAPSHOTS.with(|snapshots| *snapshots.borrow_mut() = 0);
}

#[cfg(test)]
fn sql_routine_var_snapshots() -> usize {
    SQL_ROUTINE_VAR_SNAPSHOTS.with(|snapshots| *snapshots.borrow())
}

fn sql_profile_rows_materialized(rows: usize, memory_bytes: usize) {
    sql_profile_record(|stats| {
        stats.rows_materialized += rows;
        sql_profile_observe_memory(stats, memory_bytes);
    });
}

fn sql_profile_join_intermediate(rows: usize, candidate_pairs: usize, memory_bytes: usize) {
    sql_profile_record(|stats| {
        stats.join_steps += 1;
        stats.join_intermediate_rows += rows;
        stats.join_candidate_pairs += candidate_pairs;
        sql_profile_observe_memory(stats, memory_bytes);
    });
}

fn sql_profile_index_lookup() {
    sql_profile_record(|stats| stats.index_lookup_count += 1);
}

fn sql_profile_index_catalog_entry_considered() {
    sql_profile_record(|stats| stats.index_catalog_entries_considered += 1);
}

fn sql_profile_join_predicate_row_merge() {
    sql_profile_record(|stats| stats.join_predicate_row_merges += 1);
}

fn sql_profile_write_batch(rows: usize) {
    if rows == 0 {
        return;
    }
    sql_profile_record(|stats| {
        stats.write_batches += 1;
        stats.write_rows += rows;
    });
}

fn sql_profile_full_scan() {
    sql_profile_record(|stats| stats.full_scan_count += 1);
}

fn sql_profile_record_id_prefix_scan() {
    sql_profile_record(|stats| stats.record_id_prefix_scans += 1);
}

fn sql_profile_scalar_subquery() {
    sql_profile_record(|stats| stats.scalar_subqueries += 1);
}

fn sql_profile_scalar_subquery_elapsed(started: Instant) {
    sql_profile_record(|stats| {
        stats.scalar_subquery_elapsed_ms += started.elapsed().as_secs_f64() * 1000.0
    });
}

fn sql_profile_foreign_key_parent_delete_schema_scan() {
    sql_profile_record(|stats| stats.foreign_key_parent_delete_schema_scans += 1);
}

fn sql_profile_foreign_key_child_scan() {
    sql_profile_record(|stats| stats.foreign_key_child_scans += 1);
}

fn sql_profile_write_elapsed(started: Instant) {
    sql_profile_record(|stats| stats.write_elapsed_ms += started.elapsed().as_secs_f64() * 1000.0);
}

fn sql_profile_records_materialized<R: AsRef<Record>>(records: &[R]) {
    if sql_profile_active() {
        sql_profile_rows_materialized(records.len(), records_memory_estimate(records));
    }
}

fn sql_profile_sql_rows_materialized<R: SqlRowMemoryEstimate>(rows: &[R]) {
    if sql_profile_active() {
        sql_profile_rows_materialized(rows.len(), sql_rows_memory_estimate(rows));
    }
}

fn sql_profile_sql_row_refs_materialized<'a, R: SqlRowMemoryEstimate + 'a>(
    rows: impl Iterator<Item = &'a R>,
) {
    if sql_profile_active() {
        let (count, bytes) = rows.fold((0, 0), |(count, bytes), row| {
            (count + 1, bytes + row.memory_estimate())
        });
        sql_profile_rows_materialized(count, bytes);
    }
}

fn sql_profile_join_rows<R: SqlRowMemoryEstimate>(rows: &[R], candidate_pairs: usize) {
    if sql_profile_active() {
        sql_profile_join_intermediate(rows.len(), candidate_pairs, sql_rows_memory_estimate(rows));
    }
}

fn sql_result_memory_estimate(result: &SqlResult) -> usize {
    result.columns.iter().map(String::len).sum::<usize>()
        + sql_result_rows_memory_estimate(&result.rows)
}

fn sql_result_rows_memory_estimate(rows: &[Vec<SqlValue>]) -> usize {
    rows.iter()
        .flatten()
        .map(sql_value_memory_estimate)
        .sum::<usize>()
}

fn records_memory_estimate<R: AsRef<Record>>(records: &[R]) -> usize {
    records
        .iter()
        .map(|record| record_memory_estimate(record.as_ref()))
        .sum()
}

fn record_memory_estimate(record: &Record) -> usize {
    record.id.len()
        + serde_json::to_vec(&record.metadata)
            .map(|bytes| bytes.len())
            .unwrap_or_default()
        + record
            .vector
            .as_ref()
            .map(|vector| vector.len() * std::mem::size_of::<f32>())
            .unwrap_or_default()
        + record.payload.as_ref().map(Vec::len).unwrap_or_default()
        + record
            .geometry
            .as_ref()
            .map(|geometry| geometry.to_wkt().len())
            .unwrap_or_default()
        + record
            .timestamp
            .map(|_| std::mem::size_of::<i64>())
            .unwrap_or_default()
}

trait SqlRowMemoryEstimate {
    fn memory_estimate(&self) -> usize;
}

impl SqlRowMemoryEstimate for FxHashMap<String, SqlValue> {
    fn memory_estimate(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.len() + sql_value_memory_estimate(value))
            .sum()
    }
}

impl SqlRowMemoryEstimate for BTreeMap<String, SqlValue> {
    fn memory_estimate(&self) -> usize {
        self.iter()
            .map(|(key, value)| key.len() + sql_value_memory_estimate(value))
            .sum()
    }
}

impl SqlRowMemoryEstimate for Vec<SqlValue> {
    fn memory_estimate(&self) -> usize {
        self.iter().map(sql_value_memory_estimate).sum()
    }
}

fn sql_rows_memory_estimate<R: SqlRowMemoryEstimate>(rows: &[R]) -> usize {
    rows.iter().map(SqlRowMemoryEstimate::memory_estimate).sum()
}

#[derive(Debug, Error)]
pub enum SqlError {
    #[error(transparent)]
    BicDb(#[from] BicDbError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid SQL: {0}")]
    InvalidSql(String),

    #[error("{0}")]
    InvalidTextRepresentation(String),

    #[error("{message}")]
    DataException {
        sqlstate: &'static str,
        message: String,
        data_type: Option<String>,
    },

    #[error("{message}")]
    ResourceLimit {
        sqlstate: &'static str,
        message: String,
    },

    #[error("type \"{name}\" does not exist")]
    UndefinedType { name: String },

    #[error("unsupported SQL: {0}")]
    Unsupported(String),

    #[error("unsupported PL/pgSQL feature: {0}")]
    UnsupportedPlpgsqlFeature(String),

    #[error("query returned no rows")]
    NoDataFound,

    #[error("collection not found: {0}")]
    InvalidCollection(String),

    #[error("column \"{column}\" of relation \"{table}\" does not exist")]
    UndefinedColumn { table: String, column: String },

    #[error("{message}")]
    TypeMismatch {
        table: String,
        column: String,
        expected: String,
        message: String,
    },

    #[error("{message}")]
    ConstraintViolation {
        sqlstate: &'static str,
        message: String,
        table: Option<String>,
        column: Option<String>,
        constraint: Option<String>,
    },

    #[error("savepoint \"{name}\" does not exist")]
    InvalidSavepoint { name: String },

    #[error("role \"{name}\" already exists")]
    DuplicateRole { name: String },

    #[error("role \"{name}\" does not exist")]
    UndefinedRole { name: String },

    #[error("{message}")]
    InvalidTransactionState { message: String },

    #[error("{message}")]
    RaisedException {
        sqlstate: String,
        message: String,
        detail: Option<String>,
    },
}

impl SqlError {
    pub(crate) fn resource_limit(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self::ResourceLimit {
            sqlstate,
            message: message.into(),
        }
    }

    pub(crate) fn is_resource_limit(&self) -> bool {
        matches!(self, Self::ResourceLimit { .. })
    }

    pub(crate) fn is_query_interruption(&self) -> bool {
        matches!(
            self,
            Self::BicDb(BicDbError::QueryCanceled | BicDbError::QueryTimedOut)
        )
    }

    pub fn invalid_text_representation(
        data_type: impl Into<String>,
        detail: impl std::fmt::Display,
    ) -> Self {
        let data_type = data_type.into();
        Self::DataException {
            sqlstate: "22P02",
            message: format!("invalid input syntax for type {data_type}: {detail}"),
            data_type: Some(data_type),
        }
    }

    pub fn numeric_value_out_of_range(message: impl Into<String>) -> Self {
        Self::data_exception("22003", message, Some("numeric".to_string()))
    }

    pub fn string_data_right_truncation(message: impl Into<String>) -> Self {
        Self::data_exception("22001", message, Some("text".to_string()))
    }

    pub fn invalid_datetime_format(message: impl Into<String>) -> Self {
        Self::data_exception("22007", message, None)
    }

    pub fn invalid_parameter_value(message: impl Into<String>) -> Self {
        Self::data_exception("22023", message, None)
    }

    pub fn invalid_xml_content(message: impl Into<String>) -> Self {
        Self::data_exception("2200N", message, Some("xml".to_string()))
    }

    pub fn money_out_of_range() -> Self {
        Self::data_exception("22003", "money out of range", Some("money".to_string()))
    }

    pub fn cannot_coerce(message: impl Into<String>) -> Self {
        Self::data_exception("42846", message, None)
    }

    pub fn undefined_function(message: impl Into<String>) -> Self {
        Self::data_exception("42883", message, None)
    }

    pub fn undefined_object(message: impl Into<String>) -> Self {
        Self::data_exception("42704", message, None)
    }

    pub fn sequence_generator_limit(message: impl Into<String>) -> Self {
        Self::data_exception("2200H", message, None)
    }

    pub fn dependent_objects_still_exist(message: impl Into<String>) -> Self {
        Self::data_exception("2BP01", message, None)
    }

    pub fn object_not_in_prerequisite_state(message: impl Into<String>) -> Self {
        Self::data_exception("55000", message, None)
    }

    pub fn generated_always_violation(message: impl Into<String>) -> Self {
        Self::data_exception("428C9", message, None)
    }

    pub fn undefined_type(name: impl Into<String>) -> Self {
        Self::UndefinedType { name: name.into() }
    }

    fn data_exception(
        sqlstate: &'static str,
        message: impl Into<String>,
        data_type: Option<String>,
    ) -> Self {
        Self::DataException {
            sqlstate,
            message: message.into(),
            data_type,
        }
    }

    pub(crate) fn data_exception_public(
        sqlstate: &'static str,
        message: impl Into<String>,
        data_type: Option<String>,
    ) -> Self {
        Self::data_exception(sqlstate, message, data_type)
    }

    pub(crate) fn duplicate_constraint(table: &str, constraint: &str) -> Self {
        Self::ConstraintViolation {
            sqlstate: "42710",
            message: format!("constraint \"{constraint}\" for relation \"{table}\" already exists"),
            table: Some(table.to_string()),
            column: None,
            constraint: Some(constraint.to_string()),
        }
    }

    pub fn sqlstate(&self) -> &str {
        match self {
            Self::ConstraintViolation { sqlstate, .. } => sqlstate,
            Self::InvalidSavepoint { .. } => "3B001",
            Self::DuplicateRole { .. } => "42710",
            Self::UndefinedRole { .. } => "42704",
            Self::InvalidTransactionState { .. } => "25P01",
            Self::RaisedException { sqlstate, .. } => sqlstate,
            Self::InvalidSql(_) => "42601",
            Self::InvalidTextRepresentation(_) => "22P02",
            Self::DataException { sqlstate, .. } => sqlstate,
            Self::ResourceLimit { sqlstate, .. } => sqlstate,
            Self::UndefinedType { .. } => "42704",
            Self::Unsupported(_) | Self::UnsupportedPlpgsqlFeature(_) => "0A000",
            Self::NoDataFound => "P0002",
            Self::InvalidCollection(_) => "42P01",
            Self::UndefinedColumn { .. } => "42703",
            Self::TypeMismatch { .. } => "22P02",
            Self::BicDb(error) => bicdb_error_sqlstate(error),
            Self::Json(_) => "XX000",
        }
    }

    pub fn fields(&self) -> Vec<SqlErrorField> {
        match self {
            Self::RaisedException {
                detail: Some(detail),
                ..
            } => vec![SqlErrorField::Detail(detail.clone())],
            Self::InvalidCollection(table) => vec![SqlErrorField::Table(table.clone())],
            Self::UndefinedColumn { table, column } => vec![
                SqlErrorField::Table(table.clone()),
                SqlErrorField::Column(column.clone()),
            ],
            Self::TypeMismatch {
                table,
                column,
                expected,
                ..
            } => vec![
                SqlErrorField::Table(table.clone()),
                SqlErrorField::Column(column.clone()),
                SqlErrorField::DataType(expected.clone()),
            ],
            Self::DataException {
                data_type: Some(data_type),
                ..
            } => vec![SqlErrorField::DataType(data_type.clone())],
            Self::ConstraintViolation {
                table,
                column,
                constraint,
                ..
            } => {
                let mut fields = Vec::new();
                if let Some(table) = table {
                    fields.push(SqlErrorField::Table(table.clone()));
                }
                if let Some(column) = column {
                    fields.push(SqlErrorField::Column(column.clone()));
                }
                if let Some(constraint) = constraint {
                    fields.push(SqlErrorField::Constraint(constraint.clone()));
                }
                fields
            }
            Self::BicDb(BicDbError::CollectionNotFound(collection)) => {
                vec![SqlErrorField::Table(collection.clone())]
            }
            _ => Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlErrorField {
    Detail(String),
    Schema(String),
    Table(String),
    Column(String),
    DataType(String),
    Constraint(String),
}

fn bicdb_error_sqlstate(error: &BicDbError) -> &'static str {
    match error {
        BicDbError::Authorization(_) => "42501",
        BicDbError::CollectionNotFound(_) => "42P01",
        BicDbError::CollectionAlreadyExists(_) => "42P07",
        BicDbError::InvalidCollectionName(_) => "42602",
        BicDbError::TransactionConflict(_) => "40001",
        BicDbError::TransactionNotPending => "25P01",
        BicDbError::QueryCanceled | BicDbError::QueryTimedOut => "57014",
        BicDbError::EmptyRecordId
        | BicDbError::EmptyVector
        | BicDbError::NonFiniteVectorValue
        | BicDbError::InvalidTopK
        | BicDbError::DimensionMismatch { .. }
        | BicDbError::InvalidTimeRange { .. } => "22000",
        _ => "XX000",
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_tag: Option<String>,
    /// Per-column logical PostgreSQL type name (e.g. `"int8"`, `"int4"`, `"text"`),
    /// determined from the schema/expression at plan time rather than from the
    /// runtime value width. `Some(name)` authoritatively sets the wire type OID;
    /// `None` (or a short/empty vector) means "unknown", and the pgwire layer
    /// falls back to its value-width heuristic. Empty by default so this stays
    /// additive — callers that don't populate it keep their previous behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_types: Vec<Option<String>>,
    /// PostgreSQL origin metadata for each result column. Direct table-column
    /// references carry their relation OID, attribute number, and declared
    /// typmod; computed expressions use PostgreSQL's zero/-1 sentinel values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_metadata: Vec<SqlColumnMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlColumnMetadata {
    pub table_oid: i32,
    pub attribute_number: i16,
    pub type_modifier: i32,
}

impl Default for SqlColumnMetadata {
    fn default() -> Self {
        Self {
            table_oid: 0,
            attribute_number: 0,
            type_modifier: -1,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SqlRowStream {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
    position: usize,
    command_tag: Option<String>,
    column_types: Vec<Option<String>>,
    column_metadata: Vec<SqlColumnMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlIntegrityReport {
    pub valid: bool,
    pub tables_checked: usize,
    pub records_checked: usize,
    pub constraints_checked: usize,
    pub indexes_checked: usize,
    pub violations: Vec<SqlIntegrityViolation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlIntegrityViolation {
    pub check: String,
    pub table: Option<String>,
    pub object: Option<String>,
    pub record_id: Option<String>,
    pub message: String,
}

impl SqlResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self {
            columns,
            rows,
            command_tag: None,
            column_types: Vec::new(),
            column_metadata: Vec::new(),
        }
    }

    pub fn empty(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            command_tag: None,
            column_types: Vec::new(),
            column_metadata: Vec::new(),
        }
    }

    pub fn command(tag: impl Into<String>) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: Some(tag.into()),
            column_types: Vec::new(),
            column_metadata: Vec::new(),
        }
    }

    /// Attach per-column logical type names (see [`SqlResult::column_types`]).
    /// Used by the SELECT/aggregate planners so the wire layer can report the
    /// schema/expression type OID instead of guessing from the value width.
    ///
    /// When every entry is `None` (nothing could be typed statically) the vector
    /// is dropped: an all-`None` vector is behaviorally identical to an empty one
    /// (the wire layer falls back per column either way), and keeping it empty
    /// preserves `SqlResult` equality for callers that don't expect metadata.
    pub fn with_column_types(mut self, column_types: Vec<Option<String>>) -> Self {
        self.column_types = if column_types.iter().any(Option::is_some) {
            column_types
        } else {
            Vec::new()
        };
        self
    }

    pub fn with_column_metadata(mut self, mut metadata: Vec<SqlColumnMetadata>) -> Self {
        metadata.resize(self.columns.len(), SqlColumnMetadata::default());
        metadata.truncate(self.columns.len());
        if metadata
            .iter()
            .any(|item| item != &SqlColumnMetadata::default())
        {
            self.column_metadata = metadata;
        }
        self
    }

    pub fn command_complete_tag(&self) -> String {
        self.command_tag
            .clone()
            .unwrap_or_else(|| format!("SELECT {}", self.rows.len()))
    }

    pub fn into_stream(self) -> SqlRowStream {
        SqlRowStream {
            columns: self.columns,
            rows: self.rows,
            position: 0,
            command_tag: self.command_tag,
            column_types: self.column_types,
            column_metadata: self.column_metadata,
        }
    }
}

impl SqlRowStream {
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Per-column logical PostgreSQL type names carried from the originating
    /// [`SqlResult`] (see [`SqlResult::column_types`]).
    pub fn column_types(&self) -> &[Option<String>] {
        &self.column_types
    }

    pub fn column_metadata(&self) -> &[SqlColumnMetadata] {
        &self.column_metadata
    }

    pub fn remaining_rows(&self) -> usize {
        self.rows.len().saturating_sub(self.position)
    }

    pub fn is_done(&self) -> bool {
        self.position >= self.rows.len()
    }

    pub fn memory_estimate(&self) -> usize {
        self.columns.iter().map(String::len).sum::<usize>()
            + self
                .rows
                .iter()
                .skip(self.position)
                .flatten()
                .map(sql_value_memory_estimate)
                .sum::<usize>()
    }

    pub fn next_batch(&mut self, max_rows: usize) -> Vec<Vec<SqlValue>> {
        let limit = if max_rows == 0 {
            self.remaining_rows()
        } else {
            max_rows.min(self.remaining_rows())
        };
        let end = self.position + limit;
        let batch = self.rows[self.position..end].to_vec();
        self.position = end;
        batch
    }

    pub fn command_complete_tag(&self) -> String {
        self.command_tag
            .clone()
            .unwrap_or_else(|| format!("SELECT {}", self.rows.len()))
    }

    pub fn empty_result(&self) -> SqlResult {
        SqlResult {
            columns: self.columns.clone(),
            rows: Vec::new(),
            command_tag: self.command_tag.clone(),
            column_types: self.column_types.clone(),
            column_metadata: self.column_metadata.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlCompositeField {
    pub name: String,
    pub pg_type: String,
    pub value: SqlValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SqlComposite {
    pub type_oid: Option<u32>,
    pub type_name: String,
    pub fields: Vec<SqlCompositeField>,
}

impl SqlComposite {
    pub fn anonymous(values: Vec<(String, SqlValue)>) -> Self {
        Self {
            type_oid: None,
            type_name: "record".to_string(),
            fields: values
                .into_iter()
                .enumerate()
                .map(|(index, (pg_type, value))| SqlCompositeField {
                    name: format!("f{}", index + 1),
                    pg_type,
                    value,
                })
                .collect(),
        }
    }

    pub fn field(&self, name: &str) -> Option<&SqlValue> {
        self.fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(name))
            .map(|field| &field.value)
    }

    pub fn to_postgres_text(&self) -> String {
        let fields = self
            .fields
            .iter()
            .map(composite_field_text)
            .collect::<Vec<_>>();
        format!("({})", fields.join(","))
    }
}

pub fn pg_composite_from_array_json(value: &JsonValue) -> Option<SqlComposite> {
    let composite = value.get("$bicdb_composite")?;
    let type_oid = composite
        .get("type_oid")
        .and_then(JsonValue::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    let type_name = composite.get("type_name")?.as_str()?.to_string();
    let fields = composite
        .get("fields")?
        .as_array()?
        .iter()
        .map(|field| {
            let value = field.get("value")?;
            Some(SqlCompositeField {
                name: field.get("name")?.as_str()?.to_string(),
                pg_type: field.get("pg_type")?.as_str()?.to_string(),
                value: if let Some(composite) = pg_composite_from_array_json(value) {
                    SqlValue::Composite(composite)
                } else {
                    match value {
                        JsonValue::Null => SqlValue::Null,
                        JsonValue::Bool(value) => SqlValue::Bool(*value),
                        JsonValue::Number(value) if value.is_i64() => {
                            SqlValue::Int(value.as_i64()?)
                        }
                        JsonValue::Number(value) => SqlValue::Float(value.as_f64()?),
                        JsonValue::String(value) => SqlValue::String(value.clone()),
                        value => SqlValue::Json(value.clone()),
                    }
                },
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(SqlComposite {
        type_oid,
        type_name,
        fields,
    })
}

fn composite_field_text(field: &SqlCompositeField) -> String {
    if matches!(field.value, SqlValue::Null) {
        return String::new();
    }
    let text = if field.pg_type.ends_with("[]") {
        composite_array_text(&field.value).unwrap_or_else(|| field.value.to_cell())
    } else {
        field.value.to_cell()
    };
    let quoted = text.is_empty()
        || text.chars().any(|character| {
            character.is_whitespace() || matches!(character, ',' | '(' | ')' | '"' | '\\')
        });
    if !quoted {
        return text;
    }
    let mut escaped = String::with_capacity(text.len() + 2);
    escaped.push('"');
    for character in text.chars() {
        if matches!(character, '"' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('"');
    escaped
}

fn composite_array_text(value: &SqlValue) -> Option<String> {
    let SqlValue::Json(value) = value else {
        return None;
    };
    let value = value
        .as_object()
        .and_then(|object| object.get("$bicdb_array_input"))
        .and_then(JsonValue::as_object)
        .and_then(|input| input.get("value"))
        .unwrap_or(value);
    fn render(value: &JsonValue) -> String {
        match value {
            JsonValue::Array(values) => format!(
                "{{{}}}",
                values.iter().map(render).collect::<Vec<_>>().join(",")
            ),
            JsonValue::Null => "NULL".to_string(),
            JsonValue::String(value) => {
                let quoted = value.is_empty()
                    || value.eq_ignore_ascii_case("null")
                    || value.chars().any(|character| {
                        character.is_whitespace()
                            || matches!(character, ',' | '{' | '}' | '"' | '\\')
                    });
                if !quoted {
                    return value.clone();
                }
                let mut escaped = String::with_capacity(value.len() + 2);
                escaped.push('"');
                for character in value.chars() {
                    if matches!(character, '"' | '\\') {
                        escaped.push('\\');
                    }
                    escaped.push(character);
                }
                escaped.push('"');
                escaped
            }
            value => value.to_string(),
        }
    }
    value.is_array().then(|| render(value))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    JsonText(PgJsonText),
    Json(JsonValue),
    Geometry(Geometry),
    TsQuery(#[serde(serialize_with = "serialize_sql_tsquery")] PgTsQuery),
    Composite(SqlComposite),
}

fn serialize_sql_tsquery<S>(
    value: &PgTsQuery,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_postgres_text())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PgJsonText {
    raw: String,
    parsed: JsonValue,
    #[serde(default)]
    invalid_unicode_escape: bool,
}

impl PgJsonText {
    pub fn parse(raw: String) -> std::result::Result<Self, serde_json::Error> {
        let (parseable, invalid_unicode_escape) = sanitize_pg_json_unicode(&raw);
        let parsed = serde_json::from_str(&parseable)?;
        Ok(Self {
            raw,
            parsed,
            invalid_unicode_escape,
        })
    }

    pub fn from_value(parsed: JsonValue) -> Self {
        Self {
            raw: parsed.to_string(),
            parsed,
            invalid_unicode_escape: false,
        }
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub fn parsed(&self) -> &JsonValue {
        &self.parsed
    }

    pub fn has_invalid_unicode_escape(&self) -> bool {
        self.invalid_unicode_escape
    }
}

pub(crate) fn sanitize_pg_json_unicode(raw: &str) -> (String, bool) {
    let bytes = raw.as_bytes();
    let mut output = String::with_capacity(raw.len());
    let mut invalid = false;
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'\\' || cursor + 1 >= bytes.len() {
            let character = raw[cursor..]
                .chars()
                .next()
                .expect("cursor remains on a character boundary");
            output.push(character);
            cursor += character.len_utf8();
            continue;
        }
        if bytes[cursor + 1] != b'u' || cursor + 6 > bytes.len() {
            let end = (cursor + 2).min(bytes.len());
            output.push_str(&raw[cursor..end]);
            cursor = end;
            continue;
        }
        let Some(code) = std::str::from_utf8(&bytes[cursor + 2..cursor + 6])
            .ok()
            .and_then(|digits| u16::from_str_radix(digits, 16).ok())
        else {
            output.push_str(&raw[cursor..cursor + 6]);
            cursor += 6;
            continue;
        };
        if code == 0 {
            invalid = true;
        }
        if (0xD800..=0xDBFF).contains(&code) {
            let paired = cursor + 12 <= bytes.len()
                && &bytes[cursor + 6..cursor + 8] == b"\\u"
                && std::str::from_utf8(&bytes[cursor + 8..cursor + 12])
                    .ok()
                    .and_then(|digits| u16::from_str_radix(digits, 16).ok())
                    .is_some_and(|low| (0xDC00..=0xDFFF).contains(&low));
            if paired {
                output.push_str(&raw[cursor..cursor + 12]);
                cursor += 12;
                continue;
            }
            output.push_str("\\uFFFD");
            invalid = true;
            cursor += 6;
            continue;
        }
        if (0xDC00..=0xDFFF).contains(&code) {
            output.push_str("\\uFFFD");
            invalid = true;
            cursor += 6;
            continue;
        }
        output.push_str(&raw[cursor..cursor + 6]);
        cursor += 6;
    }
    (output, invalid)
}

fn sql_value_memory_estimate(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 0,
        SqlValue::Bool(_) => std::mem::size_of::<bool>(),
        SqlValue::Int(_) => std::mem::size_of::<i64>(),
        SqlValue::Float(_) => std::mem::size_of::<f64>(),
        SqlValue::String(value) => value.len(),
        SqlValue::TsQuery(value) => value.to_postgres_text().len(),
        SqlValue::JsonText(value) => value.raw.len() + value.parsed.to_string().len(),
        SqlValue::Json(value) => value.to_string().len(),
        SqlValue::Geometry(value) => format!("{value:?}").len(),
        SqlValue::Composite(value) => value
            .fields
            .iter()
            .map(|field| {
                field.name.len() + field.pg_type.len() + sql_value_memory_estimate(&field.value)
            })
            .sum(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionControl {
    Begin,
    SetTransaction,
}

#[derive(Clone, Debug)]
struct BulkInsertDefault {
    column: String,
    values: BulkInsertDefaultValues,
}

#[derive(Clone, Debug)]
enum BulkInsertDefaultValues {
    Sequence(Vec<i64>),
    Static(SqlValue),
    Expr { expr: Expr, pg_type: String },
}

impl SqlValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            Self::Json(JsonValue::Number(value)) => value.as_f64(),
            _ => None,
        }
    }

    pub fn to_cell(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Bool(value) => value.to_string(),
            // itoa emits the same bytes as `i64: Display` without going through
            // the `core::fmt` machinery — this runs per returned cell on the
            // pgwire DataRow encode path.
            Self::Int(value) => itoa::Buffer::new().format(*value).to_owned(),
            Self::Float(value) => {
                let mut rendered = value.to_string();
                if rendered.ends_with(".0") {
                    rendered.truncate(rendered.len() - 2);
                }
                rendered
            }
            Self::String(value) => pg_internal_char_byte(self)
                .map(pg_internal_char_text)
                .unwrap_or_else(|| value.clone()),
            Self::TsQuery(value) => value.to_postgres_text(),
            Self::JsonText(value) => value.raw.clone(),
            Self::Json(value) => crate::jsonb::compact_jsonb_cell_text(value),
            Self::Geometry(value) => value.to_wkt(),
            Self::Composite(value) => value.to_postgres_text(),
        }
    }
}

// PostgreSQL rejects NUL in text, so this internal byte carrier cannot collide with user input.
const INTERNAL_CHAR_PREFIX: &str = "\0bicdb-pg-char:";

pub fn pg_internal_char_value(byte: u8) -> SqlValue {
    SqlValue::String(format!("{INTERNAL_CHAR_PREFIX}{byte:02x}"))
}

pub fn pg_internal_char_byte(value: &SqlValue) -> Option<u8> {
    let SqlValue::String(value) = value else {
        return None;
    };
    value
        .strip_prefix(INTERNAL_CHAR_PREFIX)
        .filter(|hex| hex.len() == 2)
        .and_then(|hex| u8::from_str_radix(hex, 16).ok())
}

pub fn pg_internal_char_text(byte: u8) -> String {
    match byte {
        0 => String::new(),
        1..=127 => char::from(byte).to_string(),
        _ => format!("\\{byte:03o}"),
    }
}

const SCHEMA_COLLECTION: &str = "__bicdb_pg_schema";
const NAMESPACE_COLLECTION: &str = "__bicdb_pg_namespaces";
const DATABASE_COLLECTION: &str = "__bicdb_pg_databases";
const EXTENSION_COLLECTION: &str = "__bicdb_pg_extensions";
const SEQUENCE_COLLECTION: &str = "__bicdb_pg_sequences";
const LASTVAL_SESSION_KEY: &str = "__bicdb_lastval";
const VIEW_COLLECTION: &str = "__bicdb_pg_views";
const ROUTINE_COLLECTION: &str = "__bicdb_pg_routines";
const TRIGGER_COLLECTION: &str = "__bicdb_pg_triggers";
const NOTIFICATION_COLLECTION: &str = "__bicdb_pg_notifications";
const ROLE_COLLECTION: &str = "__bicdb_pg_roles";
const ROLE_MEMBERSHIP_COLLECTION: &str = "__bicdb_pg_role_memberships";
const PRIVILEGE_COLLECTION: &str = "__bicdb_pg_privileges";
const DEFAULT_PRIVILEGE_COLLECTION: &str = "__bicdb_pg_default_privileges";
const USER_TYPE_COLLECTION: &str = "__bicdb_pg_user_types";
const USER_TYPE_OID_COLLECTION: &str = "__bicdb_pg_user_type_oids";
const MIGRATION_VERSION_COLLECTION: &str = "__bicdb_schema_version";
const MIGRATION_HISTORY_COLLECTION: &str = "__bicdb_migration_history";
const GRAPH_NODES_TABLE: &str = "bicdb_graph_nodes";
const GRAPH_EDGES_TABLE: &str = "bicdb_graph_edges";
const MAX_RECURSIVE_CTE_ITERATIONS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorSearchMode {
    Exact,
    Ann,
}

#[derive(Clone, Copy, Debug)]
struct SqlSettings {
    vector_search: VectorSearchMode,
    ef_search: usize,
}

impl Default for SqlSettings {
    fn default() -> Self {
        Self {
            vector_search: VectorSearchMode::Exact,
            ef_search: 50,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TableSchema {
    name: String,
    #[serde(default = "default_schema_name")]
    schema_name: String,
    #[serde(default)]
    row_type_oid: Option<i64>,
    #[serde(default)]
    row_array_type_oid: Option<i64>,
    columns: Vec<ColumnSchema>,
    #[serde(default)]
    primary_key_name: Option<String>,
    #[serde(default)]
    indexes: Vec<IndexSchema>,
    #[serde(default)]
    constraints: Vec<ConstraintSchema>,
    #[serde(default)]
    rls_enabled: bool,
    #[serde(default)]
    rls_forced: bool,
    #[serde(default)]
    policies: Vec<PolicySchema>,
    // Owning role; None on schemas persisted before ownership tracking, which
    // are treated as owned by the bootstrap role.
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    partitioning: Option<PartitioningSchema>,
    #[serde(default)]
    partition_of: Option<PartitionOfSchema>,
}

/// A logical SQL column projected over an existing BicDB collection for an
/// embedded application. The collection remains the storage owner; this
/// schema gives the SQL executor the same typed field vocabulary as the
/// application resource runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedTableColumn {
    pub name: String,
    pub pg_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub vector_dimensions: Option<u32>,
}

/// Compiler-signed PostgreSQL RLS projection for one embedded application
/// relation. The application runtime installs this contract as forced row
/// security so raw/declared SQL and typed resource access share one policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedTableRowPolicy {
    pub select_using: String,
    pub insert_with_check: String,
    pub update_using: String,
    pub update_with_check: String,
    pub delete_using: String,
}

/// Reversible catalog change returned by [`ensure_embedded_table_schema`].
#[derive(Clone, Debug)]
pub struct EmbeddedTableSchemaReceipt {
    table: String,
    previous: Option<TableSchema>,
}

impl EmbeddedTableSchemaReceipt {
    /// Restore the SQL catalog entry that preceded an embedded application
    /// schema activation. Collection records are not changed.
    pub fn rollback(&self, db: &mut BicDb) -> Result<()> {
        match &self.previous {
            Some(schema) => save_schema(db, schema),
            None => delete_schema(db, &self.table),
        }
    }
}

#[derive(Clone, Debug)]
struct TableCatalogSummary {
    name: String,
    schema_name: String,
    partitioned: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartitioningSchema {
    strategy: String,
    key_columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartitionOfSchema {
    parent_table: String,
    parent_schema: String,
    bound: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ColumnSchema {
    name: String,
    pg_type: String,
    #[serde(default)]
    user_type: Option<UserTypeColumnSchema>,
    #[serde(default)]
    collation: Option<String>,
    #[serde(default)]
    type_modifier: Option<PgTypeModifier>,
    #[serde(default)]
    array_ndims: usize,
    #[serde(default)]
    compression: Option<char>,
    #[serde(default)]
    primary_key: bool,
    #[serde(default)]
    hidden: bool,
    #[serde(default = "default_nullable")]
    nullable: bool,
    #[serde(default)]
    vector_dim: Option<usize>,
    #[serde(default)]
    default_sequence: Option<String>,
    #[serde(default)]
    default_value: Option<SqlValue>,
    #[serde(default)]
    default_expr: Option<String>,
    #[serde(default)]
    generated_expr: Option<String>,
    #[serde(default)]
    identity: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PgTypeModifier {
    Numeric {
        precision: u16,
        scale: i16,
    },
    Character {
        length: u32,
    },
    Bit {
        length: u32,
    },
    Temporal {
        precision: u8,
    },
    Interval {
        fields: Option<String>,
        precision: Option<u8>,
    },
    Vector {
        dimensions: u16,
    },
}

impl PgTypeModifier {
    fn catalog_value(&self) -> i64 {
        match self {
            Self::Numeric { precision, scale } => {
                let encoded_scale = i32::from(*scale) & 0x7ff;
                i64::from(((i32::from(*precision) << 16) | encoded_scale) + 4)
            }
            Self::Character { length } => i64::from(*length) + 4,
            Self::Bit { length } => i64::from(*length),
            Self::Temporal { precision } => i64::from(*precision),
            Self::Interval { fields, precision } => {
                let range = fields.as_deref().map(interval_field_mask).unwrap_or(0x7fff);
                let precision = precision.map(u32::from).unwrap_or(0xffff);
                i64::from((range << 16) | precision)
            }
            Self::Vector { dimensions } => i64::from(*dimensions),
        }
    }
}

fn interval_field_mask(fields: &str) -> u32 {
    match fields {
        "YEAR" => 0x0004,
        "MONTH" => 0x0002,
        "DAY" => 0x0008,
        "HOUR" => 0x0400,
        "MINUTE" => 0x0800,
        "SECOND" => 0x1000,
        "YEAR TO MONTH" => 0x0006,
        "DAY TO HOUR" => 0x0408,
        "DAY TO MINUTE" => 0x0c08,
        "DAY TO SECOND" => 0x1c08,
        "HOUR TO MINUTE" => 0x0c00,
        "HOUR TO SECOND" => 0x1c00,
        "MINUTE TO SECOND" => 0x1800,
        _ => 0x7fff,
    }
}

impl ColumnSchema {
    fn effective_default_expr(&self) -> Option<String> {
        self.default_expr.clone().or_else(|| {
            self.user_type.as_ref().and_then(|user_type| {
                if user_type.array {
                    return None;
                }
                match &user_type.kind {
                    UserTypeKind::Base { default_expr, .. } => default_expr.clone(),
                    UserTypeKind::Domain { default_expr, .. } => default_expr.clone(),
                    UserTypeKind::Shell
                    | UserTypeKind::Enum { .. }
                    | UserTypeKind::Composite { .. }
                    | UserTypeKind::Range { .. }
                    | UserTypeKind::Multirange { .. } => None,
                }
            })
        })
    }

    fn catalog_array_ndims(&self) -> i64 {
        if self.type_is_array() {
            self.array_ndims.max(1) as i64
        } else {
            0
        }
    }

    fn type_oid(&self) -> i64 {
        if self.hidden && self.pg_type.is_empty() && self.user_type.is_none() {
            return 0;
        }
        self.user_type
            .as_ref()
            .map(UserTypeColumnSchema::type_oid)
            .unwrap_or_else(|| pg_type_oid(&self.pg_type))
    }

    fn type_len(&self) -> i64 {
        match self.user_type.as_ref() {
            Some(user_type) if user_type.array => -1,
            Some(user_type) => user_type.scalar_type_len(),
            None => pg_type_len(&self.pg_type),
        }
    }

    fn type_by_value(&self) -> bool {
        match self.user_type.as_ref() {
            Some(user_type) => !user_type.array && user_type.scalar_by_value(),
            None => pg_type_by_value(&self.pg_type),
        }
    }

    fn type_align(&self) -> char {
        self.user_type
            .as_ref()
            .map(|user_type| {
                if user_type.array {
                    if user_type.scalar_align() == 'd' {
                        'd'
                    } else {
                        'i'
                    }
                } else {
                    user_type.scalar_align()
                }
            })
            .unwrap_or_else(|| pg_type_align(&self.pg_type))
    }

    fn type_storage(&self) -> char {
        match self.user_type.as_ref() {
            Some(user_type) if user_type.array => 'x',
            Some(user_type) => user_type.scalar_storage(),
            None => pg_type_storage(&self.pg_type),
        }
    }

    fn type_is_array(&self) -> bool {
        self.user_type
            .as_ref()
            .map(|user_type| user_type.array)
            .unwrap_or_else(|| pg_type_is_array(&self.pg_type))
    }

    fn collation_oid(&self) -> i64 {
        self.collation
            .as_deref()
            .and_then(collation_oid)
            .unwrap_or_else(|| {
                self.user_type
                    .as_ref()
                    .map(UserTypeColumnSchema::scalar_collation_oid)
                    .unwrap_or_else(|| type_collation_oid(&self.pg_type))
            })
    }

    fn catalog_typmod(&self) -> i64 {
        self.type_modifier
            .as_ref()
            .map(PgTypeModifier::catalog_value)
            .unwrap_or(-1)
    }

    fn formatted_pg_type(&self) -> String {
        if self.hidden && self.pg_type.is_empty() && self.user_type.is_none() {
            return "-".to_string();
        }
        if let Some(user_type) = &self.user_type {
            return user_type.formatted_name();
        }
        pg_format_type(
            pg_type_oid(&self.pg_type) as i32,
            self.catalog_typmod() as i32,
        )
        .unwrap_or_else(|| self.pg_type.clone())
    }

    fn information_schema_data_type(&self) -> String {
        if self.type_is_array() {
            return "ARRAY".to_string();
        }
        if let Some(user_type) = &self.user_type {
            return user_type.information_schema_scalar_data_type();
        }
        information_schema_builtin_data_type(&self.pg_type)
    }

    fn information_schema_udt_name(&self) -> String {
        if let Some(user_type) = &self.user_type {
            return if user_type.array {
                format!("_{}", user_type.name)
            } else {
                user_type.information_schema_scalar_udt_name()
            };
        }
        if let Some(spec) = i32::try_from(self.type_oid())
            .ok()
            .and_then(pg_array_element_spec_by_oid)
        {
            return format!("_{}", spec.name);
        }
        self.pg_type.clone()
    }

    fn information_schema_udt_schema(&self) -> String {
        self.user_type
            .as_ref()
            .map(|user_type| {
                if user_type.array {
                    user_type.schema_name.clone()
                } else {
                    user_type.information_schema_scalar_udt_schema()
                }
            })
            .unwrap_or_else(|| "pg_catalog".to_string())
    }

    fn information_schema_domain(&self) -> Option<(String, String)> {
        self.user_type.as_ref().and_then(|user_type| {
            (!user_type.array && matches!(user_type.kind, UserTypeKind::Domain { .. }))
                .then(|| (user_type.schema_name.clone(), user_type.name.clone()))
        })
    }

    fn character_maximum_length(&self) -> Option<i64> {
        if !matches!(self.pg_type.as_str(), "bpchar" | "varchar") {
            return None;
        }
        match self.type_modifier.as_ref() {
            Some(PgTypeModifier::Character { length }) => Some(i64::from(*length)),
            _ => None,
        }
    }

    fn character_octet_length(&self) -> Option<i64> {
        match self.pg_type.as_str() {
            "text" | "bpchar" | "varchar" => Some(
                self.character_maximum_length()
                    .map(|length| length * 4)
                    .unwrap_or(1_073_741_824),
            ),
            _ => None,
        }
    }
}

fn information_schema_builtin_data_type(pg_type: &str) -> String {
    match pg_type {
        "bool" => "boolean",
        "int2" => "smallint",
        "int4" => "integer",
        "int8" => "bigint",
        "float4" => "real",
        "float8" => "double precision",
        "bpchar" => "character",
        "varchar" => "character varying",
        other => other,
    }
    .to_string()
}

fn collation_oid(name: &str) -> Option<i64> {
    match name {
        "default" => Some(100),
        "C" => Some(950),
        "POSIX" => Some(951),
        "ucs_basic" => Some(962),
        "en-x-icu" => Some(810_001),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexSchema {
    name: String,
    expression: String,
    #[serde(default)]
    source_expressions: Vec<String>,
    #[serde(default)]
    operator_classes: Vec<String>,
    #[serde(default)]
    internal_index_names: Vec<String>,
    #[serde(default)]
    collations: Vec<i64>,
    #[serde(default)]
    unique: bool,
    #[serde(default = "default_index_access_method")]
    access_method: String,
    #[serde(default)]
    metadata_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PolicySchema {
    name: String,
    command: PolicyCommand,
    #[serde(default)]
    using_expr: Option<String>,
    #[serde(default)]
    check_expr: Option<String>,
    #[serde(default = "default_true")]
    permissive: bool,
    #[serde(default)]
    enforced: bool,
    // Roles the policy applies to (`TO role, ...`); `public` matches every role.
    #[serde(default = "default_policy_roles")]
    roles: Vec<String>,
}

fn default_policy_roles() -> Vec<String> {
    vec!["public".to_string()]
}

impl PolicySchema {
    fn applies_to_public(&self) -> bool {
        self.roles
            .iter()
            .any(|role| role.eq_ignore_ascii_case("public"))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolicyAction {
    Select,
    Insert,
    Update,
    Delete,
}

impl PolicyCommand {
    fn from_keyword(keyword: &str) -> Result<Self> {
        match keyword.to_ascii_uppercase().as_str() {
            "ALL" => Ok(Self::All),
            "SELECT" => Ok(Self::Select),
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            other => Err(SqlError::Unsupported(format!(
                "CREATE POLICY command {other} is not supported"
            ))),
        }
    }

    fn pg_code(self) -> &'static str {
        match self {
            Self::All => "*",
            Self::Select => "r",
            Self::Insert => "a",
            Self::Update => "w",
            Self::Delete => "d",
        }
    }

    fn pg_policies_command(self) -> &'static str {
        match self {
            Self::All => "ALL",
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

fn default_index_access_method() -> String {
    "btree".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ConstraintSchema {
    Unique {
        name: String,
        columns: Vec<String>,
        #[serde(default = "default_true")]
        validated: bool,
    },
    Check {
        name: String,
        expression: String,
        #[serde(default = "default_true")]
        validated: bool,
    },
    ForeignKey {
        name: String,
        columns: Vec<String>,
        foreign_table: String,
        referred_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
        #[serde(default = "default_true")]
        validated: bool,
    },
    Exclusion {
        name: String,
        #[serde(default = "default_exclusion_access_method")]
        access_method: String,
        equal_columns: Vec<String>,
        #[serde(default)]
        range: Option<ExclusionRangeSchema>,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default = "default_true")]
        validated: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExclusionRangeSchema {
    function: String,
    #[serde(default = "default_exclusion_range_operator")]
    operator: String,
    start_column: String,
    end_column: String,
    bounds: String,
    #[serde(default)]
    range_column: Option<String>,
}

fn default_exclusion_access_method() -> String {
    "gist".to_string()
}

fn default_exclusion_range_operator() -> String {
    "&&".to_string()
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Clone, Debug)]
struct InboundForeignKeyUpdate {
    child_schema: TableSchema,
    name: String,
    columns: Vec<String>,
    referred_columns: Vec<String>,
    on_update: ForeignKeyAction,
}

/// A foreign key whose parent is the deleted table (the DELETE twin of
/// `InboundForeignKeyUpdate`), memoized per table by `catalog_memo`.
#[derive(Clone, Debug)]
struct InboundForeignKeyDelete {
    child_schema: TableSchema,
    name: String,
    columns: Vec<String>,
    referred_columns: Vec<String>,
    on_delete: ForeignKeyAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RoleSchema {
    name: String,
    #[serde(default)]
    superuser: bool,
    #[serde(default = "default_true")]
    inherit: bool,
    #[serde(default)]
    create_role: bool,
    #[serde(default)]
    create_db: bool,
    #[serde(default)]
    can_login: bool,
    #[serde(default)]
    replication: bool,
    #[serde(default)]
    bypass_rls: bool,
    #[serde(default = "default_connection_limit")]
    connection_limit: i64,
    #[serde(default)]
    password_set: bool,
    #[serde(default)]
    valid_until: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PrivilegeGrant {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    column: Option<String>,
    object_type: PrivilegeObjectType,
    object_name: String,
    grantee: String,
    privilege: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DefaultPrivilegeGrant {
    grantor: String,
    schema_name: String,
    object_type: PrivilegeObjectType,
    grantee: String,
    privilege: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RoleMembership {
    role: String,
    member: String,
    grantor: String,
    #[serde(default)]
    admin_option: bool,
    /// None preserves the pre-option metadata's role-level inheritance rule.
    #[serde(default)]
    inherit_option: Option<bool>,
    #[serde(default = "default_true")]
    set_option: bool,
}

#[derive(Clone, Debug)]
enum RawRoleMembershipDdl {
    Grant {
        roles: Vec<String>,
        members: Vec<String>,
        admin_option: Option<bool>,
        inherit_option: Option<bool>,
        set_option: Option<bool>,
    },
    Revoke {
        roles: Vec<String>,
        members: Vec<String>,
    },
}

#[derive(Clone, Debug)]
pub enum DatabaseDdl {
    Create { name: String, owner: Option<String> },
    AlterOwner { name: String, owner: String },
    SetTablespace { name: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceSchema {
    name: String,
    #[serde(default = "current_role_name")]
    owner: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DatabaseSchema {
    name: String,
    owner: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExtensionSchema {
    name: String,
    schema: String,
    version: Option<String>,
}

const FIRST_USER_TYPE_OID: i64 = 800_000;
const USER_TYPE_OID_LIMIT: i64 = 1_100_000_000;
const USER_TYPE_OID_ALLOCATOR_VERSION: u32 = 1;
const DOMAIN_NOT_NULL_MARKER: &str = "__bicdb_domain_not_null";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct UserTypeSchema {
    name: String,
    schema_name: String,
    owner: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    acl_explicit: bool,
    oid: i64,
    array_oid: i64,
    kind: UserTypeKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct UserTypeColumnSchema {
    oid: i64,
    array_oid: i64,
    schema_name: String,
    name: String,
    array: bool,
    kind: UserTypeKind,
}

impl UserTypeColumnSchema {
    fn type_oid(&self) -> i64 {
        if self.array {
            self.array_oid
        } else {
            self.oid
        }
    }

    fn formatted_name(&self) -> String {
        let suffix = if self.array { "[]" } else { "" };
        if self.schema_name == "public" {
            format!("{}{suffix}", self.name)
        } else {
            format!("{}.{}{suffix}", self.schema_name, self.name)
        }
    }

    fn information_schema_scalar_data_type(&self) -> String {
        match &self.kind {
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::information_schema_scalar_data_type)
                .unwrap_or_else(|| information_schema_builtin_data_type(base_type)),
            _ => "USER-DEFINED".to_string(),
        }
    }

    fn information_schema_scalar_udt_name(&self) -> String {
        match &self.kind {
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::information_schema_scalar_udt_name)
                .unwrap_or_else(|| base_type.clone()),
            _ => self.name.clone(),
        }
    }

    fn information_schema_scalar_udt_schema(&self) -> String {
        match &self.kind {
            UserTypeKind::Domain { base_user_type, .. } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::information_schema_scalar_udt_schema)
                .unwrap_or_else(|| "pg_catalog".to_string()),
            _ => self.schema_name.clone(),
        }
    }

    fn scalar_type_len(&self) -> i64 {
        match &self.kind {
            UserTypeKind::Shell => 4,
            UserTypeKind::Base {
                internal_length, ..
            } => *internal_length,
            UserTypeKind::Enum { .. } => 4,
            UserTypeKind::Composite { .. } => -1,
            UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => -1,
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::scalar_type_len)
                .unwrap_or_else(|| pg_type_len(base_type)),
        }
    }

    fn scalar_by_value(&self) -> bool {
        match &self.kind {
            UserTypeKind::Shell => true,
            UserTypeKind::Base {
                passed_by_value, ..
            } => *passed_by_value,
            UserTypeKind::Enum { .. } => true,
            UserTypeKind::Composite { .. } => false,
            UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => false,
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::scalar_by_value)
                .unwrap_or_else(|| pg_type_by_value(base_type)),
        }
    }

    fn scalar_align(&self) -> char {
        match &self.kind {
            UserTypeKind::Shell => 'i',
            UserTypeKind::Base { alignment, .. } => *alignment,
            UserTypeKind::Enum { .. } => 'i',
            UserTypeKind::Composite { .. } => 'd',
            UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => 'i',
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::scalar_align)
                .unwrap_or_else(|| pg_type_align(base_type)),
        }
    }

    fn scalar_storage(&self) -> char {
        match &self.kind {
            UserTypeKind::Shell => 'p',
            UserTypeKind::Base { storage, .. } => *storage,
            UserTypeKind::Enum { .. } => 'p',
            UserTypeKind::Composite { .. } => 'x',
            UserTypeKind::Range { .. } | UserTypeKind::Multirange { .. } => 'x',
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::scalar_storage)
                .unwrap_or_else(|| pg_type_storage(base_type)),
        }
    }

    fn scalar_collation_oid(&self) -> i64 {
        match &self.kind {
            UserTypeKind::Shell => 0,
            UserTypeKind::Base { collatable, .. } => {
                if *collatable {
                    100
                } else {
                    0
                }
            }
            UserTypeKind::Enum { .. } => 0,
            UserTypeKind::Composite { .. } => 0,
            UserTypeKind::Range { value, .. } | UserTypeKind::Multirange { value, .. } => value
                .collation
                .as_deref()
                .and_then(collation_oid)
                .unwrap_or(0),
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                collation,
                ..
            } => collation
                .as_deref()
                .and_then(collation_oid)
                .or_else(|| {
                    base_user_type
                        .as_deref()
                        .map(UserTypeColumnSchema::scalar_collation_oid)
                })
                .unwrap_or_else(|| type_collation_oid(base_type)),
        }
    }

    fn scalar_delimiter(&self) -> char {
        match &self.kind {
            UserTypeKind::Base { delimiter, .. } => *delimiter,
            UserTypeKind::Domain {
                base_type,
                base_user_type,
                ..
            } => base_user_type
                .as_deref()
                .map(UserTypeColumnSchema::scalar_delimiter)
                .unwrap_or_else(|| pg_type_delimiter(base_type).unwrap_or(',')),
            _ => ',',
        }
    }
}

impl UserTypeSchema {
    fn column_type(&self, array: bool) -> UserTypeColumnSchema {
        UserTypeColumnSchema {
            oid: self.oid,
            array_oid: self.array_oid,
            schema_name: self.schema_name.clone(),
            name: self.name.clone(),
            array,
            kind: self.kind.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UserTypeKind {
    Shell,
    Base {
        codec_type: String,
        input: String,
        output: String,
        receive: Option<String>,
        send: Option<String>,
        internal_length: i64,
        passed_by_value: bool,
        alignment: char,
        storage: char,
        category: char,
        preferred: bool,
        default_expr: Option<String>,
        element_type: Option<String>,
        delimiter: char,
        collatable: bool,
    },
    Enum {
        labels: Vec<EnumLabelSchema>,
    },
    Composite {
        relation_oid: i64,
        attributes: Vec<CompositeAttributeSchema>,
    },
    Domain {
        base_type: String,
        base_user_type: Option<Box<UserTypeColumnSchema>>,
        type_modifier: Option<PgTypeModifier>,
        collation: Option<String>,
        default_expr: Option<String>,
        not_null: bool,
        #[serde(default)]
        not_null_constraint_name: Option<String>,
        constraints: Vec<DomainConstraintSchema>,
    },
    Range {
        value: UserRangeValueSchema,
        multirange_schema_name: String,
        multirange_name: String,
        multirange_oid: i64,
    },
    Multirange {
        value: UserRangeValueSchema,
        range_schema_name: String,
        range_name: String,
        range_oid: i64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct UserRangeValueSchema {
    subtype: String,
    #[serde(default)]
    subtype_user_type: Option<Box<UserTypeColumnSchema>>,
    subtype_oid: i64,
    subtype_opclass: String,
    subtype_opclass_oid: i64,
    #[serde(default)]
    collation: Option<String>,
    #[serde(default)]
    canonical: Option<String>,
    #[serde(default)]
    canonical_oid: i64,
    #[serde(default)]
    canonical_discrete: bool,
    #[serde(default)]
    subtype_diff: Option<String>,
    #[serde(default)]
    subtype_diff_oid: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CompositeAttributeSchema {
    name: String,
    pg_type: String,
    #[serde(default)]
    user_type: Option<UserTypeColumnSchema>,
    #[serde(default)]
    collation: Option<String>,
    #[serde(default)]
    type_modifier: Option<PgTypeModifier>,
    #[serde(default)]
    array_ndims: usize,
    #[serde(default)]
    dropped: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct DomainConstraintSchema {
    name: String,
    expression: String,
    #[serde(default = "default_true")]
    validated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct EnumLabelSchema {
    oid: i64,
    sort_order: f64,
    label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserTypeOidAllocator {
    #[serde(default)]
    version: u32,
    next_oid: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserTypeOidReservation {
    version: u32,
    oid: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PrivilegeObjectType {
    Database,
    Schema,
    Table,
    Function,
    Sequence,
    Type,
}

fn default_nullable() -> bool {
    true
}

fn default_true() -> bool {
    true
}

fn default_connection_limit() -> i64 {
    -1
}

fn default_schema_name() -> String {
    "public".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SequenceSchema {
    name: String,
    #[serde(default = "default_sequence_data_type")]
    data_type: String,
    increment_by: i64,
    min_value: i64,
    max_value: i64,
    start_value: i64,
    cache_size: i64,
    cycle: bool,
    last_value: i64,
    is_called: bool,
    #[serde(default = "default_sequence_owner")]
    owner: String,
    #[serde(default)]
    owned_by_table: Option<String>,
    #[serde(default)]
    owned_by_column: Option<String>,
}

fn default_sequence_owner() -> String {
    BOOTSTRAP_ROLE_NAME.to_string()
}

fn default_sequence_data_type() -> String {
    "int8".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ViewSchema {
    name: String,
    query_sql: String,
    columns: Vec<ColumnSchema>,
    #[serde(default)]
    materialized: bool,
    #[serde(default)]
    indexes: Vec<IndexSchema>,
    // Owning role; None on views persisted before ownership tracking, which
    // are treated as owned by the bootstrap role.
    #[serde(default)]
    owner: Option<String>,
    // PostgreSQL's `WITH (security_invoker = true)`: underlying-table RLS is
    // checked as the querying user instead of the view owner.
    #[serde(default)]
    security_invoker: bool,
    // Parsed and persisted for catalog fidelity; BicDB's row filters already
    // run before user predicates, so it has no separate planner effect.
    #[serde(default)]
    security_barrier: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RoutineSchema {
    name: String,
    kind: RoutineKind,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    arg_types: Vec<RoutineTypeSchema>,
    return_type: String,
    #[serde(default)]
    return_type_modifier: Option<PgTypeModifier>,
    #[serde(default)]
    return_type_declaration: Option<String>,
    #[serde(default)]
    returns_set: bool,
    language: String,
    definition: String,
    /// Owning schema. Routines are KEYED by bare name on every path
    /// (`routine_key`), so the schema is carried as data — baking it into
    /// the key made AST-created functions unreachable from raw-parsed
    /// triggers. Empty on records written before this field existed;
    /// `routine_schema_name` falls back for those.
    #[serde(default)]
    schema: String,
    #[serde(default)]
    internal_symbol: Option<String>,
    // Owning role; `None` on routines persisted before ownership tracking.
    // Read through `RoutineSchema::owner()` for ownership and display, where an
    // unrecorded owner is treated as bootstrap-owned (fail-closed), and through
    // `RoutineSchema::definer_owner()` for SECURITY DEFINER execution, where an
    // unrecorded owner must confer NO identity.
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    security_definer: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RoutineTypeSchema {
    pg_type: String,
    #[serde(default)]
    type_modifier: Option<PgTypeModifier>,
}

impl RoutineTypeSchema {
    fn catalog_oid(&self) -> i64 {
        pg_type_oid(&self.pg_type)
    }

    fn catalog_typmod(&self) -> i64 {
        self.type_modifier
            .as_ref()
            .map(PgTypeModifier::catalog_value)
            .unwrap_or(-1)
    }

    fn formatted(&self) -> String {
        pg_format_type(self.catalog_oid() as i32, self.catalog_typmod() as i32)
            .unwrap_or_else(|| self.pg_type.clone())
    }
}

impl RoutineSchema {
    /// The owning role for OWNERSHIP and display. A routine persisted before
    /// ownership tracking records no owner and is treated as bootstrap-owned,
    /// so only a superuser may alter it — the fail-closed reading.
    fn owner(&self) -> &str {
        self.owner.as_deref().unwrap_or(BOOTSTRAP_ROLE_NAME)
    }

    /// The identity a SECURITY DEFINER body runs as, if one was recorded.
    ///
    /// Deliberately NOT the same default as [`RoutineSchema::owner`]. The owner
    /// field used to default to the bootstrap role for both readings, which is
    /// fail-closed for ownership but fail-OPEN here: a routine with no recorded
    /// owner executed its body as a superuser. `None` means invoker semantics —
    /// absence of a recorded owner confers no authority.
    fn definer_owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    fn with_legacy_security_metadata(mut self) -> Self {
        if !self.security_definer && routine_definition_is_security_definer(&self.definition) {
            self.security_definer = true;
        }
        self
    }

    fn formatted_return_type(&self) -> String {
        if pg_type_oid_by_name(&self.return_type).is_none() {
            return self
                .return_type_declaration
                .clone()
                .unwrap_or_else(|| self.return_type.clone());
        }
        RoutineTypeSchema {
            pg_type: self.return_type.clone(),
            type_modifier: self.return_type_modifier.clone(),
        }
        .formatted()
    }
}

fn routine_definition_is_security_definer(definition: &str) -> bool {
    definition
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>()
        .windows(2)
        .any(|tokens| tokens == ["security", "definer"])
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum RoutineKind {
    Function,
    Procedure,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RoutineIrCacheKey {
    kind: RoutineKind,
    name: String,
    args_hash: u64,
    args_len: usize,
    return_type: String,
    returns_set: bool,
    language: String,
    definition_hash: u64,
    definition_len: usize,
}

thread_local! {
    static ROUTINE_IR_CACHE: RefCell<FxHashMap<RoutineIrCacheKey, Arc<RoutineIR>>> = RefCell::new(FxHashMap::default());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoutineArgMode {
    In,
    Out,
    InOut,
}

#[derive(Clone, Debug)]
struct RoutineParam {
    type_schema: Option<RoutineTypeSchema>,
    name: Option<String>,
    mode: RoutineArgMode,
    index: usize,
    default_expr: Option<Expr>,
}

#[derive(Clone, Debug)]
struct RoutineExpr {
    expr: Expr,
    bound: Option<BoundExpr>,
    /// Hoisted non-builtin calls the bound tree depends on (empty for a plain
    /// bound expression); see `BoundUserCall`.
    user_calls: Vec<BoundUserCall>,
}

impl RoutineExpr {
    fn unbound(expr: Expr) -> Self {
        Self {
            expr,
            bound: None,
            user_calls: Vec::new(),
        }
    }

    fn bind(&mut self, scope: &BoundExprScope) {
        match scope.bind_with_user_calls(&self.expr) {
            Some((bound, user_calls)) => {
                self.bound = Some(bound);
                self.user_calls = user_calls;
            }
            None => {
                self.bound = None;
                self.user_calls.clear();
            }
        }
    }
}

impl fmt::Display for RoutineExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

#[derive(Clone, Debug)]
enum RoutineDecl {
    Alias {
        name: String,
        position: usize,
    },
    Variable {
        name: String,
        pg_type: Option<String>,
        type_modifier: Option<PgTypeModifier>,
        default_expr: Option<RoutineExpr>,
    },
    Cursor {
        name: String,
        query: Query,
    },
}

#[derive(Clone, Debug)]
enum RoutineAssignmentTarget {
    Variable(String),
    ArrayElement {
        array_name: String,
        index: RoutineExpr,
    },
}

#[derive(Clone, Debug)]
enum RoutineStmt {
    ContinueLoop,
    Null,
    Assignment {
        target: RoutineAssignmentTarget,
        expr: RoutineExpr,
    },
    SelectInto {
        query: Query,
        targets: Vec<String>,
        strict: bool,
    },
    /// `PERFORM expr` — evaluate a query for its side effects and discard the
    /// rows. Sets FOUND from whether any row came back, exactly as PostgreSQL
    /// does; trigger bodies use it to invoke check functions.
    Perform {
        query: Query,
    },
    Sql(Statement),
    SqlInto {
        statement: Statement,
        targets: Vec<String>,
    },
    DynamicExecute(RoutineExpr),
    If {
        condition: RoutineExpr,
        then_body: Vec<RoutineStmt>,
        else_body: Vec<RoutineStmt>,
    },
    ForLoop {
        iterator: String,
        lower: RoutineExpr,
        upper: RoutineExpr,
        body: Vec<RoutineStmt>,
    },
    ForeachLoop {
        target: String,
        slice: usize,
        array: RoutineExpr,
        body: Vec<RoutineStmt>,
    },
    QueryForLoop {
        target: String,
        query: Query,
        body: Vec<RoutineStmt>,
    },
    OpenCursor {
        name: String,
    },
    FetchCursor {
        name: String,
        targets: Vec<String>,
    },
    CloseCursor {
        name: String,
    },
    RaiseException {
        message: String,
        arguments: Vec<RoutineExpr>,
        detail: Option<RoutineExpr>,
        sqlstate: String,
    },
    ReturnQuery(Query),
    Return(Option<RoutineExpr>),
}

#[derive(Clone, Debug)]
enum RoutineControl {
    NextIteration,
    Continue,
    Return(Option<SqlValue>),
}

#[derive(Clone, Debug)]
struct RoutineExceptionHandler {
    conditions: Vec<RoutineExceptionCondition>,
    statements: Vec<RoutineStmt>,
}

#[derive(Clone, Debug)]
enum RoutineExceptionCondition {
    Others,
    SqlState(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlpgsqlIfBranch {
    Elsif,
    Else,
}

#[derive(Clone, Debug)]
struct RoutineIR {
    params: Vec<RoutineParam>,
    symbol_names: Vec<String>,
    declarations: Vec<RoutineDecl>,
    statements: Vec<RoutineStmt>,
    exception_handlers: Vec<RoutineExceptionHandler>,
}

fn routine_block_may_write(statements: &[RoutineStmt]) -> bool {
    statements.iter().any(routine_statement_may_write)
}

fn routine_handlers_may_write(handlers: &[RoutineExceptionHandler]) -> bool {
    handlers
        .iter()
        .any(|handler| routine_block_may_write(&handler.statements))
}

fn routine_declarations_may_write(declarations: &[RoutineDecl]) -> bool {
    declarations.iter().any(|declaration| match declaration {
        RoutineDecl::Cursor { query, .. } => query_may_write(query),
        RoutineDecl::Alias { .. } | RoutineDecl::Variable { .. } => false,
    })
}

fn routine_statement_may_write(statement: &RoutineStmt) -> bool {
    match statement {
        RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
            statement_may_write(statement)
        }
        RoutineStmt::DynamicExecute(_) => true,
        RoutineStmt::SelectInto { query, .. } => query_may_write(query),
        RoutineStmt::Perform { query } | RoutineStmt::ReturnQuery(query) => query_may_write(query),
        RoutineStmt::QueryForLoop { query, body, .. } => {
            query_may_write(query) || routine_block_may_write(body)
        }
        RoutineStmt::If {
            then_body,
            else_body,
            ..
        } => routine_block_may_write(then_body) || routine_block_may_write(else_body),
        RoutineStmt::ForLoop { body, .. } | RoutineStmt::ForeachLoop { body, .. } => {
            routine_block_may_write(body)
        }
        RoutineStmt::ContinueLoop
        | RoutineStmt::Null
        | RoutineStmt::Assignment { .. }
        | RoutineStmt::OpenCursor { .. }
        | RoutineStmt::FetchCursor { .. }
        | RoutineStmt::CloseCursor { .. }
        | RoutineStmt::RaiseException { .. }
        | RoutineStmt::Return(_) => false,
    }
}

fn statement_may_write(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_may_write(query),
        Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Truncate(_)
        | Statement::Call(_)
        | Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::CreateSequence { .. }
        | Statement::CreateDomain(_)
        | Statement::CreateType { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateExtension(_)
        | Statement::CreateIndex(_)
        | Statement::CreateFunction(_)
        | Statement::CreateProcedure { .. }
        | Statement::CreateTrigger(_)
        | Statement::CreateRole(_)
        | Statement::Drop { .. }
        | Statement::DropFunction(_)
        | Statement::DropProcedure { .. }
        | Statement::DropTrigger(_)
        | Statement::AlterTable(_)
        | Statement::AlterFunction(_) => true,
        _ => false,
    }
}

fn query_has_row_locks(query: &Query) -> bool {
    use sqlparser::ast::{Visit, Visitor};
    use std::ops::ControlFlow;
    struct Locks;
    impl Visitor for Locks {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if query.locks.is_empty() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }
    matches!(query.visit(&mut Locks), ControlFlow::Break(()))
}

fn query_may_write(query: &Query) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| query_may_write(&cte.query))
    }) || set_expr_may_write(query.body.as_ref())
}

fn set_expr_may_write(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) => true,
        SetExpr::Query(query) => query_may_write(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_may_write(left) || set_expr_may_write(right)
        }
        _ => false,
    }
}

#[derive(Clone, Debug)]
struct RoutineFrame {
    assignment_types: Arc<FxHashMap<String, RoutineTypeSchema>>,
    values: Arc<BTreeMap<String, SqlValue>>,
    // Shared with the engine while an embedded statement runs (see
    // `RoutineSlotBinding`): the statement's bound expressions read the slots
    // in place. Writes go through `Rc::make_mut`, which copies only while a
    // statement still holds the previous snapshot.
    slot_values: std::rc::Rc<Vec<SqlValue>>,
    // Shared with the routine's frame template: the slot layout is fixed for
    // the life of the compiled routine, so a nested call clones two Arcs
    // instead of rebuilding the name map.
    slot_names: Arc<Vec<String>>,
    slot_ids: Arc<FxHashMap<String, VarId>>,
    values_dirty: bool,
    // Per-slot dirty flags so sync_values only re-materializes the variables
    // that actually changed since the last sync, instead of re-cloning every
    // variable into the string-keyed map on every embedded SQL statement.
    dirty_slots: Vec<bool>,
    full_resync: bool,
    positional: Vec<SqlValue>,
    output_names: Vec<String>,
    cursors: BTreeMap<String, RoutineCursor>,
    returned_set: Option<SqlResult>,
}

/// The per-routine part of a `RoutineFrame`: slot layout, positional keys and
/// the parameter binding plan. Built once per compiled routine and reused by
/// every call (`RoutineFrame::from_template`); a TPC-C NEWORD makes ~40
/// nested `DBMS_RANDOM` calls, each of which used to rebuild all of this.
#[derive(Clone, Debug)]
pub(crate) struct RoutineFrameTemplate {
    assignment_types: Arc<FxHashMap<String, RoutineTypeSchema>>,
    slot_names: Arc<Vec<String>>,
    slot_ids: Arc<FxHashMap<String, VarId>>,
    params: Vec<RoutineFrameTemplateParam>,
    output_names: Vec<String>,
}

#[derive(Clone, Debug)]
struct RoutineFrameTemplateParam {
    positional_key: String,
    name: Option<String>,
    mode: RoutineArgMode,
    default_expr: Option<Expr>,
}

#[derive(Clone, Debug)]
struct RoutineCursor {
    query: Query,
    rows: Vec<Vec<SqlValue>>,
    position: usize,
    open: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TriggerEnabledMode {
    #[default]
    Origin,
    Disabled,
    Replica,
    Always,
}

impl TriggerEnabledMode {
    fn pg_code(self) -> &'static str {
        match self {
            Self::Origin => "O",
            Self::Disabled => "D",
            Self::Replica => "R",
            Self::Always => "A",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TriggerSchema {
    name: String,
    table_name: String,
    function_name: String,
    /// Text arguments supplied to `EXECUTE FUNCTION`. PostgreSQL exposes
    /// these through zero-based `TG_ARGV` and `TG_NARGS`.
    #[serde(default)]
    arguments: Vec<String>,
    definition: String,
    /// Lower-cased event list as authored: `"insert"`, `"update"`, `"delete"`,
    /// or a multi-event form like `"insert or update"`. Match with
    /// [`TriggerSchema::fires_on`], never with `==`.
    #[serde(default)]
    event: String,
    #[serde(default)]
    timing: String,
    #[serde(default)]
    for_each: String,
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    enabled_mode: TriggerEnabledMode,
    /// CREATE CONSTRAINT TRIGGER. Constraint triggers are always AFTER ... FOR
    /// EACH ROW and may defer to commit.
    #[serde(default)]
    is_constraint: bool,
    /// DEFERRABLE INITIALLY DEFERRED: the trigger queues on the transaction
    /// and fires at COMMIT, which is what lets a multi-statement write (a
    /// journal entry and its lines) be checked as a whole.
    #[serde(default)]
    initially_deferred: bool,
}

impl TriggerSchema {
    /// Whether this trigger covers `op` ("insert" | "update" | "delete").
    /// The stored `event` is either a single event or an `or`-joined list.
    fn fires_on(&self, op: &str) -> bool {
        self.event
            .split(" or ")
            .any(|event| event.trim().eq_ignore_ascii_case(op))
    }

    fn set_enabled_mode(&mut self, mode: TriggerEnabledMode) {
        self.enabled = mode != TriggerEnabledMode::Disabled;
        self.enabled_mode = mode;
    }

    fn pg_enabled_code(&self) -> &'static str {
        if self.enabled {
            self.enabled_mode.pg_code()
        } else {
            TriggerEnabledMode::Disabled.pg_code()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NotificationRecord {
    id: i64,
    channel: String,
    payload: String,
    table_name: String,
    trigger_name: String,
    function_name: String,
    created_at: i64,
}

impl SequenceSchema {
    fn new_typed(name: String, data_type: &str, increment_by: i64) -> Self {
        let data_type = canonical_sequence_data_type(data_type).to_string();
        let (type_min, type_max) = sequence_type_limits(&data_type);
        let (min_value, max_value, start_value) = if increment_by >= 0 {
            (1, type_max, 1)
        } else {
            (type_min, -1, -1)
        };
        Self {
            name,
            data_type,
            increment_by,
            min_value,
            max_value,
            start_value,
            cache_size: 1,
            cycle: false,
            last_value: start_value,
            is_called: false,
            owner: current_role_name(),
            owned_by_table: None,
            owned_by_column: None,
        }
    }
}

fn canonical_sequence_data_type(data_type: &str) -> &'static str {
    match data_type.to_ascii_lowercase().as_str() {
        "smallint" | "int2" => "int2",
        "integer" | "int" | "int4" => "int4",
        _ => "int8",
    }
}

fn sequence_type_limits(data_type: &str) -> (i64, i64) {
    match canonical_sequence_data_type(data_type) {
        "int2" => (i16::MIN as i64, i16::MAX as i64),
        "int4" => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    }
}

fn sequence_data_type_name(data_type: &str) -> &'static str {
    match canonical_sequence_data_type(data_type) {
        "int2" => "smallint",
        "int4" => "integer",
        _ => "bigint",
    }
}

impl TableSchema {
    fn row_type_oid(&self) -> i64 {
        self.row_type_oid
            .unwrap_or_else(|| table_row_type_oid(namespace_oid(&self.schema_name), &self.name))
    }

    fn row_array_type_oid(&self) -> i64 {
        self.row_array_type_oid.unwrap_or_else(|| {
            table_row_array_type_oid(namespace_oid(&self.schema_name), &self.name)
        })
    }

    fn primary_key_column(&self) -> Option<&ColumnSchema> {
        self.columns.iter().find(|column| column.primary_key)
    }

    fn primary_key_constraint_name(&self) -> String {
        self.primary_key_name
            .clone()
            .unwrap_or_else(|| default_primary_key_name(&self.name))
    }

    fn primary_key(&self) -> &str {
        self.primary_key_column()
            .map(|column| column.name.as_str())
            .unwrap_or("id")
    }

    fn has_hidden_primary_key(&self) -> bool {
        self.primary_key_column()
            .is_some_and(|column| column.hidden)
    }

    fn column(&self, name: &str) -> Option<&ColumnSchema> {
        // AST identifiers are folded at the materialization boundary. Matching
        // persisted names exactly keeps quoted column identities distinct.
        self.columns.iter().find(|column| column.name == name)
    }

    fn add_column(&mut self, column: ColumnSchema) {
        if self.column(&column.name).is_none() {
            self.columns.push(column);
        }
    }
}

fn default_primary_key_name(table: &str) -> String {
    // Built from the PHYSICAL relation name on purpose: index names share one
    // namespace, so two tables called `users` in different schemas must not
    // both derive `users_pkey`. Catalogs decode this for display.
    format!("{table}_pkey")
}

/// Install or reconcile the typed SQL projection for a collection owned by an
/// embedded application runtime. Existing PostgreSQL-created schemas are
/// preserved and must be type-compatible; additive application fields are
/// appended without rewriting collection records.
pub fn ensure_embedded_table_schema(
    db: &mut BicDb,
    table: &str,
    columns: &[EmbeddedTableColumn],
) -> Result<Option<EmbeddedTableSchemaReceipt>> {
    if table.is_empty() || columns.is_empty() {
        return Err(SqlError::InvalidSql(
            "embedded table schemas require a table and at least one column".to_string(),
        ));
    }
    if !db
        .collections()
        .iter()
        .any(|collection| collection.name.eq_ignore_ascii_case(table))
    {
        return Err(SqlError::InvalidSql(format!(
            "embedded table schema `{table}` has no backing collection"
        )));
    }
    let primary_keys = columns.iter().filter(|column| column.primary_key).count();
    if primary_keys != 1 {
        return Err(SqlError::InvalidSql(format!(
            "embedded table schema `{table}` requires exactly one primary key"
        )));
    }
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.name.is_empty() || !seen.insert(column.name.to_ascii_lowercase()) {
            return Err(SqlError::InvalidSql(format!(
                "embedded table schema `{table}` has an empty or duplicate column"
            )));
        }
        if !matches!(
            column.pg_type.as_str(),
            "bool"
                | "int8"
                | "float8"
                | "numeric"
                | "text"
                | "bytea"
                | "uuid"
                | "timestamptz"
                | "date"
                | "jsonb"
                | "vector"
        ) {
            return Err(SqlError::Unsupported(format!(
                "embedded table schema `{table}` uses unsupported type `{}`",
                column.pg_type
            )));
        }
        if column.pg_type == "vector" && column.vector_dimensions.is_none() {
            return Err(SqlError::InvalidSql(format!(
                "embedded vector column `{}.{}` requires dimensions",
                table, column.name
            )));
        }
        if column.pg_type != "vector" && column.vector_dimensions.is_some() {
            return Err(SqlError::InvalidSql(format!(
                "non-vector embedded column `{}.{}` declares vector dimensions",
                table, column.name
            )));
        }
    }

    let previous = load_schema(db, table)?;
    let mut schema = previous.clone().unwrap_or_else(|| TableSchema {
        name: table.to_string(),
        schema_name: default_schema_name(),
        row_type_oid: Some(table_row_type_oid(
            namespace_oid(&default_schema_name()),
            table,
        )),
        row_array_type_oid: Some(table_row_array_type_oid(
            namespace_oid(&default_schema_name()),
            table,
        )),
        columns: Vec::new(),
        primary_key_name: None,
        indexes: Vec::new(),
        constraints: Vec::new(),
        rls_enabled: false,
        rls_forced: false,
        policies: Vec::new(),
        owner: Some(current_role_name()),
        partitioning: None,
        partition_of: None,
    });
    let mut changed = previous.is_none();
    for requested in columns {
        let vector_modifier = requested
            .vector_dimensions
            .map(u16::try_from)
            .transpose()
            .map_err(|_| {
                SqlError::InvalidSql(format!(
                    "embedded vector column `{}.{}` exceeds supported dimensions",
                    table, requested.name
                ))
            })?;
        let vector_dim = requested
            .vector_dimensions
            .map(|dimensions| dimensions as usize);
        if let Some(existing) = schema
            .columns
            .iter_mut()
            .find(|column| column.name.eq_ignore_ascii_case(&requested.name))
        {
            if existing.pg_type != requested.pg_type
                || existing.primary_key != requested.primary_key
                || existing.vector_dim != vector_dim
            {
                return Err(SqlError::InvalidSql(format!(
                    "embedded table column `{}.{}` conflicts with the existing SQL schema",
                    table, requested.name
                )));
            }
            if existing.nullable != requested.nullable {
                existing.nullable = requested.nullable;
                changed = true;
            }
            continue;
        }
        schema.columns.push(ColumnSchema {
            name: requested.name.clone(),
            pg_type: requested.pg_type.clone(),
            user_type: None,
            collation: None,
            type_modifier: vector_modifier.map(|dimensions| PgTypeModifier::Vector { dimensions }),
            array_ndims: 0,
            compression: None,
            primary_key: requested.primary_key,
            hidden: false,
            nullable: requested.nullable && !requested.primary_key,
            vector_dim,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        });
        changed = true;
    }
    let requested_primary_key = columns
        .iter()
        .find(|column| column.primary_key)
        .expect("validated primary key");
    if schema.primary_key_name.is_none() {
        schema.primary_key_name = Some(default_primary_key_name(table));
        changed = true;
    }
    if schema.primary_key() != requested_primary_key.name {
        return Err(SqlError::InvalidSql(format!(
            "embedded table `{table}` primary key conflicts with the existing SQL schema"
        )));
    }
    if !changed {
        return Ok(None);
    }
    save_schema(db, &schema)?;
    Ok(Some(EmbeddedTableSchemaReceipt {
        table: table.to_string(),
        previous,
    }))
}

const EMBEDDED_POLICY_PREFIX: &str = "__carrier_embedded_";
const EMBEDDED_POLICY_STATE_PREFIX: &str = "__carrier_embedded_select_admit_state_e";

fn embedded_policy_state_name(rls_enabled: bool, rls_forced: bool) -> String {
    format!(
        "{EMBEDDED_POLICY_STATE_PREFIX}{}_f{}",
        u8::from(rls_enabled),
        u8::from(rls_forced),
    )
}

fn embedded_policy_previous_state(schema: &TableSchema) -> Option<(bool, bool)> {
    schema.policies.iter().find_map(|policy| {
        let encoded = policy.name.strip_prefix(EMBEDDED_POLICY_STATE_PREFIX)?;
        let (enabled, forced) = encoded.split_once("_f")?;
        match (enabled, forced) {
            ("0", "0") => Some((false, false)),
            ("0", "1") => Some((false, true)),
            ("1", "0") => Some((true, false)),
            ("1", "1") => Some((true, true)),
            _ => None,
        }
    })
}

fn embedded_policy_schema(
    name: &str,
    command: PolicyCommand,
    using_expr: Option<String>,
    check_expr: Option<String>,
    permissive: bool,
) -> PolicySchema {
    PolicySchema {
        name: name.to_string(),
        command,
        using_expr,
        check_expr,
        permissive,
        enforced: true,
        roles: default_policy_roles(),
    }
}

/// Reconcile the compiler-owned row policies for an embedded relation.
///
/// Guard policies are restrictive and paired with compiler-owned permissive
/// admission policies. Consequently a pre-existing permissive policy cannot
/// widen BicDB application authority, while unrelated restrictive PostgreSQL policies
/// can still narrow it. The returned receipt restores the exact prior schema
/// during failed activation or upgrade compensation.
pub fn reconcile_embedded_table_row_policy(
    db: &mut BicDb,
    table: &str,
    policy: Option<&EmbeddedTableRowPolicy>,
) -> Result<EmbeddedTableSchemaReceipt> {
    let previous = load_schema(db, table)?.ok_or_else(|| {
        SqlError::InvalidCollection(format!(
            "embedded row policy `{table}` has no SQL table schema"
        ))
    })?;
    if let Some(policy) = policy {
        for (command, using_expr, check_expr) in [
            (
                PolicyCommand::Select,
                Some(policy.select_using.as_str()),
                None,
            ),
            (
                PolicyCommand::Insert,
                None,
                Some(policy.insert_with_check.as_str()),
            ),
            (
                PolicyCommand::Update,
                Some(policy.update_using.as_str()),
                Some(policy.update_with_check.as_str()),
            ),
            (
                PolicyCommand::Delete,
                Some(policy.delete_using.as_str()),
                None,
            ),
        ] {
            validate_policy_clauses(command, using_expr.is_some(), check_expr.is_some())?;
            for expression in [using_expr, check_expr].into_iter().flatten() {
                let parsed = parse_policy_expression(expression)?;
                validate_policy_expression_columns(&previous, &parsed)?;
            }
        }
    }

    let mut schema = previous.clone();
    let previous_policy_state = embedded_policy_previous_state(&schema);
    let had_compiler_policy = schema
        .policies
        .iter()
        .any(|candidate| candidate.name.starts_with(EMBEDDED_POLICY_PREFIX));
    schema
        .policies
        .retain(|candidate| !candidate.name.starts_with(EMBEDDED_POLICY_PREFIX));
    if let Some(policy) = policy {
        let (prior_rls_enabled, prior_rls_forced) =
            previous_policy_state.unwrap_or((schema.rls_enabled, schema.rls_forced));
        schema.rls_enabled = true;
        schema.rls_forced = true;
        for (suffix, command, using_expr, check_expr) in [
            (
                "select",
                PolicyCommand::Select,
                Some(policy.select_using.clone()),
                None,
            ),
            (
                "insert",
                PolicyCommand::Insert,
                None,
                Some(policy.insert_with_check.clone()),
            ),
            (
                "update",
                PolicyCommand::Update,
                Some(policy.update_using.clone()),
                Some(policy.update_with_check.clone()),
            ),
            (
                "delete",
                PolicyCommand::Delete,
                Some(policy.delete_using.clone()),
                None,
            ),
        ] {
            let admit_name = if suffix == "select" {
                embedded_policy_state_name(prior_rls_enabled, prior_rls_forced)
            } else {
                format!("{EMBEDDED_POLICY_PREFIX}{suffix}_admit")
            };
            schema.policies.push(embedded_policy_schema(
                &admit_name,
                command,
                using_expr.as_ref().map(|_| "TRUE".to_string()),
                check_expr.as_ref().map(|_| "TRUE".to_string()),
                true,
            ));
            schema.policies.push(embedded_policy_schema(
                &format!("{EMBEDDED_POLICY_PREFIX}{suffix}"),
                command,
                using_expr,
                check_expr,
                false,
            ));
        }
    } else if had_compiler_policy {
        if let Some((rls_enabled, rls_forced)) = previous_policy_state {
            schema.rls_enabled = rls_enabled;
            schema.rls_forced = rls_forced;
        } else if schema.policies.is_empty() {
            // Compatibility for an unreleased compiler-policy schema that did
            // not yet encode its prior RLS state.
            schema.rls_enabled = false;
            schema.rls_forced = false;
        }
    }
    save_schema(db, &schema)?;
    Ok(EmbeddedTableSchemaReceipt {
        table: table.to_string(),
        previous: Some(previous),
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cell_rows_tests;
