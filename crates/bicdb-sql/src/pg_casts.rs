//! PostgreSQL 18 cast catalog and coercion-context rules.

use crate::*;
use std::sync::LazyLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgCastContext {
    Explicit,
    Assignment,
    Implicit,
}

impl PgCastContext {
    fn from_catalog(value: &str) -> Option<Self> {
        match value {
            "e" => Some(Self::Explicit),
            "a" => Some(Self::Assignment),
            "i" => Some(Self::Implicit),
            _ => None,
        }
    }

    pub(crate) fn catalog_code(self) -> char {
        match self {
            Self::Explicit => 'e',
            Self::Assignment => 'a',
            Self::Implicit => 'i',
        }
    }

    fn allows(self, requested: Self) -> bool {
        match requested {
            Self::Explicit => true,
            Self::Assignment => matches!(self, Self::Assignment | Self::Implicit),
            Self::Implicit => self == Self::Implicit,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PgCastMethod {
    Function,
    Binary,
    InOut,
}

impl PgCastMethod {
    fn from_catalog(value: &str) -> Option<Self> {
        match value {
            "f" => Some(Self::Function),
            "b" => Some(Self::Binary),
            "i" => Some(Self::InOut),
            _ => None,
        }
    }

    pub(crate) fn catalog_code(self) -> char {
        match self {
            Self::Function => 'f',
            Self::Binary => 'b',
            Self::InOut => 'i',
        }
    }

    fn cost(self) -> u16 {
        match self {
            Self::Binary => 0,
            Self::Function => 1,
            Self::InOut => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PgCastSpec {
    pub oid: i64,
    pub source: &'static str,
    pub target: &'static str,
    pub function_oid: i64,
    pub function_name: Option<&'static str>,
    pub context: PgCastContext,
    pub method: PgCastMethod,
}

static PG_CAST_SPECS: LazyLock<Vec<PgCastSpec>> = LazyLock::new(|| {
    include_str!("pg_casts.tsv")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(fields.len(), 7, "invalid pg_cast compatibility row: {line}");
            PgCastSpec {
                oid: fields[0].parse().expect("pg_cast OID"),
                source: fields[1],
                target: fields[2],
                function_oid: fields[3].parse().expect("pg_cast function OID"),
                function_name: (!fields[4].is_empty()).then_some(fields[4]),
                context: PgCastContext::from_catalog(fields[5]).expect("pg_cast context"),
                method: PgCastMethod::from_catalog(fields[6]).expect("pg_cast method"),
            }
        })
        .collect()
});

pub(crate) fn pg_cast_specs() -> &'static [PgCastSpec] {
    PG_CAST_SPECS.as_slice()
}

fn canonical_cast_type(pg_type: &str) -> &str {
    pg_type_spec(pg_type)
        .map(|spec| spec.name)
        .unwrap_or(pg_type)
}

fn catalog_managed_cast_type(pg_type: &str) -> bool {
    let scalar = pg_type.strip_suffix("[]").unwrap_or(pg_type);
    pg_type_spec(scalar).is_some()
}

pub(crate) fn pg_cast_spec(source: &str, target: &str) -> Option<&'static PgCastSpec> {
    let source = canonical_cast_type(source);
    let target = canonical_cast_type(target);
    pg_cast_specs()
        .iter()
        .find(|cast| cast.source == source && cast.target == target)
}

fn string_category_type(pg_type: &str) -> bool {
    matches!(pg_type, "text" | "varchar" | "bpchar" | "name" | "char")
}

/// Return the parser coercion cost when PostgreSQL permits this conversion in
/// the requested context. Binary-compatible casts are cheapest, then function
/// casts, then I/O casts. Identical types cost zero. Arrays recurse through the
/// element cast even though PostgreSQL does not materialize those rows in
/// `pg_cast`.
pub(crate) fn pg_cast_cost(source: &str, target: &str, requested: PgCastContext) -> Option<u16> {
    let source = canonical_cast_type(source);
    let target = canonical_cast_type(target);
    if source == target {
        return Some(0);
    }
    if let Some(spec) = pg_cast_spec(source, target) {
        return spec.context.allows(requested).then_some(spec.method.cost());
    }
    if let (Some(source), Some(target)) = (source.strip_suffix("[]"), target.strip_suffix("[]")) {
        return pg_cast_cost(source, target, requested).map(|cost| cost.saturating_add(1));
    }

    // PostgreSQL's parser supports CoerceViaIO for explicit string casts even
    // when no pg_cast row exists. Assignment to a string destination is also
    // allowed, while parsing a string into another family remains explicit.
    if requested == PgCastContext::Explicit
        && (string_category_type(source) || string_category_type(target))
    {
        return Some(PgCastMethod::InOut.cost());
    }
    if requested == PgCastContext::Assignment && string_category_type(target) {
        return Some(PgCastMethod::InOut.cost());
    }
    None
}

pub(crate) fn pg_cast_allows(source: &str, target: &str, requested: PgCastContext) -> bool {
    pg_cast_cost(source, target, requested).is_some()
}

pub(crate) fn validate_catalog_cast(
    source: &str,
    target: &str,
    requested: PgCastContext,
) -> Result<()> {
    if !catalog_managed_cast_type(source) || !catalog_managed_cast_type(target) {
        return Ok(());
    }
    if pg_cast_allows(source, target, requested) {
        return Ok(());
    }
    Err(SqlError::cannot_coerce(format!(
        "cannot cast type {} to {}",
        pg_cast_display_type(source),
        pg_cast_display_type(target)
    )))
}

pub(crate) fn pg_cast_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let mut rows = pg_cast_specs()
        .iter()
        .map(|cast| {
            virtual_row([
                ("oid", SqlValue::Int(cast.oid)),
                (
                    "castsource",
                    SqlValue::Int(i64::from(
                        pg_type_oid_by_name(cast.source).expect("catalog cast source type"),
                    )),
                ),
                (
                    "casttarget",
                    SqlValue::Int(i64::from(
                        pg_type_oid_by_name(cast.target).expect("catalog cast target type"),
                    )),
                ),
                ("castfunc", SqlValue::Int(cast.function_oid)),
                (
                    "castcontext",
                    SqlValue::String(cast.context.catalog_code().to_string()),
                ),
                (
                    "castmethod",
                    SqlValue::String(cast.method.catalog_code().to_string()),
                ),
            ])
        })
        .collect::<Vec<_>>();

    for range in list_user_types(db)? {
        let UserTypeKind::Range {
            multirange_schema_name,
            multirange_name,
            multirange_oid,
            ..
        } = &range.kind
        else {
            continue;
        };
        let signature = format!("{multirange_schema_name}.{multirange_name}({})", range.oid);
        rows.push(virtual_row([
            (
                "oid",
                SqlValue::Int(stable_name_hash_wide(
                    2_000_000_000,
                    &format!("pg_cast:{signature}"),
                    100_000_000,
                )),
            ),
            ("castsource", SqlValue::Int(range.oid)),
            ("casttarget", SqlValue::Int(*multirange_oid)),
            (
                "castfunc",
                SqlValue::Int(routine_oid(RoutineKind::Function, &signature)),
            ),
            ("castcontext", SqlValue::String("e".to_string())),
            ("castmethod", SqlValue::String("f".to_string())),
        ]));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_cast_catalog_is_complete_and_unique() {
        assert_eq!(pg_cast_specs().len(), 235);
        let identities = pg_cast_specs()
            .iter()
            .map(|cast| (cast.source, cast.target))
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), pg_cast_specs().len());
        for cast in pg_cast_specs() {
            assert!(
                pg_type_oid_by_name(cast.source).is_some(),
                "missing cast source type {}",
                cast.source
            );
            assert!(
                pg_type_oid_by_name(cast.target).is_some(),
                "missing cast target type {}",
                cast.target
            );
        }
    }

    #[test]
    fn coercion_context_and_cost_follow_catalog_rules() {
        assert_eq!(
            pg_cast_cost("int2", "int8", PgCastContext::Implicit),
            Some(1)
        );
        assert_eq!(pg_cast_cost("int8", "int2", PgCastContext::Implicit), None);
        assert_eq!(
            pg_cast_cost("int8", "int2", PgCastContext::Assignment),
            Some(1)
        );
        assert_eq!(
            pg_cast_cost("int4", "oid", PgCastContext::Implicit),
            Some(0)
        );
        assert_eq!(
            pg_cast_cost("uuid", "text", PgCastContext::Explicit),
            Some(2)
        );
        assert_eq!(
            pg_cast_cost("int2[]", "int8[]", PgCastContext::Implicit),
            Some(2)
        );
        assert!(validate_catalog_cast("uuid", "int4", PgCastContext::Explicit).is_err());
        assert!(validate_catalog_cast("uuid", "text", PgCastContext::Explicit).is_ok());
    }
}
