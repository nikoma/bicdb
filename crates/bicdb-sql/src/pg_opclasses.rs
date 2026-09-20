//! PostgreSQL 18 built-in operator classes and default-selection rules.

use crate::*;
use std::sync::LazyLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PgOpclassSpec {
    pub oid: i64,
    pub method_oid: i64,
    pub name: &'static str,
    pub namespace_oid: i64,
    pub owner_oid: i64,
    pub family_oid: i64,
    pub input_type_oid: i32,
    pub is_default: bool,
    pub key_type_oid: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgOpfamilySpec {
    oid: i64,
    method_oid: i64,
    name: &'static str,
    namespace_oid: i64,
    owner_oid: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgAmopSpec {
    oid: i64,
    family_oid: i64,
    left_type_oid: i64,
    right_type_oid: i64,
    strategy: i64,
    purpose: &'static str,
    operator_oid: i64,
    method_oid: i64,
    sort_family_oid: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PgAmprocSpec {
    oid: i64,
    family_oid: i64,
    left_type_oid: i64,
    right_type_oid: i64,
    procedure_number: i64,
    procedure_oid: i64,
}

static PG_OPCLASS_SPECS: LazyLock<Vec<PgOpclassSpec>> = LazyLock::new(|| {
    include_str!("pg_opclasses.tsv")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                9,
                "invalid pg_opclass compatibility row: {line}"
            );
            PgOpclassSpec {
                oid: fields[0].parse().expect("pg_opclass OID"),
                method_oid: fields[1].parse().expect("pg_opclass method OID"),
                name: fields[2],
                namespace_oid: fields[3].parse().expect("pg_opclass namespace OID"),
                owner_oid: fields[4].parse().expect("pg_opclass owner OID"),
                family_oid: fields[5].parse().expect("pg_opclass family OID"),
                input_type_oid: fields[6].parse().expect("pg_opclass input type OID"),
                is_default: fields[7] == "t",
                key_type_oid: fields[8].parse().expect("pg_opclass key type OID"),
            }
        })
        .collect()
});

static PG_OPFAMILY_SPECS: LazyLock<Vec<PgOpfamilySpec>> = LazyLock::new(|| {
    include_str!("pg_opfamilies.tsv")
        .lines()
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                5,
                "invalid pg_opfamily compatibility row: {line}"
            );
            PgOpfamilySpec {
                oid: fields[0].parse().expect("pg_opfamily OID"),
                method_oid: fields[1].parse().expect("pg_opfamily method OID"),
                name: fields[2],
                namespace_oid: fields[3].parse().expect("pg_opfamily namespace OID"),
                owner_oid: fields[4].parse().expect("pg_opfamily owner OID"),
            }
        })
        .collect()
});

static PG_AMOP_SPECS: LazyLock<Vec<PgAmopSpec>> = LazyLock::new(|| {
    include_str!("pg_amops.tsv")
        .lines()
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(fields.len(), 9, "invalid pg_amop compatibility row: {line}");
            PgAmopSpec {
                oid: fields[0].parse().expect("pg_amop OID"),
                family_oid: fields[1].parse().expect("pg_amop family OID"),
                left_type_oid: fields[2].parse().expect("pg_amop left type OID"),
                right_type_oid: fields[3].parse().expect("pg_amop right type OID"),
                strategy: fields[4].parse().expect("pg_amop strategy"),
                purpose: fields[5],
                operator_oid: fields[6].parse().expect("pg_amop operator OID"),
                method_oid: fields[7].parse().expect("pg_amop method OID"),
                sort_family_oid: fields[8].parse().expect("pg_amop sort family OID"),
            }
        })
        .collect()
});

static PG_AMPROC_SPECS: LazyLock<Vec<PgAmprocSpec>> = LazyLock::new(|| {
    include_str!("pg_amprocs.tsv")
        .lines()
        .map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                6,
                "invalid pg_amproc compatibility row: {line}"
            );
            PgAmprocSpec {
                oid: fields[0].parse().expect("pg_amproc OID"),
                family_oid: fields[1].parse().expect("pg_amproc family OID"),
                left_type_oid: fields[2].parse().expect("pg_amproc left type OID"),
                right_type_oid: fields[3].parse().expect("pg_amproc right type OID"),
                procedure_number: fields[4].parse().expect("pg_amproc procedure number"),
                procedure_oid: fields[5].parse().expect("pg_amproc procedure OID"),
            }
        })
        .collect()
});

pub(crate) fn pg_opclass_specs() -> &'static [PgOpclassSpec] {
    PG_OPCLASS_SPECS.as_slice()
}

pub(crate) fn pg_opclass_rows() -> Vec<BTreeMap<String, SqlValue>> {
    pg_opclass_specs()
        .iter()
        .map(|spec| {
            virtual_row([
                ("oid", SqlValue::Int(spec.oid)),
                ("opcmethod", SqlValue::Int(spec.method_oid)),
                ("opcname", SqlValue::String(spec.name.to_string())),
                ("opcnamespace", SqlValue::Int(spec.namespace_oid)),
                ("opcowner", SqlValue::Int(spec.owner_oid)),
                ("opcfamily", SqlValue::Int(spec.family_oid)),
                ("opcintype", SqlValue::Int(i64::from(spec.input_type_oid))),
                ("opcdefault", SqlValue::Bool(spec.is_default)),
                ("opckeytype", SqlValue::Int(i64::from(spec.key_type_oid))),
            ])
        })
        .collect()
}

pub(crate) fn pg_opfamily_rows() -> Vec<BTreeMap<String, SqlValue>> {
    PG_OPFAMILY_SPECS
        .iter()
        .map(|spec| {
            virtual_row([
                ("oid", SqlValue::Int(spec.oid)),
                ("opfmethod", SqlValue::Int(spec.method_oid)),
                ("opfname", SqlValue::String(spec.name.to_string())),
                ("opfnamespace", SqlValue::Int(spec.namespace_oid)),
                ("opfowner", SqlValue::Int(spec.owner_oid)),
            ])
        })
        .collect()
}

pub(crate) fn pg_amop_rows() -> Vec<BTreeMap<String, SqlValue>> {
    PG_AMOP_SPECS
        .iter()
        .map(|spec| {
            virtual_row([
                ("oid", SqlValue::Int(spec.oid)),
                ("amopfamily", SqlValue::Int(spec.family_oid)),
                ("amoplefttype", SqlValue::Int(spec.left_type_oid)),
                ("amoprighttype", SqlValue::Int(spec.right_type_oid)),
                ("amopstrategy", SqlValue::Int(spec.strategy)),
                ("amoppurpose", SqlValue::String(spec.purpose.to_string())),
                ("amopopr", SqlValue::Int(spec.operator_oid)),
                ("amopmethod", SqlValue::Int(spec.method_oid)),
                ("amopsortfamily", SqlValue::Int(spec.sort_family_oid)),
            ])
        })
        .collect()
}

pub(crate) fn pg_amproc_rows() -> Vec<BTreeMap<String, SqlValue>> {
    PG_AMPROC_SPECS
        .iter()
        .map(|spec| {
            virtual_row([
                ("oid", SqlValue::Int(spec.oid)),
                ("amprocfamily", SqlValue::Int(spec.family_oid)),
                ("amproclefttype", SqlValue::Int(spec.left_type_oid)),
                ("amprocrighttype", SqlValue::Int(spec.right_type_oid)),
                ("amprocnum", SqlValue::Int(spec.procedure_number)),
                ("amproc", SqlValue::Int(spec.procedure_oid)),
            ])
        })
        .collect()
}

fn builtin_opclass_input(pg_type: &str) -> Option<i32> {
    if pg_type.ends_with("[]") {
        return Some(2277);
    }
    if is_builtin_range_type(pg_type) {
        return Some(3831);
    }
    if is_builtin_multirange_type(pg_type) {
        return Some(4537);
    }
    match pg_type {
        // PostgreSQL indexes these binary-compatible types with the base
        // type's operator classes.
        "varchar" => Some(25),
        "cidr" => Some(869),
        _ => pg_type_oid_by_name(pg_type),
    }
}

fn user_type_opclass_input(user_type: &UserTypeColumnSchema) -> Option<i32> {
    if user_type.array {
        return Some(2277);
    }
    match &user_type.kind {
        UserTypeKind::Enum { .. } => Some(3500),
        UserTypeKind::Composite { .. } => Some(2249),
        UserTypeKind::Domain {
            base_type,
            base_user_type,
            ..
        } => base_user_type
            .as_deref()
            .and_then(user_type_opclass_input)
            .or_else(|| builtin_opclass_input(base_type)),
        UserTypeKind::Range { .. } => Some(3831),
        UserTypeKind::Multirange { .. } => Some(4537),
        UserTypeKind::Shell | UserTypeKind::Base { .. } => None,
    }
}

fn pg_opclass_input(db: &BicDb, pg_type: &str) -> Result<Option<i32>> {
    let canonical = pg_type_spec(pg_type)
        .map(|spec| spec.name)
        .unwrap_or(pg_type);
    if let Some(input) = builtin_opclass_input(canonical) {
        return Ok(Some(input));
    }
    let (base, array) = canonical
        .strip_suffix("[]")
        .map_or((canonical, false), |base| (base, true));
    let (schema_name, type_name) = base.rsplit_once('.').unwrap_or(("public", base));
    Ok(load_user_type(db, schema_name, type_name)?
        .map(|user_type| user_type.column_type(array))
        .as_ref()
        .and_then(user_type_opclass_input))
}

pub(crate) fn pg_default_opclass(
    access_method: &str,
    pg_type: &str,
) -> Option<&'static PgOpclassSpec> {
    let method_oid = access_method_oid(access_method);
    let canonical = pg_type_spec(pg_type)
        .map(|spec| spec.name)
        .unwrap_or(pg_type);
    let input_oid = builtin_opclass_input(canonical)?;
    pg_opclass_specs().iter().find(|spec| {
        spec.method_oid == method_oid && spec.is_default && spec.input_type_oid == input_oid
    })
}

pub(crate) fn pg_default_opclass_for_type(
    db: &BicDb,
    access_method: &str,
    pg_type: &str,
) -> Result<Option<&'static PgOpclassSpec>> {
    let Some(input_oid) = pg_opclass_input(db, pg_type)? else {
        return Ok(None);
    };
    let method_oid = access_method_oid(access_method);
    Ok(pg_opclass_specs().iter().find(|spec| {
        spec.method_oid == method_oid && spec.is_default && spec.input_type_oid == input_oid
    }))
}

pub(crate) fn pg_opclass_named_for_type(
    db: &BicDb,
    access_method: &str,
    pg_type: &str,
    name: &str,
) -> Result<Option<&'static PgOpclassSpec>> {
    let Some(input_oid) = pg_opclass_input(db, pg_type)? else {
        return Ok(None);
    };
    let method_oid = access_method_oid(access_method);
    let name = name.rsplit('.').next().unwrap_or(name);
    Ok(pg_opclass_specs().iter().find(|spec| {
        spec.method_oid == method_oid && spec.name == name && spec.input_type_oid == input_oid
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_operator_class_catalog_is_complete_and_unique() {
        assert_eq!(pg_opclass_specs().len(), 178);
        assert_eq!(
            pg_opclass_specs()
                .iter()
                .map(|spec| spec.oid)
                .collect::<BTreeSet<_>>()
                .len(),
            178
        );
    }

    #[test]
    fn postgres_operator_family_members_are_complete_and_unique() {
        assert_eq!(PG_OPFAMILY_SPECS.len(), 147);
        assert_eq!(PG_AMOP_SPECS.len(), 945);
        assert_eq!(PG_AMPROC_SPECS.len(), 714);
        for oids in [
            PG_OPFAMILY_SPECS
                .iter()
                .map(|spec| spec.oid)
                .collect::<Vec<_>>(),
            PG_AMOP_SPECS
                .iter()
                .map(|spec| spec.oid)
                .collect::<Vec<_>>(),
            PG_AMPROC_SPECS
                .iter()
                .map(|spec| spec.oid)
                .collect::<Vec<_>>(),
        ] {
            assert_eq!(oids.iter().collect::<BTreeSet<_>>().len(), oids.len());
        }
    }

    #[test]
    fn default_selection_follows_type_and_access_method() {
        assert_eq!(
            pg_default_opclass("btree", "uuid").unwrap().name,
            "uuid_ops"
        );
        assert_eq!(
            pg_default_opclass("hash", "numeric").unwrap().name,
            "numeric_ops"
        );
        assert_eq!(
            pg_default_opclass("gin", "text[]").unwrap().name,
            "array_ops"
        );
        assert_eq!(
            pg_default_opclass("gist", "int4range").unwrap().name,
            "range_ops"
        );
        assert_eq!(
            pg_default_opclass("btree", "varchar").unwrap().name,
            "text_ops"
        );
        assert!(pg_default_opclass("btree", "xml").is_none());
    }
}
