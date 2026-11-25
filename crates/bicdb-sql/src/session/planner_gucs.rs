//! PostgreSQL planner and resource settings that applications set per session
//! (`SET work_mem = '64MB'`, `SET enable_seqscan = off`, ...).
//!
//! BicDB's planner and executor do not read these knobs, so accepting them
//! changes nothing an application can observe beyond the setting itself:
//! the value is validated, normalised and reported exactly as PostgreSQL does
//! (`SHOW`, `current_setting`, `set_config`, `RESET`, `SET LOCAL`). Settings
//! whose semantics are durability or isolation guarantees are deliberately
//! absent: accepting them without honouring them would be wrong.

use crate::{Result, SqlError};

#[derive(Clone, Copy)]
enum Unit {
    None,
    /// Kilobytes (`work_mem`).
    Kb,
    /// 8 kB blocks (`effective_cache_size`).
    Blocks,
}

#[derive(Clone, Copy)]
enum Kind {
    Bool,
    Int { unit: Unit, min: i64, max: i64 },
    Real { min: f64, max: f64 },
    Enum(&'static [&'static str]),
}

struct PlannerGuc {
    name: &'static str,
    kind: Kind,
    default: &'static str,
}

const INT_MAX: i64 = i32::MAX as i64;

const fn boolean(name: &'static str, default: &'static str) -> PlannerGuc {
    PlannerGuc {
        name,
        kind: Kind::Bool,
        default,
    }
}

const fn integer(
    name: &'static str,
    unit: Unit,
    min: i64,
    max: i64,
    default: &'static str,
) -> PlannerGuc {
    PlannerGuc {
        name,
        kind: Kind::Int { unit, min, max },
        default,
    }
}

const fn real(name: &'static str, min: f64, max: f64, default: &'static str) -> PlannerGuc {
    PlannerGuc {
        name,
        kind: Kind::Real { min, max },
        default,
    }
}

const PLANNER_GUCS: &[PlannerGuc] = &[
    integer("work_mem", Unit::Kb, 64, INT_MAX, "4MB"),
    integer("maintenance_work_mem", Unit::Kb, 1024, INT_MAX, "64MB"),
    integer("effective_cache_size", Unit::Blocks, 1, INT_MAX, "4GB"),
    integer("temp_buffers", Unit::Blocks, 100, 1_073_741_823, "8MB"),
    integer(
        "min_parallel_table_scan_size",
        Unit::Blocks,
        0,
        715_827_882,
        "8MB",
    ),
    integer(
        "min_parallel_index_scan_size",
        Unit::Blocks,
        0,
        715_827_882,
        "512kB",
    ),
    integer("default_statistics_target", Unit::None, 1, 10_000, "100"),
    integer("from_collapse_limit", Unit::None, 1, INT_MAX, "8"),
    integer("join_collapse_limit", Unit::None, 1, INT_MAX, "8"),
    integer("geqo_threshold", Unit::None, 2, INT_MAX, "12"),
    integer("max_parallel_workers_per_gather", Unit::None, 0, 1024, "2"),
    integer("max_parallel_maintenance_workers", Unit::None, 0, 1024, "2"),
    integer("effective_io_concurrency", Unit::None, 0, 1000, "16"),
    integer("maintenance_io_concurrency", Unit::None, 0, 1000, "16"),
    real("seq_page_cost", 0.0, f64::MAX, "1"),
    real("random_page_cost", 0.0, f64::MAX, "4"),
    real("cpu_tuple_cost", 0.0, f64::MAX, "0.01"),
    real("cpu_index_tuple_cost", 0.0, f64::MAX, "0.005"),
    real("cpu_operator_cost", 0.0, f64::MAX, "0.0025"),
    real("parallel_tuple_cost", 0.0, f64::MAX, "0.1"),
    real("parallel_setup_cost", 0.0, f64::MAX, "1000"),
    real("cursor_tuple_fraction", 0.0, 1.0, "0.1"),
    real("hash_mem_multiplier", 1.0, 1000.0, "2"),
    real("jit_above_cost", -1.0, f64::MAX, "100000"),
    real("jit_inline_above_cost", -1.0, f64::MAX, "500000"),
    real("jit_optimize_above_cost", -1.0, f64::MAX, "500000"),
    boolean("enable_async_append", "on"),
    boolean("enable_bitmapscan", "on"),
    boolean("enable_distinct_reordering", "on"),
    boolean("enable_gathermerge", "on"),
    boolean("enable_group_by_reordering", "on"),
    boolean("enable_hashagg", "on"),
    boolean("enable_hashjoin", "on"),
    boolean("enable_incremental_sort", "on"),
    boolean("enable_indexonlyscan", "on"),
    boolean("enable_indexscan", "on"),
    boolean("enable_material", "on"),
    boolean("enable_memoize", "on"),
    boolean("enable_mergejoin", "on"),
    boolean("enable_nestloop", "on"),
    boolean("enable_parallel_append", "on"),
    boolean("enable_parallel_hash", "on"),
    boolean("enable_partition_pruning", "on"),
    boolean("enable_partitionwise_aggregate", "off"),
    boolean("enable_partitionwise_join", "off"),
    boolean("enable_presorted_aggregate", "on"),
    boolean("enable_self_join_elimination", "on"),
    boolean("enable_seqscan", "on"),
    boolean("enable_sort", "on"),
    boolean("enable_tidscan", "on"),
    boolean("geqo", "on"),
    boolean("parallel_leader_participation", "on"),
    PlannerGuc {
        name: "plan_cache_mode",
        kind: Kind::Enum(&["auto", "force_generic_plan", "force_custom_plan"]),
        default: "auto",
    },
    PlannerGuc {
        name: "constraint_exclusion",
        kind: Kind::Enum(&["partition", "on", "off"]),
        default: "partition",
    },
];

fn lookup(name: &str) -> Option<&'static PlannerGuc> {
    PLANNER_GUCS.iter().find(|guc| guc.name == name)
}

/// Whether `name` (lower case) is one of the accepted planner/resource settings.
pub(crate) fn is_planner_guc(name: &str) -> bool {
    lookup(name).is_some()
}

/// The built-in default of a planner/resource setting, as `SHOW` prints it.
pub(crate) fn planner_guc_default(name: &str) -> Option<&'static str> {
    lookup(name).map(|guc| guc.default)
}

fn invalid_value(name: &str, value: &str) -> SqlError {
    SqlError::invalid_parameter_value(format!(
        "invalid value for parameter \"{name}\": \"{value}\""
    ))
}

/// Validate `value` for the setting `name` and return its normalised display
/// form, or the PostgreSQL error (SQLSTATE 22023) for an invalid value.
pub(crate) fn normalize_planner_guc(name: &str, value: &str) -> Result<Option<String>> {
    let Some(guc) = lookup(name) else {
        return Ok(None);
    };
    match guc.kind {
        Kind::Bool => parse_pg_bool(value)
            .map(|on| Some(if on { "on" } else { "off" }.into()))
            .ok_or_else(|| {
                SqlError::invalid_parameter_value(format!(
                    "parameter \"{name}\" requires a Boolean value"
                ))
            }),
        Kind::Enum(values) => {
            let lower = value.trim().to_ascii_lowercase();
            if let Some(found) = values.iter().find(|candidate| **candidate == lower) {
                return Ok(Some((*found).to_string()));
            }
            // constraint_exclusion also takes the boolean spellings.
            if values.contains(&"on") {
                if let Some(on) = parse_pg_bool(value) {
                    return Ok(Some(if on { "on" } else { "off" }.to_string()));
                }
            }
            Err(invalid_value(name, value))
        }
        Kind::Real { min, max } => {
            let trimmed = value.trim();
            let parsed = trimmed
                .parse::<f64>()
                .ok()
                .filter(|parsed| parsed.is_finite())
                .ok_or_else(|| invalid_value(name, value))?;
            if parsed < min || parsed > max {
                return Err(SqlError::invalid_parameter_value(format!(
                    "{} is outside the valid range for parameter \"{name}\" ({} .. {})",
                    format_g(parsed),
                    format_g(min),
                    format_g(max)
                )));
            }
            Ok(Some(format_g(parsed)))
        }
        Kind::Int { unit, min, max } => {
            let parsed =
                parse_int_with_unit(value, unit).ok_or_else(|| invalid_value(name, value))?;
            if parsed < min || parsed > max {
                let suffix = match unit {
                    Unit::None => "",
                    Unit::Kb => " kB",
                    Unit::Blocks => " 8kB",
                };
                return Err(SqlError::invalid_parameter_value(format!(
                    "{parsed}{suffix} is outside the valid range for parameter \"{name}\" ({min}{suffix} .. {max}{suffix})"
                )));
            }
            Ok(Some(format_int_with_unit(parsed, unit)))
        }
    }
}

/// PostgreSQL's `parse_bool`: on/off, true/false, yes/no, 1/0 and unique
/// prefixes of the words.
fn parse_pg_bool(value: &str) -> Option<bool> {
    let lower = value.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    let prefix_of = |word: &str| word.starts_with(lower.as_str());
    match lower.as_str() {
        "1" => return Some(true),
        "0" => return Some(false),
        "on" => return Some(true),
        "of" | "off" => return Some(false),
        _ => {}
    }
    if lower.len() >= 2 || !lower.starts_with('o') {
        if prefix_of("true") || prefix_of("yes") {
            return Some(true);
        }
        if prefix_of("false") || prefix_of("no") {
            return Some(false);
        }
    }
    None
}

/// Bytes per unit spelling, as PostgreSQL's memory unit table.
fn memory_unit_bytes(unit: &str) -> Option<f64> {
    Some(match unit {
        "B" => 1.0,
        "kB" => 1024.0,
        "MB" => 1024.0 * 1024.0,
        "GB" => 1024.0 * 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    })
}

fn base_unit_bytes(unit: Unit) -> f64 {
    match unit {
        Unit::None => 1.0,
        Unit::Kb => 1024.0,
        Unit::Blocks => 8192.0,
    }
}

/// PostgreSQL's `parse_int` with `GUC_UNIT_*` handling: an integer or
/// fractional number, optionally followed by a memory unit; the result is
/// rounded to the setting's base unit.
fn parse_int_with_unit(value: &str, unit: Unit) -> Option<i64> {
    let text = value.trim();
    let number_end =
        text.char_indices()
            .find(|(index, ch)| {
                !(ch.is_ascii_digit()
                    || *ch == '.'
                    || ((*ch == '-' || *ch == '+') && *index == 0)
                    || ((*ch == 'e' || *ch == 'E')
                        && text[index + 1..].chars().next().is_some_and(|next| {
                            next.is_ascii_digit() || next == '-' || next == '+'
                        })))
            })
            .map(|(index, _)| index)
            .unwrap_or(text.len());
    let (number, rest) = text.split_at(number_end);
    let rest = rest.trim();
    let mut parsed = if let Ok(integer) = number.parse::<i64>() {
        integer as f64
    } else {
        number
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())?
            .round_ties_even()
    };
    if !rest.is_empty() {
        if matches!(unit, Unit::None) {
            return None;
        }
        // A fractional number keeps its fraction until the unit conversion.
        let raw = number.parse::<f64>().ok()?;
        parsed = (raw * memory_unit_bytes(rest)? / base_unit_bytes(unit)).round_ties_even();
    }
    if parsed > i64::MAX as f64 / 2.0 || parsed < i64::MIN as f64 / 2.0 {
        return None;
    }
    Some(parsed as i64)
}

/// Display an integer setting in the largest memory unit that divides it
/// exactly, as PostgreSQL's `SHOW` does.
fn format_int_with_unit(value: i64, unit: Unit) -> String {
    let base = match unit {
        Unit::None => return value.to_string(),
        Unit::Kb => 1024i128,
        Unit::Blocks => 8192i128,
    };
    let bytes = value as i128 * base;
    if bytes == 0 {
        return match unit {
            Unit::Blocks => "0".to_string(),
            _ => "0kB".to_string(),
        };
    }
    for (name, size) in [
        ("TB", 1i128 << 40),
        ("GB", 1i128 << 30),
        ("MB", 1i128 << 20),
        ("kB", 1i128 << 10),
    ] {
        if bytes % size == 0 {
            return format!("{}{name}", bytes / size);
        }
    }
    format!("{bytes}B")
}

/// C's `%g`: six significant digits, trailing zeros removed, exponent form
/// for very small or large magnitudes.
pub(crate) fn format_g(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let exponent = value.abs().log10().floor() as i32;
    let scientific = format!("{:.5e}", value);
    let (mantissa, exp) = scientific.split_once('e').expect("exponent form");
    let exp = exp.parse::<i32>().expect("exponent");
    let exponent = if exp != exponent { exp } else { exponent };
    if !(-4..6).contains(&exponent) {
        let mantissa = trim_fraction(mantissa);
        return format!(
            "{mantissa}e{}{:02}",
            if exponent < 0 { '-' } else { '+' },
            exponent.abs()
        );
    }
    let decimals = (5 - exponent).max(0) as usize;
    trim_fraction(&format!("{value:.decimals$}")).to_string()
}

fn trim_fraction(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(name: &str, value: &str) -> String {
        normalize_planner_guc(name, value).unwrap().unwrap()
    }

    #[test]
    fn memory_settings_normalise_like_postgres() {
        assert_eq!(normalized("work_mem", "64MB"), "64MB");
        assert_eq!(normalized("work_mem", "65536"), "64MB");
        assert_eq!(normalized("work_mem", "1024kB"), "1MB");
        assert_eq!(normalized("work_mem", "1.5MB"), "1536kB");
        assert_eq!(normalized("work_mem", "100"), "100kB");
        assert_eq!(normalized("work_mem", "1GB"), "1GB");
        assert_eq!(normalized("effective_cache_size", "4GB"), "4GB");
        assert_eq!(normalized("effective_cache_size", "100kB"), "96kB");
        assert_eq!(normalized("effective_cache_size", "1000"), "8000kB");
        assert_eq!(normalized("maintenance_work_mem", "256MB"), "256MB");
    }

    #[test]
    fn invalid_memory_values_are_22023() {
        for (name, value) in [("work_mem", "64XB"), ("work_mem", "abc"), ("work_mem", "")] {
            let error = normalize_planner_guc(name, value).unwrap_err();
            assert_eq!(error.sqlstate(), "22023", "{value}");
        }
        let error = normalize_planner_guc("work_mem", "10").unwrap_err();
        assert_eq!(
            error.to_string(),
            "10 kB is outside the valid range for parameter \"work_mem\" (64 kB .. 2147483647 kB)"
        );
    }

    #[test]
    fn real_bool_and_enum_settings() {
        assert_eq!(normalized("random_page_cost", "1.1"), "1.1");
        assert_eq!(normalized("random_page_cost", "4"), "4");
        assert_eq!(normalized("cpu_operator_cost", "0.0025"), "0.0025");
        assert_eq!(normalized("enable_seqscan", "off"), "off");
        assert_eq!(normalized("enable_seqscan", "TRUE"), "on");
        assert_eq!(normalized("enable_seqscan", "n"), "off");
        assert_eq!(
            normalized("plan_cache_mode", "FORCE_CUSTOM_PLAN"),
            "force_custom_plan"
        );
        assert!(normalize_planner_guc("enable_seqscan", "maybe").is_err());
        assert!(normalize_planner_guc("random_page_cost", "-1").is_err());
        assert!(normalize_planner_guc("synchronous_commit", "off")
            .unwrap()
            .is_none());
        assert_eq!(format_g(f64::MAX), "1.79769e+308");
        assert_eq!(format_g(100000.0), "100000");
        assert_eq!(format_g(1000000.0), "1e+06");
    }
}
