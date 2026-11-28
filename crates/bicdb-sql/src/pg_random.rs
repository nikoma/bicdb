use crate::{pg_numeric_from_sql_value, PgNumeric, Result, SqlError, SqlValue};
use num_bigint::{BigInt, BigUint, Sign};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

pub(crate) fn catalog_signature(
    oid: i64,
) -> Option<(&'static str, &'static str, Option<[&'static str; 2]>)> {
    let bounds = Some(["min", "max"]);
    Some(match oid {
        1598 => ("", "drandom", None),
        1599 => ("701", "setseed", None),
        6212 => ("701 701", "drandom_normal", Some(["mean", "stddev"])),
        6339 => ("23 23", "int4random", bounds),
        6340 => ("20 20", "int8random", bounds),
        6341 => ("1700 1700", "numeric_random", bounds),
        _ => return None,
    })
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RandomState(Arc<Mutex<Option<Generator>>>);

thread_local! {
    static SESSION: RefCell<Option<RandomState>> = const { RefCell::new(None) };
    static FALLBACK: RandomState = RandomState::default();
}

pub(crate) struct RandomScope(Option<RandomState>);

impl RandomScope {
    pub(crate) fn enter(state: &RandomState) -> Self {
        Self(SESSION.with(|session| session.replace(Some(state.clone()))))
    }
}

impl Drop for RandomScope {
    fn drop(&mut self) {
        SESSION.with(|session| session.replace(self.0.take()));
    }
}

fn with_generator<T>(operation: impl FnOnce(&mut Generator) -> T) -> T {
    let state = SESSION
        .with(|session| session.borrow().clone())
        .unwrap_or_else(|| FALLBACK.with(Clone::clone));
    let mut state = state.0.lock().unwrap_or_else(|error| error.into_inner());
    operation(state.get_or_insert_with(Generator::unseeded))
}

#[derive(Debug)]
struct Generator {
    first: u64,
    second: u64,
}

impl Generator {
    fn seed(mut seed: u64) -> Self {
        // SplitMix64 expands the scalar seed into a pair of state words.
        let mut next = || {
            seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mixed = (seed ^ (seed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^ (mixed >> 31)
        };
        Self {
            first: next(),
            second: next(),
        }
    }

    fn unseeded() -> Self {
        let mut seed = [0; 8];
        if getrandom::fill(&mut seed).is_ok() {
            return Self::seed(u64::from_ne_bytes(seed));
        }
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64);
        Self::seed(time ^ (u64::from(std::process::id()) << 32))
    }

    fn next(&mut self) -> u64 {
        // PostgreSQL's SQL random functions share a xoroshiro128** stream.
        let mixed = self.first ^ self.second;
        let value = self.first.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        self.first = self.first.rotate_left(24) ^ mixed ^ (mixed << 16);
        self.second = mixed.rotate_left(37);
        value
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 12) as f64 / (1u64 << 52) as f64
    }

    fn offset(&mut self, inclusive_max: u64) -> u64 {
        if inclusive_max == 0 {
            return 0;
        }
        loop {
            let sample = self.next() >> inclusive_max.leading_zeros();
            if sample <= inclusive_max {
                return sample;
            }
        }
    }

    fn normal(&mut self, mean: f64, deviation: f64) -> f64 {
        let radius = (-2.0 * (1.0 - self.unit()).ln()).sqrt();
        let angle = std::f64::consts::TAU * (1.0 - self.unit());
        radius * angle.sin() * deviation + mean
    }
}

pub(crate) fn unit() -> f64 {
    with_generator(Generator::unit)
}

pub(crate) fn range_pg_type(types: &[Option<String>]) -> Option<&'static str> {
    let [left, right] = types else {
        return None;
    };
    let types = [left.as_deref(), right.as_deref()];
    if !types.iter().all(|kind| {
        matches!(
            kind,
            None | Some("unknown" | "int2" | "int4" | "int8" | "numeric")
        )
    }) {
        return None;
    }
    ["numeric", "int8", "int4"]
        .into_iter()
        .find(|candidate| types.contains(&Some(candidate)))
}

fn invalid_overload(name: &str, types: &[Option<String>], ambiguous: bool) -> SqlError {
    let inputs = types
        .iter()
        .map(|ty| crate::common_type_display(ty.as_deref().unwrap_or("unknown")))
        .collect::<Vec<_>>()
        .join(", ");
    let (state, suffix, hint) = if ambiguous {
        ("42725", "is not unique", "Could not choose a best candidate function. You might need to add explicit type casts.")
    } else {
        ("42883", "does not exist", "No function matches the given name and argument types. You might need to add explicit type casts.")
    };
    SqlError::coded(
        state,
        format!("function {name}({inputs}) {suffix}"),
        None,
        Some(hint.into()),
    )
}

fn invalid_bounds() -> SqlError {
    SqlError::invalid_parameter_value("lower bound must be less than or equal to upper bound")
}

fn numeric_bound(value: &SqlValue, side: &str) -> Result<(BigInt, i32)> {
    match pg_numeric_from_sql_value(value.clone())? {
        PgNumeric::Finite {
            negative,
            coefficient,
            display_scale,
        } => {
            let sign = if negative { Sign::Minus } else { Sign::Plus };
            let value = BigUint::parse_bytes(coefficient.as_bytes(), 10).unwrap_or_default();
            Ok((BigInt::from_biguint(sign, value), display_scale))
        }
        PgNumeric::NaN => Err(SqlError::invalid_parameter_value(format!(
            "{side} bound cannot be NaN"
        ))),
        PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity => Err(
            SqlError::invalid_parameter_value(format!("{side} bound cannot be infinity")),
        ),
    }
}

fn numeric_range(lower: &SqlValue, upper: &SqlValue) -> Result<SqlValue> {
    let (lower, lower_scale) = numeric_bound(lower, "lower")?;
    let (upper, upper_scale) = numeric_bound(upper, "upper")?;
    let scale = 0.max(lower_scale).max(upper_scale);
    let lower = lower * BigInt::from(10).pow((scale - lower_scale) as u32);
    let upper = upper * BigInt::from(10).pow((scale - upper_scale) as u32);
    if lower > upper {
        return Err(invalid_bounds());
    }
    let difference = upper - &lower;
    let offset =
        with_generator(|generator| numeric_offset(generator, difference.magnitude(), scale));
    let picked = lower + BigInt::from(offset);
    let value = PgNumeric::finite(
        picked.sign() == Sign::Minus,
        picked.magnitude().to_str_radix(10),
        scale,
    )
    .map_err(|_| SqlError::numeric_value_out_of_range("numeric value out of range"))?;
    Ok(SqlValue::String(value.to_decimal_text()))
}

fn numeric_offset(generator: &mut Generator, limit: &BigUint, scale: i32) -> BigUint {
    const BASE: u64 = 10_000;
    if limit == &BigUint::default() {
        return BigUint::default();
    }
    // Use the same base-10000 groups as PostgreSQL so a seed produces the
    // same decimal series as well as the same integer and floating series.
    let padding = 10u64.pow((4 - scale % 4) as u32 % 4);
    let aligned = (limit * padding).to_str_radix(10);
    let group_count = aligned.len().div_ceil(4);
    let prefix_groups = group_count.min(4);
    let prefix_width = aligned.len() - (group_count - prefix_groups) * 4;
    let prefix = aligned[..prefix_width].parse::<u64>().unwrap_or_default();
    if group_count <= 4 {
        return generator.offset(prefix / padding).into();
    }
    loop {
        use std::fmt::Write;
        let mut candidate = format!("{:0prefix_width$}", generator.offset(prefix));
        let whole_groups = group_count - usize::from(padding != 1);
        let mut remaining = whole_groups - prefix_groups;
        while remaining >= 4 {
            let mut block = generator.offset(BASE.pow(4) - 1);
            for _ in 0..4 {
                let _ = write!(candidate, "{:04}", block % BASE);
                block /= BASE;
            }
            remaining -= 4;
        }
        for _ in 0..remaining {
            let _ = write!(candidate, "{:04}", generator.offset(BASE - 1));
        }
        if padding != 1 {
            let _ = write!(
                candidate,
                "{:04}",
                generator.offset(BASE / padding - 1) * padding
            );
        }
        if candidate <= aligned {
            return BigUint::parse_bytes(candidate.as_bytes(), 10).unwrap_or_default() / padding;
        }
    }
}

pub(crate) fn evaluate(
    name: &str,
    args: &[SqlValue],
    types: Option<&[Option<String>]>,
) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if !matches!(name, "random" | "random_normal" | "setseed") {
        return Ok(None);
    }
    let inferred;
    let types = match types {
        Some(types) if types.len() == args.len() => types,
        _ => {
            inferred = args
                .iter()
                .map(|value| match value {
                    SqlValue::Int(value) if i32::try_from(*value).is_ok() => Some("int4".into()),
                    SqlValue::Int(_) => Some("int8".into()),
                    SqlValue::Float(_) => Some("float8".into()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            inferred.as_slice()
        }
    };
    let null = args.iter().any(|value| matches!(value, SqlValue::Null));
    let value = match (name, args) {
        ("random", []) => SqlValue::Float(unit()),
        ("random", [lower, upper]) => {
            let range_type = range_pg_type(types).ok_or_else(|| {
                let ambiguous = types
                    .iter()
                    .all(|ty| matches!(ty.as_deref(), None | Some("unknown" | "int2")));
                invalid_overload(name, types, ambiguous)
            })?;
            if null {
                SqlValue::Null
            } else if range_type == "numeric" {
                numeric_range(lower, upper)?
            } else {
                let lower = integer_bound(lower, range_type)?;
                let upper = integer_bound(upper, range_type)?;
                if lower > upper {
                    return Err(invalid_bounds());
                }
                let difference = (upper as u64).wrapping_sub(lower as u64);
                let offset = with_generator(|generator| generator.offset(difference));
                SqlValue::Int(lower.wrapping_add(offset as i64))
            }
        }
        ("random_normal", args) if args.len() <= 2 => {
            validate_float_types(name, types)?;
            if null {
                SqlValue::Null
            } else {
                let mean = args.first().map(float_argument).transpose()?.unwrap_or(0.0);
                let deviation = args.get(1).map(float_argument).transpose()?.unwrap_or(1.0);
                SqlValue::Float(with_generator(|generator| {
                    generator.normal(mean, deviation)
                }))
            }
        }
        ("setseed", [value]) => {
            validate_float_types(name, types)?;
            if null {
                SqlValue::Null
            } else {
                let seed = float_argument(value)?;
                if !(-1.0..=1.0).contains(&seed) {
                    let shown = if seed.is_nan() {
                        "nan".into()
                    } else {
                        seed.to_string()
                    };
                    return Err(SqlError::invalid_parameter_value(format!(
                        "setseed parameter {shown} is out of allowed range [-1,1]"
                    )));
                }
                let seed = (seed * ((1u64 << 52) - 1) as f64) as i64;
                with_generator(|generator| *generator = Generator::seed(seed as u64));
                SqlValue::String(String::new())
            }
        }
        _ => return Err(invalid_overload(name, types, false)),
    };
    Ok(Some(value))
}

fn integer_bound(value: &SqlValue, range_type: &str) -> Result<i64> {
    let cast = crate::cast_value_to_pg_type(value.clone(), range_type)?;
    crate::sql_value_i64(&cast).ok_or_else(invalid_bounds)
}

fn validate_float_types(name: &str, types: &[Option<String>]) -> Result<()> {
    if types.iter().all(|kind| {
        matches!(
            kind.as_deref(),
            None | Some("unknown" | "int2" | "int4" | "int8" | "numeric" | "float4" | "float8")
        )
    }) {
        Ok(())
    } else {
        Err(invalid_overload(name, types, false))
    }
}

fn float_argument(value: &SqlValue) -> Result<f64> {
    let value = crate::cast_value_to_pg_type(value.clone(), "float8")?;
    crate::sql_value_f64(&value)
        .ok_or_else(|| SqlError::invalid_parameter_value("floating-point argument required"))
}
