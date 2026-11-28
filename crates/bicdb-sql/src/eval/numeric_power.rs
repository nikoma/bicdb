//! NUMERIC exponentiation (`numeric ^ numeric`, `power(numeric, numeric)`),
//! with PostgreSQL's result scale and its exact results for integral
//! exponents.
//!
//! The result scale follows PostgreSQL: 16 significant digits counted from
//! the result's estimated decimal weight (`16 - trunc(exp * log10|base|)`),
//! but never fewer fractional digits than either input carries, and at most
//! 1000. An integral exponent is computed exactly and rounded once; any other
//! exponent goes through `exp(exp * ln(base))` in fixed point with guard
//! digits, so the rounded result is the correctly rounded value.
use super::*;
use num_bigint::BigInt;
use num_traits::{One, Signed, ToPrimitive, Zero};

const MIN_SIG_DIGITS: i64 = 16;
const MAX_DISPLAY_SCALE: i64 = 1000;
/// Extra fractional digits carried through the series computations.
const GUARD_DIGITS: i64 = 24;

fn power_error(message: &str) -> SqlError {
    SqlError::data_exception("2201F", message, None)
}

fn to_f64(value: &Decimal) -> f64 {
    let digits = value.mantissa.to_string();
    format!("{digits}e-{}", value.scale)
        .parse::<f64>()
        .unwrap_or(f64::NAN)
}

/// `value` as an integer when it has no fractional part.
fn integral(value: &Decimal) -> Option<BigInt> {
    let divisor = pow10_bigint(value.scale);
    (&value.mantissa % &divisor)
        .is_zero()
        .then(|| &value.mantissa / &divisor)
}

/// Fractional digits written in the input (PostgreSQL's display scale).
fn display_scale(value: &Decimal) -> i64 {
    i64::from(value.scale)
}

fn result_scale(estimated_weight: f64, base: &Decimal, exponent: &Decimal) -> i64 {
    let weight = if estimated_weight.is_finite() {
        estimated_weight.clamp(-1e6, 1e6) as i64
    } else {
        0
    };
    (MIN_SIG_DIGITS - weight)
        .max(display_scale(base))
        .max(display_scale(exponent))
        .clamp(0, MAX_DISPLAY_SCALE)
}

/// `numerator / 10^shift` rounded half away from zero, as a Decimal of
/// `scale` fractional digits where `numerator` carries `from_scale` digits.
fn rescale(mantissa: &BigInt, from_scale: i64, scale: i64) -> Decimal {
    if scale >= from_scale {
        return Decimal {
            mantissa: mantissa * pow10_bigint((scale - from_scale) as u32),
            scale: scale as u32,
        };
    }
    let divisor = pow10_bigint((from_scale - scale) as u32);
    let negative = mantissa.is_negative();
    let magnitude = mantissa.abs();
    let (mut quotient, remainder) = (&magnitude / &divisor, &magnitude % &divisor);
    if &remainder * 2_i32 >= divisor {
        quotient += 1_i32;
    }
    Decimal {
        mantissa: if negative { -quotient } else { quotient },
        scale: scale as u32,
    }
}

/// `numerator / denominator` with `scale` fractional digits, half away from
/// zero.
fn divide(numerator: &BigInt, denominator: &BigInt, scale: i64) -> BigInt {
    let scaled = numerator * pow10_bigint(scale.max(0) as u32);
    let negative = scaled.is_negative() != denominator.is_negative();
    let (num, den) = (scaled.abs(), denominator.abs());
    let (mut quotient, remainder) = (&num / &den, &num % &den);
    if &remainder * 2_i32 >= den {
        quotient += 1_i32;
    }
    if negative {
        -quotient
    } else {
        quotient
    }
}

/// Fixed-point helpers: a value `v` is held as `round(v * 10^scale)`.
struct Fixed {
    scale: i64,
    one: BigInt,
}

impl Fixed {
    fn new(scale: i64) -> Self {
        Self {
            scale,
            one: pow10_bigint(scale as u32),
        }
    }

    fn mul(&self, left: &BigInt, right: &BigInt) -> BigInt {
        divide(&(left * right), &self.one, 0)
    }

    fn div(&self, left: &BigInt, right: &BigInt) -> BigInt {
        divide(&(left * &self.one), right, 0)
    }

    /// `2 * atanh(t)` for |t| < 1 in fixed point: ln((1 + t) / (1 - t)).
    fn two_atanh(&self, t: &BigInt) -> BigInt {
        let t2 = self.mul(t, t);
        let mut term = t.clone();
        let mut sum = BigInt::zero();
        let mut k = 1_u64;
        while !term.is_zero() {
            sum += &term / BigInt::from(k);
            term = self.mul(&term, &t2);
            k += 2;
        }
        sum * 2_i32
    }

    /// ln(x) for x > 0 in fixed point.
    fn ln(&self, x: &BigInt) -> BigInt {
        // x = m * 2^k with m in [2/3, 4/3): ln x = ln m + k ln 2.
        let low = divide(&(&self.one * 2_i32), &BigInt::from(3), 0);
        let high = divide(&(&self.one * 4_i32), &BigInt::from(3), 0);
        let mut m = x.clone();
        let mut k = 0_i64;
        while m >= high {
            m = divide(&m, &BigInt::from(2), 0);
            k += 1;
        }
        while m < low {
            m *= 2_i32;
            k -= 1;
        }
        let t = self.div(&(&m - &self.one), &(&m + &self.one));
        let ln_m = self.two_atanh(&t);
        if k == 0 {
            return ln_m;
        }
        // ln 2 = 2 atanh(1/3).
        let ln2 = self.two_atanh(&divide(&self.one, &BigInt::from(3), 0));
        ln_m + ln2 * BigInt::from(k)
    }

    fn ln_decimal(&self, input: &Decimal) -> BigInt {
        let magnitude = input.mantissa.abs();
        let digits = magnitude.to_str_radix(10).len() as i64;
        let weight = digits - 1 - i64::from(input.scale);
        // Normalizing the mantissa keeps even very small positive inputs
        // away from zero in fixed point.
        let scaled = rescale(&magnitude, digits - 1, self.scale).mantissa;
        let base = self.ln(&scaled);
        if weight == 0 {
            base
        } else {
            base + self.ln(&(&self.one * 10_i32)) * BigInt::from(weight)
        }
    }

    /// exp(z) for |z| small enough that the series converges quickly, after
    /// halving `halvings` times and squaring back.
    fn exp(&self, z: &BigInt) -> BigInt {
        let halvings = 12_u32;
        let r = divide(z, &BigInt::from(2_u32).pow(halvings), 0);
        let mut term = self.one.clone();
        let mut sum = self.one.clone();
        let mut k = 1_u64;
        loop {
            term = divide(&self.mul(&term, &r), &BigInt::from(k), 0);
            if term.is_zero() {
                break;
            }
            sum += &term;
            k += 1;
        }
        for _ in 0..halvings {
            sum = self.mul(&sum, &sum);
        }
        sum
    }
}

/// `base ^ exponent` for NUMERIC operands.
pub(crate) fn numeric_power(base: &Decimal, exponent: &Decimal) -> Result<Decimal> {
    let base_zero = base.mantissa.is_zero();
    if base_zero && exponent.mantissa.is_negative() {
        return Err(power_error("zero raised to a negative power is undefined"));
    }
    let integral_exponent = integral(exponent);
    if base.mantissa.is_negative() && integral_exponent.is_none() {
        return Err(power_error(
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    if base_zero {
        let scale = result_scale(0.0, base, exponent);
        let value = if exponent.mantissa.is_zero() {
            BigInt::one()
        } else {
            BigInt::zero()
        };
        return Ok(rescale(&value, 0, scale));
    }
    let log10_base = to_f64(base).abs().log10();
    let estimate = to_f64(exponent) * log10_base;
    let scale = result_scale(estimate, base, exponent);
    if let Some(n) = integral_exponent.as_ref().and_then(BigInt::to_i64) {
        // Exact: base^|n| has scale |n| * base.scale.
        let digits = base.mantissa.abs().to_string().len() as i64;
        let magnitude = n.unsigned_abs();
        if magnitude.saturating_mul(digits as u64) <= 200_000 {
            let power = base.mantissa.pow(magnitude as u32);
            let power_scale = i64::from(base.scale) * magnitude as i64;
            if n >= 0 {
                return Ok(rescale(&power, power_scale, scale));
            }
            // 1 / (power / 10^power_scale) = 10^power_scale / power.
            let numerator = pow10_bigint(power_scale as u32);
            return Ok(Decimal {
                mantissa: divide(&numerator, &power, scale),
                scale: scale as u32,
            });
        }
    }
    if estimate.abs() > 1e5 {
        return Err(SqlError::numeric_value_out_of_range(
            "value overflows numeric format",
        ));
    }
    // exp(exponent * ln(base)) = 10^w * exp(exponent * ln(base) - w ln 10).
    let weight = estimate.floor() as i64;
    let working = scale + weight.max(0) + GUARD_DIGITS;
    let fixed = Fixed::new(working);
    let negative_result = base.mantissa.is_negative()
        && integral_exponent
            .as_ref()
            .is_some_and(|n| (n % BigInt::from(2)) != BigInt::zero());
    let exponent_fixed = rescale(&exponent.mantissa, i64::from(exponent.scale), working).mantissa;
    let ln_base = fixed.ln_decimal(base);
    let ln10 = fixed.ln(&(&fixed.one * 10_i32));
    let product = fixed.mul(&ln_base, &exponent_fixed);
    let reduced = &product - &ln10 * BigInt::from(weight);
    let mantissa = fixed.exp(&reduced);
    // result = mantissa / 10^working * 10^weight.
    let mut result = rescale(&mantissa, working - weight, scale);
    if negative_result {
        result.mantissa = -result.mantissa;
    }
    Ok(result)
}

pub(super) fn pg_numeric_power_value(base: &SqlValue, exponent: &SqlValue) -> Result<SqlValue> {
    if matches!(base, SqlValue::Null) || matches!(exponent, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let base = pg_numeric_from_sql_value(base.clone())?;
    let exponent = pg_numeric_from_sql_value(exponent.clone())?;
    match (&base, &exponent) {
        (PgNumeric::Finite { .. }, PgNumeric::Finite { .. }) => {
            let base = decimal_from_pg_numeric(base)?;
            let exponent = decimal_from_pg_numeric(exponent)?;
            Ok(SqlValue::String(numeric_power(&base, &exponent)?.format()))
        }
        _ => special_numeric_power(base, exponent),
    }
}

fn special_numeric_power(base: PgNumeric, exponent: PgNumeric) -> Result<SqlValue> {
    let finite_base = match &base {
        PgNumeric::Finite { .. } => Some(decimal_from_pg_numeric(base.clone())?),
        _ => None,
    };
    let base_magnitude = finite_base
        .as_ref()
        .map(|value| value.mantissa.abs().cmp(&pow10_bigint(value.scale)));
    let base_is_one =
        base_magnitude == Some(std::cmp::Ordering::Equal) && !pg_numeric_is_negative(&base);
    let exponent_is_zero = pg_numeric_is_zero(&exponent);
    let result = match (&base, &exponent) {
        (PgNumeric::NaN, _) if !exponent_is_zero => "NaN",
        (_, PgNumeric::NaN) if !base_is_one => "NaN",
        _ if base_is_one || exponent_is_zero => "1",
        _ if pg_numeric_is_zero(&base) && pg_numeric_is_negative(&exponent) => {
            return Err(power_error("zero raised to a negative power is undefined"));
        }
        (_, PgNumeric::PositiveInfinity | PgNumeric::NegativeInfinity) => match base_magnitude {
            Some(std::cmp::Ordering::Equal) => "1",
            Some(std::cmp::Ordering::Less) if pg_numeric_is_negative(&exponent) => "Infinity",
            Some(std::cmp::Ordering::Less) => "0",
            _ if pg_numeric_is_negative(&exponent) => "0",
            _ => "Infinity",
        },
        (PgNumeric::PositiveInfinity, _) if pg_numeric_is_negative(&exponent) => "0",
        (PgNumeric::PositiveInfinity, _) => "Infinity",
        (PgNumeric::NegativeInfinity, _) => {
            let exponent = decimal_from_pg_numeric(exponent)?;
            let Some(integer) = integral(&exponent) else {
                return Err(power_error(
                    "a negative number raised to a non-integer power yields a complex result",
                ));
            };
            if integer.is_negative() {
                "0"
            } else if (integer % 2_i32).is_zero() {
                "Infinity"
            } else {
                "-Infinity"
            }
        }
        _ => unreachable!("finite inputs are handled by numeric_power"),
    };
    Ok(SqlValue::String(result.to_string()))
}

pub(super) fn numeric_sqrt(value: &SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let value = pg_numeric_from_sql_value(value.clone())?;
    if pg_numeric_is_negative(&value) {
        return Err(power_error("cannot take square root of a negative number"));
    }
    if matches!(value, PgNumeric::NaN | PgNumeric::PositiveInfinity) {
        return Ok(SqlValue::String(value.to_decimal_text()));
    }
    let value = decimal_from_pg_numeric(value)?;
    // PostgreSQL counts the base-10000 weight of the input to select the scale.
    let weight = if value.mantissa.is_zero() {
        0
    } else {
        (value.mantissa.to_str_radix(10).len() as i64 - 1 - i64::from(value.scale)).div_euclid(4)
    };
    let scale = (MIN_SIG_DIGITS - (weight * 2 + 1))
        .max(i64::from(value.scale))
        .clamp(0, MAX_DISPLAY_SCALE);
    // The integer root at one additional decimal place retains the exact
    // rounding digit. Truncating the radicand leaves its integer root unchanged.
    let shift = 2 * (scale + 1) - i64::from(value.scale);
    let radicand = if shift >= 0 {
        value.mantissa * pow10_bigint(shift as u32)
    } else {
        value.mantissa / pow10_bigint((-shift) as u32)
    };
    let result = rescale(&radicand.sqrt(), scale + 1, scale);
    Ok(SqlValue::String(result.format()))
}

pub(super) fn round_pg_numeric(value: &SqlValue, scale: i32) -> Result<SqlValue> {
    let number = pg_numeric_from_sql_value(value.clone())?;
    if !matches!(number, PgNumeric::Finite { .. }) {
        return Ok(SqlValue::String(number.to_decimal_text()));
    }
    let value = decimal_from_pg_numeric(number)?;
    let scale = i64::from(scale.min(PG_NUMERIC_MAX_FRACTIONAL_DIGITS));
    let current = i64::from(value.scale);
    if current - scale > value.mantissa.abs().to_str_radix(10).len() as i64 {
        return Ok(SqlValue::String("0".to_string()));
    }
    if scale < 0 {
        let digits = pow10_bigint((current - scale) as u32);
        let integer = divide(&value.mantissa, &digits, 0) * pow10_bigint((-scale) as u32);
        return cast_value_to_pg_type(SqlValue::String(integer.to_string()), "numeric");
    }
    let rounded = rescale(&value.mantissa, current, scale);
    cast_value_to_pg_type(SqlValue::String(rounded.format()), "numeric")
}

pub(super) fn pg_numeric_ceil_floor(value: &SqlValue, ceil: bool) -> Result<SqlValue> {
    let number = pg_numeric_from_sql_value(value.clone())?;
    if !matches!(number, PgNumeric::Finite { .. }) {
        return Ok(SqlValue::String(number.to_decimal_text()));
    }
    let decimal = decimal_from_pg_numeric(number)?;
    let divisor = pow10_bigint(decimal.scale);
    let mut integer = &decimal.mantissa / &divisor;
    if (&decimal.mantissa % divisor) != BigInt::ZERO && ceil != decimal.mantissa.is_negative() {
        integer += if ceil { 1 } else { -1 };
    }
    cast_value_to_pg_type(SqlValue::String(integer.to_string()), "numeric")
}

/// PostgreSQL uses the decimal weight of ln(x) to choose its display scale.
fn ln_weight(value: &Decimal) -> i64 {
    let one = pow10_bigint(value.scale);
    let difference = (&value.mantissa - &one).abs();
    if &difference * 10_i32 <= one {
        if difference.is_zero() {
            return 0;
        }
        return difference.to_str_radix(10).len() as i64 - 1 - i64::from(value.scale);
    }
    let digits = value.mantissa.to_str_radix(10);
    let weight = digits.len() as i64 - 1 - i64::from(value.scale);
    // Approximate with the first two base-10000 digits. This stays finite
    // for very large and very small numerics.
    let end = ((weight.rem_euclid(4) + 5) as usize).min(digits.len());
    let prefix: f64 = digits[..end].parse().expect("positive decimal digits");
    let approximate = prefix.ln() + (weight + 1 - end as i64) as f64 * std::f64::consts::LN_10;
    approximate.abs().log10() as i64
}

fn check_log_operand(value: &PgNumeric) -> Result<()> {
    if pg_numeric_is_negative(value) {
        return Err(SqlError::data_exception(
            "2201E",
            "cannot take logarithm of a negative number",
            None,
        ));
    }
    if pg_numeric_is_zero(value) {
        return Err(SqlError::data_exception(
            "2201E",
            "cannot take logarithm of zero",
            None,
        ));
    }
    Ok(())
}

pub(crate) fn numeric_logarithm(base: Option<&SqlValue>, value: &SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) || base.is_some_and(|base| matches!(base, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let value = pg_numeric_from_sql_value(value.clone())?;
    let base = base
        .map(|base| pg_numeric_from_sql_value(base.clone()))
        .transpose()?;
    if value == PgNumeric::NaN || base.as_ref() == Some(&PgNumeric::NaN) {
        return Ok(SqlValue::String("NaN".into()));
    }
    if let Some(base) = &base {
        check_log_operand(base)?;
    }
    check_log_operand(&value)?;

    if base == Some(PgNumeric::PositiveInfinity) {
        return Ok(SqlValue::String(
            if value == PgNumeric::PositiveInfinity {
                "NaN"
            } else {
                "0"
            }
            .into(),
        ));
    }
    if value == PgNumeric::PositiveInfinity {
        return Ok(SqlValue::String("Infinity".into()));
    }
    let value = decimal_from_pg_numeric(value)?;
    let value_weight = ln_weight(&value);
    let result = if let Some(base) = base {
        let base = decimal_from_pg_numeric(base)?;
        let base_weight = ln_weight(&base);
        let weight = value_weight - base_weight;
        let scale = (MIN_SIG_DIGITS - weight)
            .max(display_scale(&value))
            .max(display_scale(&base))
            .clamp(0, MAX_DISPLAY_SCALE);
        // When the base is very close to one, its logarithm needs extra
        // precision before dividing by it.
        let working =
            scale + weight.max(0) + (-base_weight).max(0) + (-value_weight).max(0) + GUARD_DIGITS;
        let fixed = Fixed::new(working);
        let denominator = fixed.ln_decimal(&base);
        if denominator.is_zero() {
            return Err(division_by_zero());
        }
        Decimal {
            mantissa: divide(&fixed.ln_decimal(&value), &denominator, scale),
            scale: scale as u32,
        }
    } else {
        let scale = (MIN_SIG_DIGITS - value_weight)
            .max(display_scale(&value))
            .clamp(0, MAX_DISPLAY_SCALE);
        let fixed = Fixed::new(scale + GUARD_DIGITS);
        rescale(&fixed.ln_decimal(&value), fixed.scale, scale)
    };
    Ok(SqlValue::String(result.format()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn power(base: &str, exponent: &str) -> String {
        numeric_power(
            &parse_decimal(base).unwrap(),
            &parse_decimal(exponent).unwrap(),
        )
        .unwrap()
        .format()
    }

    /// Expected values are PostgreSQL 16's `numeric ^ numeric` output.
    #[test]
    fn numeric_power_matches_postgresql() {
        for (base, exponent, expected) in [
            ("10", "2", "100.00000000000000"),
            ("10", "3", "1000.0000000000000"),
            ("10", "20", "100000000000000000000"),
            ("1.1", "2", "1.2100000000000000"),
            ("2", "10", "1024.0000000000000"),
            ("10", "-2", "0.010000000000000000"),
            ("10", "-5", "0.000010000000000000000"),
            ("2", "0.5", "1.4142135623730950"),
            ("1.5", "2.5", "2.7556759606310754"),
            ("0.5", "2", "0.2500000000000000"),
            ("100", "0.5", "10.000000000000000"),
            ("10", "2.0", "100.00000000000000"),
            ("1.23456", "2", "1.5241383936000000"),
            ("7", "0", "1.0000000000000000"),
            ("0", "2", "0.0000000000000000"),
            ("-2", "3", "-8.0000000000000000"),
            ("2", "100", "1267650600228229401496703205376"),
            ("0.001", "2", "0.0000010000000000000000"),
            ("12345.678", "3", "1881675960266.5586"),
            ("1.000001", "1000", "1.0010004996662084"),
        ] {
            assert_eq!(power(base, exponent), expected, "{base} ^ {exponent}");
        }
    }

    #[test]
    fn numeric_power_errors_match_postgresql() {
        let error = |base: &str, exponent: &str| {
            numeric_power(
                &parse_decimal(base).unwrap(),
                &parse_decimal(exponent).unwrap(),
            )
            .unwrap_err()
            .to_string()
        };
        assert!(error("0", "-1").contains("zero raised to a negative power"));
        assert!(error("-2", "0.5").contains("complex result"));
    }
}
