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
        // ln 2 = 2 atanh(1/3).
        let ln2 = self.two_atanh(&divide(&self.one, &BigInt::from(3), 0));
        ln_m + ln2 * BigInt::from(k)
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
    let base_fixed = rescale(&base.mantissa.abs(), i64::from(base.scale), working).mantissa;
    let exponent_fixed = rescale(&exponent.mantissa, i64::from(exponent.scale), working).mantissa;
    let ln_base = fixed.ln(&base_fixed);
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
