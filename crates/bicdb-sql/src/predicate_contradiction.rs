//! Deterministic WHERE-clause contradiction detection.
//!
//! Proves that the top-level conjunction of a single-table WHERE clause can
//! never evaluate to TRUE, so the planner can answer with an empty access path
//! without touching the table (`WHERE age > 60 AND age < 30`).
//!
//! # Soundness rules
//!
//! The detector only ever answers "provably never TRUE"; anything it does not
//! fully understand is ignored, never guessed. A conjunction is never TRUE if
//! any *subset* of its conjuncts is never TRUE, so skipping an unrecognized
//! conjunct can only lose a proof, never create a wrong one.
//!
//! * **NULL semantics.** WHERE keeps a row only when the predicate is TRUE.
//!   A comparison with a NULL operand is NULL, so `x = NULL`, `x < NULL`,
//!   `x BETWEEN NULL AND 5`, `x IN (NULL)` are never TRUE, and `x IS NULL`
//!   contradicts every comparison on `x` and `x IS NOT NULL`. This reasoning
//!   is only valid in a filter context, which is the only place it is used.
//! * **Bounds.** `<`/`>` are exclusive and `<=`/`>=`/`BETWEEN` inclusive; a
//!   point interval `[c, c]` is excluded by `x <> c`.
//! * **Special values.** Every constant must be finite. With finite bounds a
//!   proof always rests on an upper bound or an equality, which NaN and ±∞
//!   cannot satisfy under either IEEE or PostgreSQL ordering.
//! * **Typing.** Only plain numeric columns participate, and each type uses
//!   a domain that matches the engine's comparison semantics:
//!   - integer columns: every constant must be integral and within the
//!     column's range (the engine coerces fractional or out-of-range literals
//!     to the column type, so only integral in-range constants compare
//!     identically under every interpretation);
//!   - unconstrained `numeric`: exact decimal comparison;
//!   - `float8`: the conjunction must be unsatisfiable under *both* exact
//!     decimal comparison and comparison after rounding each constant to the
//!     nearest double, so the proof holds however the engine compares.
//!   Domains, typmod-constrained numerics, `float4`, arrays and every
//!   non-numeric type are left alone.
//! * **Constants.** Numeric literals, unary signs, and `$n` parameters bound to
//!   integer or double values. Functions (including volatile ones), casts and
//!   arbitrary expressions are never evaluated.
//!
//! A proof removes the table scan, so per-row runtime errors that other
//! conjuncts might have raised are not raised — the same latitude PostgreSQL
//! takes (it guarantees no evaluation order) and the planner already takes for
//! a literal `FALSE` conjunct.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};

/// Comparison domain of a participating column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumericDomain {
    /// Signed integer column of the given width in bits.
    Integer(u32),
    /// Unconstrained `numeric`/`decimal`.
    Exact,
    /// `float8`.
    Float,
}

/// A column reference the caller resolved against the scanned table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColumnRef {
    /// Canonical column identity (the schema's column name).
    pub(crate) key: String,
    pub(crate) domain: NumericDomain,
}

/// A constant operand the caller resolved without evaluating expressions.
#[derive(Clone, Debug)]
pub(crate) enum ConstValue {
    Null,
    Number(NumberConst),
}

/// A finite numeric constant in both exact and double form.
#[derive(Clone, Debug)]
pub(crate) struct NumberConst {
    exact: Exact,
    /// Nearest double, or `None` when it would overflow to ±∞.
    double: Option<f64>,
    /// True when the constant came from a double-typed parameter; such a
    /// constant only participates on `float8` columns.
    from_double: bool,
}

impl NumberConst {
    /// Parse a SQL numeric literal (`12`, `-1.50`, `1e-3`).
    pub(crate) fn from_literal(text: &str) -> Option<Self> {
        let exact = Exact::parse(text)?;
        let double = text.parse::<f64>().ok().filter(|value| value.is_finite());
        Some(Self {
            exact,
            double,
            from_double: false,
        })
    }

    pub(crate) fn from_i64(value: i64) -> Self {
        Self {
            exact: Exact {
                mantissa: BigInt::from(value),
                exp10: 0,
            },
            double: Some(value as f64),
            from_double: false,
        }
    }

    pub(crate) fn from_f64(value: f64) -> Option<Self> {
        Some(Self {
            exact: Exact::from_f64(value)?,
            double: Some(value),
            from_double: true,
        })
    }

    fn negated(mut self) -> Self {
        self.exact.mantissa = -self.exact.mantissa;
        self.double = self.double.map(|value| -value);
        self
    }
}

impl ConstValue {
    pub(crate) fn negated(self) -> Self {
        match self {
            ConstValue::Null => ConstValue::Null,
            ConstValue::Number(number) => ConstValue::Number(number.negated()),
        }
    }
}

/// Exact decimal `mantissa × 10^exp10`.
#[derive(Clone, Debug)]
struct Exact {
    mantissa: BigInt,
    exp10: i32,
}

/// Literals with more digits or a larger exponent than this are left alone.
const MAX_LITERAL_DIGITS: usize = 400;
const MAX_LITERAL_EXPONENT: i32 = 400;

impl Exact {
    fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let (negative, body) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (number, exponent) = match body.find(['e', 'E']) {
            Some(at) => (&body[..at], body[at + 1..].parse::<i32>().ok()?),
            None => (body, 0),
        };
        let (int_part, frac_part) = match number.split_once('.') {
            Some((int_part, frac_part)) => (int_part, frac_part),
            None => (number, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
            || int_part.len() + frac_part.len() > MAX_LITERAL_DIGITS
            || exponent.abs() > MAX_LITERAL_EXPONENT
        {
            return None;
        }
        let digits = format!("{int_part}{frac_part}");
        let mut mantissa = digits.parse::<BigInt>().ok()?;
        if negative {
            mantissa = -mantissa;
        }
        Some(Self {
            mantissa,
            exp10: exponent - frac_part.len() as i32,
        })
    }

    /// The exact decimal value of a finite double.
    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        if value == 0.0 {
            return Some(Self {
                mantissa: BigInt::zero(),
                exp10: 0,
            });
        }
        let bits = value.to_bits();
        let negative = bits >> 63 == 1;
        let biased = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1u64 << 52) - 1);
        let (significand, exp2) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | (1u64 << 52), biased - 1075)
        };
        // value = significand × 2^exp2; for exp2 < 0 that is
        // significand × 5^-exp2 × 10^exp2.
        let mut mantissa = BigInt::from(significand);
        let exp10 = if exp2 >= 0 {
            mantissa <<= exp2 as usize;
            0
        } else {
            mantissa *= BigInt::from(5u8).pow((-exp2) as u32);
            exp2
        };
        if negative {
            mantissa = -mantissa;
        }
        Some(Self { mantissa, exp10 })
    }

    fn is_integral(&self) -> bool {
        if self.exp10 >= 0 || self.mantissa.is_zero() {
            return true;
        }
        let divisor = BigInt::from(10u8).pow((-self.exp10) as u32);
        (&self.mantissa % divisor).is_zero()
    }

    fn fits_signed_bits(&self, bits: u32) -> bool {
        if !self.is_integral() {
            return false;
        }
        let value = if self.exp10 >= 0 {
            &self.mantissa * BigInt::from(10u8).pow(self.exp10 as u32)
        } else {
            &self.mantissa / BigInt::from(10u8).pow((-self.exp10) as u32)
        };
        let limit = BigInt::from(1u8) << (bits - 1) as usize;
        value >= -limit.clone() && value < limit
    }
}

impl PartialEq for Exact {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Exact {}
impl PartialOrd for Exact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Exact {
    fn cmp(&self, other: &Self) -> Ordering {
        // Signs first: avoids scaling when they differ.
        let (left_sign, right_sign) = (self.mantissa.signum(), other.mantissa.signum());
        if left_sign != right_sign {
            return left_sign.cmp(&right_sign);
        }
        let shift = |exact: &Exact, to: i32| -> BigInt {
            &exact.mantissa * BigInt::from(10u8).pow((exact.exp10 - to) as u32)
        };
        let base = self.exp10.min(other.exp10);
        shift(self, base).cmp(&shift(other, base))
    }
}

/// Comparison operator, normalized so the column is on the left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    fn from_binary(op: &BinaryOperator) -> Option<Self> {
        Some(match op {
            BinaryOperator::Eq => CmpOp::Eq,
            BinaryOperator::NotEq => CmpOp::NotEq,
            BinaryOperator::Lt => CmpOp::Lt,
            BinaryOperator::LtEq => CmpOp::LtEq,
            BinaryOperator::Gt => CmpOp::Gt,
            BinaryOperator::GtEq => CmpOp::GtEq,
            _ => return None,
        })
    }

    /// `c op x` → `x op' c`.
    fn flipped(self) -> Self {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::LtEq => CmpOp::GtEq,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::GtEq => CmpOp::LtEq,
            other => other,
        }
    }
}

/// One numeric constraint on a column.
#[derive(Clone, Debug)]
enum Atom {
    Cmp(CmpOp, NumberConst),
    /// `x IN (...)` with its non-NULL members.
    InSet(Vec<NumberConst>),
}

#[derive(Default)]
struct ColumnFacts {
    domain: Option<NumericDomain>,
    is_null: bool,
    is_not_null: bool,
    /// Any comparison, BETWEEN or IN on the column (each needs non-NULL `x`).
    compared: bool,
    atoms: Vec<Atom>,
}

/// True when the top-level AND-conjunction of `selection` provably never
/// evaluates to TRUE. `column` resolves an expression to a participating
/// column of the scanned table; `constant` resolves a constant operand.
pub(crate) fn conjunction_is_unsatisfiable(
    selection: &Expr,
    column: &dyn Fn(&Expr) -> Option<ColumnRef>,
    constant: &dyn Fn(&Expr) -> Option<ConstValue>,
) -> bool {
    let mut terms = Vec::new();
    flatten_and(selection, &mut terms);
    let mut facts: BTreeMap<String, ColumnFacts> = BTreeMap::new();
    fn record(facts: &mut BTreeMap<String, ColumnFacts>, column: ColumnRef) -> &mut ColumnFacts {
        let entry = facts.entry(column.key).or_default();
        entry.domain = Some(column.domain);
        entry
    }

    for term in terms {
        match term {
            Expr::Value(value) => {
                if matches!(value.value, Value::Boolean(false) | Value::Null) {
                    return true;
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let Some(op) = CmpOp::from_binary(op) else {
                    continue;
                };
                let (col, value, op) = match (column(left), column(right)) {
                    (Some(col), None) => (col, constant(right), op),
                    (None, Some(col)) => (col, constant(left), op.flipped()),
                    _ => continue,
                };
                match value {
                    // A comparison with NULL is never TRUE.
                    Some(ConstValue::Null) => return true,
                    Some(ConstValue::Number(number)) => {
                        let facts = record(&mut facts, col);
                        facts.compared = true;
                        facts.atoms.push(Atom::Cmp(op, number));
                    }
                    None => {}
                }
            }
            Expr::IsNull(inner) => {
                if let Some(col) = column(inner) {
                    record(&mut facts, col).is_null = true;
                }
            }
            Expr::IsNotNull(inner) => {
                if let Some(col) = column(inner) {
                    record(&mut facts, col).is_not_null = true;
                }
            }
            Expr::Between {
                expr,
                negated: false,
                low,
                high,
            } => {
                let Some(col) = column(expr) else {
                    continue;
                };
                match (constant(low), constant(high)) {
                    (Some(ConstValue::Null), _) | (_, Some(ConstValue::Null)) => return true,
                    (Some(ConstValue::Number(low)), Some(ConstValue::Number(high))) => {
                        let facts = record(&mut facts, col);
                        facts.compared = true;
                        facts.atoms.push(Atom::Cmp(CmpOp::GtEq, low));
                        facts.atoms.push(Atom::Cmp(CmpOp::LtEq, high));
                    }
                    _ => {}
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let Some(col) = column(expr) else {
                    continue;
                };
                let mut members = Vec::with_capacity(list.len());
                let mut understood = true;
                for item in list {
                    match constant(item) {
                        Some(ConstValue::Number(number)) => members.push(number),
                        Some(ConstValue::Null) => {}
                        None => {
                            understood = false;
                            break;
                        }
                    }
                }
                if !understood {
                    continue;
                }
                if members.is_empty() {
                    // Every member is NULL: never TRUE.
                    return true;
                }
                let facts = record(&mut facts, col);
                facts.compared = true;
                facts.atoms.push(Atom::InSet(members));
            }
            _ => {}
        }
    }

    facts.values().any(column_is_unsatisfiable)
}

fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        Expr::Nested(inner) => flatten_and(inner, out),
        other => out.push(other),
    }
}

fn column_is_unsatisfiable(facts: &ColumnFacts) -> bool {
    // NULL reasoning holds in every comparison domain.
    if facts.is_null && (facts.is_not_null || facts.compared) {
        return true;
    }
    if facts.atoms.is_empty() {
        return false;
    }
    match facts.domain {
        Some(NumericDomain::Integer(bits)) => {
            let integral_in_range = facts.atoms.iter().all(|atom| {
                atom_constants(atom)
                    .all(|number| !number.from_double && number.exact.fits_signed_bits(bits))
            });
            integral_in_range && unsatisfiable_in(&facts.atoms, |number| Some(number.exact.clone()))
        }
        Some(NumericDomain::Exact) => {
            let exact_only = facts
                .atoms
                .iter()
                .all(|atom| atom_constants(atom).all(|number| !number.from_double));
            exact_only && unsatisfiable_in(&facts.atoms, |number| Some(number.exact.clone()))
        }
        Some(NumericDomain::Float) => {
            unsatisfiable_in(&facts.atoms, |number| Some(number.exact.clone()))
                && unsatisfiable_in(&facts.atoms, |number| number.double.map(FiniteF64))
        }
        None => false,
    }
}

fn atom_constants(atom: &Atom) -> Box<dyn Iterator<Item = &NumberConst> + '_> {
    match atom {
        Atom::Cmp(_, number) => Box::new(std::iter::once(number)),
        Atom::InSet(members) => Box::new(members.iter()),
    }
}

/// Totally ordered finite double.
#[derive(Clone, Copy, Debug, PartialEq)]
struct FiniteF64(f64);
impl Eq for FiniteF64 {}
impl PartialOrd for FiniteF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FiniteF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        // Finite by construction; -0.0 == 0.0 as in SQL.
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

/// Interval + finite-set reasoning in one totally ordered domain. Returns
/// false (unknown) if any constant cannot be represented in the domain.
fn unsatisfiable_in<T: Ord + Clone>(
    atoms: &[Atom],
    convert: impl Fn(&NumberConst) -> Option<T>,
) -> bool {
    // (value, inclusive)
    let mut lower: Option<(T, bool)> = None;
    let mut upper: Option<(T, bool)> = None;
    let mut allowed: Option<Vec<T>> = None;
    let mut excluded: Vec<T> = Vec::new();

    let intersect = |allowed: &mut Option<Vec<T>>, members: Vec<T>| {
        *allowed = Some(match allowed.take() {
            None => members,
            Some(current) => current
                .into_iter()
                .filter(|value| members.contains(value))
                .collect(),
        });
    };

    for atom in atoms {
        match atom {
            Atom::Cmp(op, number) => {
                let Some(value) = convert(number) else {
                    return false;
                };
                match op {
                    CmpOp::Eq => intersect(&mut allowed, vec![value]),
                    CmpOp::NotEq => excluded.push(value),
                    CmpOp::Gt | CmpOp::GtEq => {
                        let inclusive = *op == CmpOp::GtEq;
                        let tighter = match &lower {
                            None => true,
                            Some((current, current_inclusive)) => {
                                value > *current
                                    || (value == *current && *current_inclusive && !inclusive)
                            }
                        };
                        if tighter {
                            lower = Some((value, inclusive));
                        }
                    }
                    CmpOp::Lt | CmpOp::LtEq => {
                        let inclusive = *op == CmpOp::LtEq;
                        let tighter = match &upper {
                            None => true,
                            Some((current, current_inclusive)) => {
                                value < *current
                                    || (value == *current && *current_inclusive && !inclusive)
                            }
                        };
                        if tighter {
                            upper = Some((value, inclusive));
                        }
                    }
                }
            }
            Atom::InSet(members) => {
                let Some(members) = members.iter().map(&convert).collect::<Option<Vec<T>>>() else {
                    return false;
                };
                intersect(&mut allowed, members);
            }
        }
    }

    if let (Some((low, low_inclusive)), Some((high, high_inclusive))) = (&lower, &upper) {
        match low.cmp(high) {
            Ordering::Greater => return true,
            Ordering::Equal if !(*low_inclusive && *high_inclusive) => return true,
            // A point interval behaves like an equality.
            Ordering::Equal => intersect(&mut allowed, vec![low.clone()]),
            Ordering::Less => {}
        }
    }

    let Some(allowed) = allowed else {
        return false;
    };
    let within = |value: &T| {
        let above = match &lower {
            None => true,
            Some((low, true)) => value >= low,
            Some((low, false)) => value > low,
        };
        let below = match &upper {
            None => true,
            Some((high, true)) => value <= high,
            Some((high, false)) => value < high,
        };
        above && below && !excluded.contains(value)
    };
    !allowed.iter().any(within)
}

/// Resolve a constant operand: numeric literals, unary signs and NULL; `$n`
/// placeholders through `parameter`. Never evaluates functions or casts.
pub(crate) fn literal_constant(
    expr: &Expr,
    parameter: &dyn Fn(&str) -> Option<ConstValue>,
) -> Option<ConstValue> {
    match expr {
        Expr::Nested(inner) => literal_constant(inner, parameter),
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => NumberConst::from_literal(text).map(ConstValue::Number),
            Value::Null => Some(ConstValue::Null),
            Value::Placeholder(name) => parameter(name),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => literal_constant(expr, parameter).map(ConstValue::negated),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => literal_constant(expr, parameter),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_where(sql: &str) -> Expr {
        let statement = Parser::parse_sql(
            &PostgreSqlDialect {},
            &format!("SELECT 1 FROM t WHERE {sql}"),
        )
        .expect("parse")
        .remove(0);
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("not a query")
        };
        let sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("not a select")
        };
        select.selection.expect("where")
    }

    fn column_domain(name: &str) -> Option<NumericDomain> {
        match name {
            "i" => Some(NumericDomain::Integer(32)),
            "s" => Some(NumericDomain::Integer(16)),
            "n" => Some(NumericDomain::Exact),
            "f" => Some(NumericDomain::Float),
            _ => None,
        }
    }

    fn unsat_with(sql: &str, params: &[(&str, ConstValue)]) -> bool {
        let selection = parse_where(sql);
        let column = |expr: &Expr| -> Option<ColumnRef> {
            let Expr::Identifier(ident) = expr else {
                return None;
            };
            column_domain(&ident.value).map(|domain| ColumnRef {
                key: ident.value.clone(),
                domain,
            })
        };
        let parameter = |name: &str| -> Option<ConstValue> {
            params
                .iter()
                .find(|(param, _)| *param == name)
                .map(|(_, value)| value.clone())
        };
        let constant = |expr: &Expr| literal_constant(expr, &parameter);
        conjunction_is_unsatisfiable(&selection, &column, &constant)
    }

    fn unsat(sql: &str) -> bool {
        unsat_with(sql, &[])
    }

    #[test]
    fn disjoint_ranges_are_contradictions() {
        assert!(unsat("i > 60 AND i < 30"));
        assert!(unsat("60 < i AND 30 > i"));
        assert!(unsat("(i > 60) AND (other = 1 AND i < 30)"));
        assert!(!unsat("i > 30 AND i < 60"));
        assert!(!unsat("i > 60 OR i < 30"));
    }

    #[test]
    fn inclusive_and_exclusive_bounds() {
        assert!(!unsat("i >= 5 AND i <= 5"));
        assert!(unsat("i > 5 AND i <= 5"));
        assert!(unsat("i >= 5 AND i < 5"));
        assert!(unsat("i > 5 AND i < 5"));
        assert!(unsat("i >= 5 AND i <= 5 AND i <> 5"));
        assert!(!unsat("i >= 5 AND i <= 6 AND i <> 5"));
        // The tighter of two bounds at the same value is the exclusive one.
        assert!(unsat("i >= 5 AND i > 5 AND i <= 5"));
        assert!(unsat("i BETWEEN 10 AND 1"));
        assert!(!unsat("i BETWEEN 1 AND 1"));
        assert!(unsat("i BETWEEN 1 AND 3 AND i > 3"));
        assert!(!unsat("i NOT BETWEEN 10 AND 1"));
    }

    #[test]
    fn equalities_and_in_lists() {
        assert!(unsat("i = 1 AND i = 2"));
        assert!(!unsat("i = 1 AND i = 1.0"));
        assert!(unsat("i = 3 AND i <> 3"));
        assert!(unsat("i IN (1, 2) AND i > 2"));
        assert!(!unsat("i IN (1, 2) AND i >= 2"));
        assert!(unsat("i IN (1, 2) AND i IN (3, 4)"));
        assert!(unsat("i IN (1, 2) AND i <> 1 AND i <> 2"));
        assert!(!unsat("i NOT IN (1, 2) AND i = 1"));
        assert!(!unsat("i IN (1, other) AND i = 5"));
    }

    #[test]
    fn null_semantics() {
        assert!(unsat("i = NULL"));
        assert!(unsat("i < NULL AND i > 0"));
        assert!(unsat("NULL = i"));
        assert!(unsat("i IS NULL AND i > 5"));
        assert!(unsat("i IS NULL AND i IS NOT NULL"));
        assert!(unsat("i IS NULL AND i IN (1, 2)"));
        assert!(unsat("i BETWEEN NULL AND 5"));
        assert!(unsat("i IN (NULL)"));
        assert!(unsat("i IN (NULL, NULL)"));
        // NULL members contribute nothing, other members still count.
        assert!(!unsat("i IN (NULL, 3) AND i >= 3"));
        assert!(unsat("i IN (NULL, 3) AND i > 3"));
        assert!(unsat("NULL"));
        assert!(unsat("FALSE AND i = 1"));
        assert!(!unsat("i IS NULL"));
        assert!(!unsat("i IS NULL AND n > 5"));
        assert!(!unsat("i IS NOT NULL AND i > 5"));
        // IS NULL on an unrecognized expression proves nothing.
        assert!(!unsat("other IS NULL AND other > 5"));
        // NULL reasoning is type-independent, so it also holds for columns
        // whose comparison constants are ignored.
        assert!(unsat("i IS NULL AND i > 2.5"));
    }

    #[test]
    fn integer_columns_only_use_integral_in_range_constants() {
        // Fractional constants on integer columns are coerced by the engine;
        // the detector must not reason about them.
        assert!(!unsat("i > 2.5 AND i < 2.6"));
        assert!(!unsat("i = 2.5 AND i = 3"));
        assert!(!unsat("i > 2.5 AND i < 1"));
        // Integral decimals are fine.
        assert!(unsat("i >= 3.0 AND i < 3"));
        assert!(unsat("i > 1e1 AND i < 10"));
        // Out of range for the column width: left alone.
        assert!(!unsat("s > 40000 AND s < 0"));
        assert!(unsat("s > 30000 AND s < 0"));
        assert!(!unsat("i > 3000000000 AND i < 0"));
    }

    #[test]
    fn exact_numeric_comparison() {
        assert!(unsat("n > 2.5 AND n < 2.5"));
        assert!(!unsat("n > 2.4999999999999999999999 AND n < 2.5"));
        assert!(unsat("n > 2.5000000000000000000001 AND n < 2.5"));
        assert!(unsat("n = 2.50 AND n <> 2.5"));
        assert!(unsat("n > -1.5 AND n < -2"));
    }

    #[test]
    fn float_columns_need_both_domains() {
        assert!(unsat("f > 1.5 AND f < 1.0"));
        // Equal as doubles but ordered as decimals: satisfiable as doubles.
        assert!(!unsat("f >= 0.10000000000000000001 AND f <= 0.1"));
        // Equal as doubles, satisfiable as decimals: also left alone.
        assert!(!unsat("f > 0.1 AND f < 0.10000000000000000556"));
        // Out-of-range literal raises an engine error; never fold it away.
        assert!(!unsat("f > 1e400 AND f < 0"));
    }

    #[test]
    fn parameters_are_resolved_conservatively() {
        let params = [
            ("$1", ConstValue::Number(NumberConst::from_i64(60))),
            ("$2", ConstValue::Number(NumberConst::from_i64(30))),
            ("$3", ConstValue::Null),
            (
                "$4",
                ConstValue::Number(NumberConst::from_f64(2.0).unwrap()),
            ),
        ];
        assert!(unsat_with("i > $1 AND i < $2", &params));
        assert!(!unsat_with("i > $2 AND i < $1", &params));
        assert!(unsat_with("i = $3", &params));
        // Unbound parameter: unknown, never a proof.
        assert!(!unsat_with("i > $9 AND i < $2", &params));
        // Double-typed parameters only participate on float8 columns.
        assert!(!unsat_with("i > $4 AND i < 1", &params));
        assert!(unsat_with("f > $4 AND f < 1", &params));
        assert!(unsat_with("i > -$2 AND i < -$1", &params));
    }

    #[test]
    fn exact_decimals_compare_correctly() {
        let parse = |text: &str| Exact::parse(text).unwrap();
        assert_eq!(parse("2.50"), parse("2.5"));
        assert_eq!(parse("1e2"), parse("100"));
        assert!(parse("-1.5") < parse("-1.4"));
        assert!(parse("0.1") < Exact::from_f64(0.1).unwrap());
        assert_eq!(Exact::from_f64(0.5).unwrap(), parse("0.5"));
        assert_eq!(Exact::from_f64(-3.0).unwrap(), parse("-3"));
        assert!(parse("2.0").is_integral());
        assert!(!parse("2.05").is_integral());
        assert!(parse("32767").fits_signed_bits(16));
        assert!(!parse("32768").fits_signed_bits(16));
        assert!(parse("-32768").fits_signed_bits(16));
        assert!(Exact::parse("1e999").is_none());
        assert!(Exact::parse(".").is_none());
        assert!(Exact::parse("1.2.3").is_none());
    }
}
