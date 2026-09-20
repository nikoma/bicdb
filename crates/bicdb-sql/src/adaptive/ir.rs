//! Execution IR — Phase 2, Slice A: scalar integer/boolean expressions.
//!
//! This is the first concrete slice of the backend-agnostic Execution IR. It
//! models pure scalar expressions over a row of `i64` input slots — the unit
//! that the WASM backend ([`super::wasm`]) compiles, and the substrate that a
//! later slice will lower `BoundExpr` (the interpreter's bound expression tree)
//! into.
//!
//! Scope is deliberately narrow so the *pipeline* (IR -> codegen -> Wasmtime ->
//! verify) can be proven correct before widening:
//!
//! * Two logical types: `Int` (i64) and `Bool`. Booleans are canonically
//!   encoded as `0`/`1` in an `i64`, exactly as the WASM backend represents
//!   them, so the interpreter here and the compiled module are bit-comparable.
//! * Total operations only — wrapping integer arithmetic (matching WASM's
//!   2's-complement `i64.add`/`sub`/`mul`), signed comparisons, and boolean
//!   logic on canonical `0`/`1`. No division (trap semantics), no floats, no
//!   NULLs yet — those are later slices.
//!
//! [`eval_i64`] is the verification oracle: it mirrors the WASM codegen's
//! semantics exactly, so any divergence the verifier reports is a real codegen
//! bug, not a semantic mismatch baked into the comparison.

/// Logical result type of an [`IrExpr`]. Both are carried in an `i64` at the
/// ABI level; this distinguishes how a caller should interpret the result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IrType {
    Int,
    Bool,
}

/// Comparison operators (signed integer comparison, boolean result).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

/// A pure scalar expression over a row of `i64` input slots.
#[derive(Clone, Debug, PartialEq)]
pub enum IrExpr {
    /// Integer literal.
    ConstInt(i64),
    /// Boolean literal.
    ConstBool(bool),
    /// Reference to input slot `n` (an `i64`, logically `Int`).
    Col(u32),
    /// Wrapping integer negation.
    Neg(Box<IrExpr>),
    /// Wrapping integer add / sub / mul.
    Add(Box<IrExpr>, Box<IrExpr>),
    Sub(Box<IrExpr>, Box<IrExpr>),
    Mul(Box<IrExpr>, Box<IrExpr>),
    /// Signed integer comparison -> Bool.
    Cmp(CmpOp, Box<IrExpr>, Box<IrExpr>),
    /// Boolean conjunction / disjunction (operands canonical 0/1).
    And(Box<IrExpr>, Box<IrExpr>),
    Or(Box<IrExpr>, Box<IrExpr>),
    /// Boolean negation.
    Not(Box<IrExpr>),
}

/// Type-check an expression against `num_cols` available `Int` input slots.
/// Returns the logical result type, or an error describing the first
/// ill-typed node. A well-typed expression is guaranteed to produce canonical
/// `0`/`1` for every `Bool` subexpression, which is what makes [`eval_i64`] and
/// the WASM codegen agree bit-for-bit.
pub fn type_of(expr: &IrExpr, num_cols: u32) -> std::result::Result<IrType, String> {
    use IrExpr::*;
    let require = |e: &IrExpr, want: IrType| -> std::result::Result<(), String> {
        let got = type_of(e, num_cols)?;
        if got == want {
            Ok(())
        } else {
            Err(format!("expected {want:?}, found {got:?}"))
        }
    };
    match expr {
        ConstInt(_) => Ok(IrType::Int),
        ConstBool(_) => Ok(IrType::Bool),
        Col(i) => {
            if *i < num_cols {
                Ok(IrType::Int)
            } else {
                Err(format!(
                    "column slot {i} out of range (num_cols={num_cols})"
                ))
            }
        }
        Neg(a) => {
            require(a, IrType::Int)?;
            Ok(IrType::Int)
        }
        Add(a, b) | Sub(a, b) | Mul(a, b) => {
            require(a, IrType::Int)?;
            require(b, IrType::Int)?;
            Ok(IrType::Int)
        }
        Cmp(_, a, b) => {
            require(a, IrType::Int)?;
            require(b, IrType::Int)?;
            Ok(IrType::Bool)
        }
        And(a, b) | Or(a, b) => {
            require(a, IrType::Bool)?;
            require(b, IrType::Bool)?;
            Ok(IrType::Bool)
        }
        Not(a) => {
            require(a, IrType::Bool)?;
            Ok(IrType::Bool)
        }
    }
}

/// Reference interpreter / verification oracle. Returns the canonical `i64`
/// encoding (Bool as `0`/`1`). Mirrors the WASM backend's semantics exactly:
/// wrapping arithmetic, signed comparison, bitwise logic on canonical booleans,
/// and `== 0` negation (matching `i64.eqz`).
///
/// Assumes `expr` is well-typed (see [`type_of`]) and every `Col(i)` is in
/// range of `row`; callers verify shape first.
pub fn eval_i64(expr: &IrExpr, row: &[i64]) -> i64 {
    use IrExpr::*;
    match expr {
        ConstInt(v) => *v,
        ConstBool(b) => *b as i64,
        Col(i) => row[*i as usize],
        Neg(a) => eval_i64(a, row).wrapping_neg(),
        Add(a, b) => eval_i64(a, row).wrapping_add(eval_i64(b, row)),
        Sub(a, b) => eval_i64(a, row).wrapping_sub(eval_i64(b, row)),
        Mul(a, b) => eval_i64(a, row).wrapping_mul(eval_i64(b, row)),
        Cmp(op, a, b) => {
            let x = eval_i64(a, row);
            let y = eval_i64(b, row);
            let r = match op {
                CmpOp::Lt => x < y,
                CmpOp::Le => x <= y,
                CmpOp::Gt => x > y,
                CmpOp::Ge => x >= y,
                CmpOp::Eq => x == y,
                CmpOp::Ne => x != y,
            };
            r as i64
        }
        // Operands are canonical 0/1 for well-typed Bool exprs, so bitwise &/|
        // equals logical and/or and matches the WASM `i64.and`/`i64.or` codegen.
        And(a, b) => eval_i64(a, row) & eval_i64(b, row),
        Or(a, b) => eval_i64(a, row) | eval_i64(b, row),
        Not(a) => (eval_i64(a, row) == 0) as i64,
    }
}
