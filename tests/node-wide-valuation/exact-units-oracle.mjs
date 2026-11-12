// Independent BigInt specification. This does not execute or qualify Rust/Core.
export const MAX96 = (1n << 96n) - 1n;
export const UNITS_LIMIT = 1n << 254n;
export const LIST_CAP = 100_000;
const fail = category => { const error = new Error(category); error.category = category; throw error; };

// The existing shared native decoder admits exactly representable strings and
// JSON numbers. Type provenance in a signed program/SQL expression is a separate
// compiler/ABI gate; this oracle does not claim Carrier wire coercion support.
export function nativeDecimal(value) {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) fail('decimal');
    value = String(value);
  }
  if (typeof value !== 'string') fail('decimal');
  const m = /^([+-]?)([0-9]+)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$/.exec(value);
  if (!m) fail('decimal');
  let coefficient = BigInt(m[2] + (m[3] ?? ''));
  const exponent = BigInt(m[4] ?? '0');
  if (exponent < -(1n << 63n) || exponent > (1n << 63n) - 1n) fail('decimal');
  if (coefficient === 0n) return {coefficient: 0n, scale: 0};
  let scale = BigInt((m[3] ?? '').length) - exponent;
  while (scale > 0n && coefficient % 10n === 0n) { coefficient /= 10n; scale--; }
  if (scale > 28n || scale < -28n) fail('decimal');
  if (scale < 0n) { coefficient *= 10n ** -scale; scale = 0n; }
  if (coefficient > MAX96) fail('decimal');
  return {coefficient: m[1] === '-' ? -coefficient : coefficient, scale: Number(scale)};
}

function scale(value, maximum) {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > maximum) fail('scale');
  return value;
}

export function toUnits(value, precision) {
  const {coefficient, scale: source} = nativeDecimal(value);
  const exponent = precision - source;
  if (exponent >= 0) return coefficient * 10n ** BigInt(exponent);
  const divisor = 10n ** BigInt(-exponent);
  if (coefficient % divisor !== 0n) fail('subunit');
  return coefficient / divisor;
}

export class UnitsAccumulator {
  #total = 0n;
  #count = 0n;
  #scale;
  constructor(precision) { this.#scale = scale(precision, 28); }
  add(value) {
    const term = toUnits(value, this.#scale);
    const count = this.#count + 1n;
    if (count >= 1n << 64n) fail('count');
    this.#total += term;
    this.#count = count;
  }
  finish() {
    if (this.#total <= -UNITS_LIMIT || this.#total >= UNITS_LIMIT) fail('units_bound');
    return this.#total.toString();
  }
}

export function sumUnits(args) {
  if (!Array.isArray(args) || args.length !== 2) fail('arity');
  const [values, precision] = args;
  if (!Array.isArray(values)) fail('list');
  if (values.length > LIST_CAP) fail('list_cap');
  const sum = new UnitsAccumulator(precision);
  for (const value of values) sum.add(value);
  return sum.finish();
}

function unsigned(text) {
  if (typeof text !== 'string' || text.length > 77 || !/^(0|[1-9][0-9]*)$/.test(text)) fail('units_grammar');
  const value = BigInt(text);
  if (value >= UNITS_LIMIT) fail('units_bound');
  return value;
}

export function fromUnits(coefficient, precision) {
  let normalized = coefficient, remaining = precision;
  while (remaining > 0 && normalized % 10n === 0n) { normalized /= 10n; remaining--; }
  if (normalized > MAX96) fail('result_unrepresentable');
  const raw = normalized.toString().padStart(remaining + 1, '0');
  return remaining ? raw.slice(0, -remaining) + '.' + raw.slice(-remaining) : raw;
}

function nearestEven(numerator, denominator) {
  const quotient = numerator / denominator, remainder = numerator % denominator;
  // Deliberately use r compared with Q-r rather than reproducing Core's 2*r.
  return quotient + (remainder > denominator - remainder ||
    remainder === denominator - remainder && quotient % 2n !== 0n ? 1n : 0n);
}

export function prorateUnits(args) {
  if (!Array.isArray(args) || args.length !== 5) fail('arity');
  const C = unsigned(args[0]), Q = unsigned(args[1]);
  const operated = nativeDecimal(args[2]);
  const p = scale(args[3], 12), s = scale(args[4], 8);
  if (Q === 0n || operated.coefficient <= 0n) fail('quantity_domain');
  const q = toUnits(args[2], p);
  if (q > Q) fail('quantity_capacity');
  return fromUnits(nearestEven(C * q, Q), s);
}

export function displayRatio(args) {
  if (!Array.isArray(args) || args.length !== 4) fail('arity');
  const C = unsigned(args[0]), Q = unsigned(args[1]);
  const p = scale(args[2], 12), s = scale(args[3], 8);
  if (Q === 0n) fail('quantity_domain');
  // Factor the exponent on the appropriate side instead of Core's separately
  // expanded numerator and denominator. Every candidate rounds from the exact
  // rational, not from a previous rounded candidate.
  for (let d = 28; d >= 0; d--) {
    const exponent = p + d - s;
    const N = exponent >= 0 ? C * 10n ** BigInt(exponent) : C;
    const D = exponent >= 0 ? Q : Q * 10n ** BigInt(-exponent);
    const coefficient = nearestEven(N, D);
    if (coefficient <= MAX96) return fromUnits(coefficient, d);
  }
  fail('rate_unrepresentable');
}

export const OPERATIONS = {
  exact_decimal_sum_units_value: sumUnits,
  exact_prorate_units_value: prorateUnits,
  nearest_native_display_ratio_value: displayRatio,
};

export function outcome(operation, args) {
  try { return {value: OPERATIONS[operation](args)}; }
  catch (error) { return {error: error.category ?? error.message}; }
}
