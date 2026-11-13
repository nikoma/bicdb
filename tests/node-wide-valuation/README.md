# Exact unit arithmetic leaf review

These are independent Node specifications and portable vectors. They do not execute the Rust leaf or qualify a compiler, ABI, SQL terminal, model invariant, ERP workflow or release.

Run:

```sh
node --test tests/node-wide-valuation/exact-units-oracle.test.mjs
node tests/node-wide-valuation/exact-units-vectors.mjs /tmp/exact-units-vectors.json --include-limits
```

The corpus has 350 cases with list boundaries enabled, plus three raw native-only Int-kind cases. JavaScript's JSON parser loses the distinction between `2`, `2.0` and `2e0`; those raw representations need real native/ABI execution. The 100,000-item cases describe the existing Core wrapper element cap. Outer request/structural admission can refuse earlier; neither they nor the Node streaming test are native SQL/ERP capacity claims.

`ordinary-division-b998.json` contains 169 captured HTTP observations of the existing native Decimal `/` on b998. Exact unit re-encoding reproduces 149 accepted numeric results and 20 expected zero/overflow refusals. Raw formatting of historical Decimal scales is preserved in that evidence, not promised by the normalized new display operation. Wide `MAX+.01` display is explicitly **new domain semantics**, since its ordinary numerator cannot be a native Decimal; it is not an ordinary-operand equivalence claim or financial authority.

The new, unwired Core module exposes:

```text
ExactDecimalUnitsSum::new(scale: i64) -> Result<Self>
ExactDecimalUnitsSum::add(&mut self, value: &Value) -> Result<()>
ExactDecimalUnitsSum::finish(self) -> Result<String>
exact_decimal_sum_units_value(arguments: &[Value]) -> Result<Value>
exact_prorate_units_value(arguments: &[Value]) -> Result<Value>
nearest_native_display_ratio_value(arguments: &[Value]) -> Result<Value>
```

The streaming state validates every nonnull exact native Decimal term and its integrality at the unit scale before adding it, including cancelling subunit terms. It tracks `u64` count and final signed magnitude below `2^254`. The list entry point retains `MAX_EXACT_DECIMAL_LIST_SUM_ITEMS`; the state does not materialize database history. The SQL integration must propagate `add` errors and must not silently apply ordinary SQL SUM's null-skipping behavior. Empty input finishes as canonical `"0"`.

All Decimal admission delegates to the existing `json_decimal_exact`, which admits exactly representable internal JSON strings/numbers. Actual signed argument provenance/type enforcement remains a compiler/ABI responsibility, as for the existing interval leaf. No Float algorithm, lossy parse or scalar coercion was added.

Proration uses `[String C, String Q, Decimal operated, Int precision, Int scale]`, canonical unsigned ASCII strings of at most 77 digits and magnitude below `2^254`, frozen quantity precision `0..12`, amount scale `0..8`, `Q>0`, positive integral operated atoms and `q<=Q`. All domains are checked for zero C too. One quotient/remainder half-even operation rounds in approved minor units, then only trailing-zero normalization can make the final amount fit the 96-bit coefficient. An unrepresentable result fails. It does not replace current-remaining moving weighted average with original-total prefix allocation.

The display operation uses `[String C, String Q, Int precision, Int scale]` and separately chooses the greatest native scale `0..28` whose once half-even rounded coefficient fits. It is nonauthoritative; reports and postings must not use quantity times this rate as carrying value. Neither scalar `/` nor an approved financial rounding policy changes.

The largest proration product is below `2^390`; the code uses bounded-input `BigInt`, not a U256 product. No model/currency/tenant/posting authority, string codec, resource-limit increase, version bump, Core export, SQL wiring, ABI feature or Carrier call site is included here. Root owns those integration and real native qualification gates. No Cargo build, Rust test or standalone Rust helper was used for this handoff.
