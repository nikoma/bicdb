# Exact in-memory Decimal Sum

Native database and invariant SUM already use widened exact accumulation.
In-memory Carrier queries previously lost aggregate intent in the semantic
compiler and became ordinary scalar-add loops. For `[MAX96, .01, -MAX96]` this
can silently return zero. Fixing database SUM does not repair that lowering.

The compiler retains a typed, compiler-generated aggregate marker and the
existing fallback body. Ordinary native BicDB emits the internal
`query.decimal_sum` call with a Decimal result and one Decimal-item list. This
is compiler metadata, not generated-name or loop-shape matching. It grants no
database, actor, posting or source authority. Source query preprocessing and
capture evaluation remain in their original order.

The native operation validates every nonnull input as an exactly representable
96-bit Decimal before widening. Invalid individually unrepresentable terms may
not cancel each other into acceptance. Nulls are skipped; empty/all-null inputs
return zero. BigDecimal accumulation never rounds intermediate totals. Only
the final total enters the native Decimal domain; an unrepresentable final sum
fails explicitly. Existing valid input scales are retained where representable.

Lists have a structural bound of 100,000 elements. Each element also consumes
the existing invocation step/deadline budget, so the bound is not a promise
that every surrounding query can process that many items. Limits are unchanged.
No Float conversion or SQL fallback is involved.

Packages require signed `exact_decimal_list_sum`; activation records sticky
`exact_decimal_list_sum_v1` before exposing the new code, making old readers
refuse. Compiler/runtime pins and real source-derived execution are required.
Previously compiled scalar-loop packages need rebuilding; the runtime does not
guess which ordinary additions once came from an aggregate.

This change concerns in-memory Decimal Sum. Integer/Float and SQL aggregates,
ordinary Decimal arithmetic, and other backend fallback bodies retain their
existing behavior. Decimal Average still needs a separate widened
sum-through-division implementation, including `[MAX96, MAX96] -> MAX96`; it is
an open compatibility gap and is not established by Sum qualification.
