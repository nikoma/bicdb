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

## Wide unit operations

The separate signed application feature `exact_decimal_units` admits
`finance.sum_units(Decimal[], Int) -> String`,
`finance.prorate_units(String, String, Decimal, Int, Int) -> Decimal`, and
`finance.display_ratio_units(String, String, Int, Int) -> Decimal`.
Activation records sticky storage feature `exact_decimal_units_v1` before
exposing the application so older readers refuse the database. The strict SDK
call signatures are validated even in unreachable program expressions.

`sum_units` requires every nonnull Decimal operand to be individually integral
at the requested scale (0 through 28). Nulls are refused. Empty input returns
`"0"`. The list wrapper retains the 100,000 item bound and invocation charging.
The final canonical signed unit magnitude must be less than `2^254`.

The SQL aggregate `bicdb_exact_sum_units(Decimal, Int)` shares the same streaming
accumulator without a Decimal subtotal or application list limit. It runs over
the genuine authorized database relation and owning transaction. Its scale is
constant for the statement, including typed statement parameters. DISTINCT,
FILTER and window forms are refused. Decimal schema typing is mandatory; null,
Float, Bool and container coercions fail. Carrier's standalone `.SumUnits(scale)`
terminal carries an explicit compiler-only native feature marker.

The SQL execution context records the current signed application's allowance
as trusted session/engine state, never a GUC or a transaction-wide union of past
allowances. Query AST checks cover nested SQL, empty relations and LIMIT 0.
Routine execution, SECURITY DEFINER, autonomous sessions, parallel expression
workers and `ts_rewrite` must preserve that context. Local operator SQL retains
its operator context; an application cannot select it. Genuine capability and
parameter errors have typed origins (42501 and 22023); matching RAISE strings
do not acquire those origins. The host restores its owning transaction before
error mapping and rolls back failed deferred validation.

An application transaction's native mutation actor also preserves its ownership
through SQL calls. SQL cannot finish that host-owned unit with COMMIT, full
ROLLBACK, AND CHAIN, nested BEGIN or independent autonomous execution. Refusal
occurs before hooks or durable effects, with a typed `2D000` origin mapped to an
application capability denial. Local savepoints and internal error rollback
remain available; ordinary operator transactions retain their control semantics.
The host alone performs the application's final commit or rollback.

Proration takes canonical unsigned ASCII cost units and positive quantity atoms,
each less than `2^254`, with no signs, leading zeros except `"0"`, whitespace or
exponents. Quantity precision is 0 through 12 and amount scale 0 through 8.
Operated Decimal quantity must be positive, integral at that precision and no
greater than the remaining quantity. The operation computes a single half-even
rounding of `remaining_cost * operated_atoms / remaining_atoms`, then admits only
a representable native Decimal result. Sequential moving-average consumers
subtract the actual resulting cost and quantity before their next operation.

The display ratio uses the greatest native scale whose once-rounded coefficient
fits the 96-bit Decimal domain. It is nonauthoritative: display rate times
quantity must never price inventory or COGS. Ordinary scalar division, source
ownership, posting authority, quotas and financial locks are unchanged.
Native invariant calls to these three new operations and Cell execution are
not admitted. Source implementation and preflight are not runtime qualification.
