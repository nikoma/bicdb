# Exact proportional intervals

The ordinary native application runtime implements the versioned pure scalar
`finance.proportional_interval(total, quantity, quantity_precision, start_atoms,
end_atoms, amount_scale) -> Decimal`. Arguments are strictly Decimal, Decimal,
Int, String, String, Int, all positional. It supplies arithmetic, not ownership,
posting, database or tenant authority.

`total >= 0`, `quantity > 0`, quantity precision is 0..12 and amount scale 0..8.
Derive integer `Q = quantity * 10^quantity_precision` exactly; a fractional atom
is refused. Endpoints use canonical unsigned ASCII integer text, at most 41
digits, without signs, leading zeros, whitespace or scientific notation.
Require `0 <= start < end <= Q`, including when total is zero.

Let `P(k) = half_even(total * k * 10^amount_scale / Q)` as an exact integer.
The result is `(P(end) - P(start)) * 10^-amount_scale`. Bounded BigInt
intermediates retain both absolute prefixes, even when they individually
exceed Decimal. Only their final difference becomes Decimal, after exact
trailing-zero normalization and a 96-bit coefficient check. An unrepresentable
result fails explicitly. Numerators are below 2^259 and denominators below
2^230; a 256-bit accumulator alone is insufficient. No Float is involved.

Behavior programs and native invariants share the same implementation and exact
Decimal decoder. Native invariant admission checks the six argument types
against actual signed local/publication/projection field types, in addition to
the call metadata. Existing locks, source authority and posting invariants are
unaffected by this calculation. VM budgets and transaction limits are unchanged.

Packages require signed `exact_proportional_intervals`. Activation records the
sticky `exact_proportional_intervals_v1` storage feature before schema/program
activation, so old readers refuse. Removing a package does not remove that
compatibility fence. Recovery and source rollback must retain a capable binary;
binary downgrade is not supported after activation. Other targets, including
Cell, require independent implementation and qualification and are refused by
the official compiler for this API.

Implementation, compiler admission, pure arithmetic tests, actual accounting
workflows and release qualification are separate states. Runtime qualification
must record the exact compiler, package and native binary hashes; exercise
independent BigInt vectors, native guard tampering, unsupported old readers,
tenant/role boundaries, atomic failures, restart and cold restoration; and then
run the real stock and financial workflows. Source checks alone prove none of
those execution requirements.
