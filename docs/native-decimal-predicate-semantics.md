# Decimal predicate comparison generations

A persisted predicate definition does not identify the comparison rules used
to build its index. In particular, legacy Decimal comparisons rejected values
with more than 18 fractional digits. Exact comparison can make an existing row
qualify without changing the predicate JSON. Reusing the old physical index
would omit that row and can break partial uniqueness.

The Native index catalog records comparison version 1 with authoritative
B-tree generation bindings. The required format flag
`decimal_predicate_comparison_v1` makes older readers refuse the upgraded store.
The existing index-definition contract permits predicates only on B-trees and
excludes predicates from exclusion indexes.

On opening a legacy store with affected indexes, Core holds the database
ownership lock and returns no public handle until repair finishes. A durable
plan records the original catalog, physical bindings, source commit horizon
and exact owned shadow definitions. Each shadow uses the bounded Native build
and verification protocol. Definition, replacement binding and completed plan
progress share one atomic catalog publication. Retired physical entries remain
intact. After all affected indexes verify and a full checkpoint succeeds, Core
publishes the current comparison certificate and removes the plan atomically.
An interrupted open resumes and validates this plan before admitting callers.
Embedded storage reconstructs its indexes from authoritative records and
verifies them before certification.

Legacy repair requires fsync and secondary-index loading. Recovery/export
configuration cannot silently admit stale predicate generations. Changed or
unsupported plans, definitions, bindings or source horizons refuse admission.
If exact comparison introduces a unique-key conflict among existing rows,
opening fails and preserves both rows. Resolving business data conflicts
requires a separately controlled recovery; the upgrade never drops rows or
relaxes the constraint. Failed durable publication retains the normal Native
durability fence and reopen verification protocol.

See [source-scoped qualification](evidence/decimal-predicate-generation-transition-2026-11-12/README.md).
The development implementation and its focused evidence are separate from a
published release, final application qualification and mixed-version HA
support. Do not infer those delivery states from the presence of these files.
