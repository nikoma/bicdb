# Logical wildcard projection through join planning

An application background query exposed a general projection defect: a join's
physical relation order and internal alias bindings leaked into `SELECT *`.
The query returned 162 columns rather than 81, and prepared column metadata could
associate a timestamp label with a numeric value. Qualified source projections
read correctly. The prior observation remains in the
[sales-order evidence](../idempiere-sales-order-complete-2026-11-03/join-projection-baseline.json).

Wildcard expansion now follows logical SQL source order while reading the
corresponding physical row slots. Optimized join order remains enabled. Internal
alias copies are excluded, genuine repeated labels retain their own slots, and
positional ordering reads the selected slot with its original type. No application,
schema or table identity selects the runtime behavior.

The [reference fixture](../../../examples/oracle/reference/join_wildcards.py)
matches **23/23 Oracle cases** and **23/23 PostgreSQL cases**, compared with
**6/23** and **3/23** on the prior binary. See [Oracle](oracle-controls.json),
[PostgreSQL](postgres-controls.json) and their [Oracle baseline](oracle-baseline.json)
and [PostgreSQL baseline](postgres-baseline.json). The same fixture digest is
recorded for each final before/after comparison; all final source expectations
qualify and cleanup reports no errors. Labels are compared after case folding;
numeric types are compared as a transport family rather than identical OID widths.

Cases include reordered inner/comma/cross joins, matched and unmatched outer joins,
empty results, qualified/repeated/mixed wildcards, three relations, derived tables,
CTEs, stored views and positional sorting through repeated labels. An intermediate
[18/19 Oracle comparison](oracle-duplicate-ordinal-baseline.json) caught a second
bug: sorting a repeated output label could select the first occurrence. That
failure is retained; the final slot-based implementation passes both ordinal cases.

Stored-view reference forms differ explicitly: PostgreSQL tests a declared-column
view defined by a joined wildcard; Oracle tests unique explicit aliases in the
view definition. In this Oracle reference environment the former definition
creates successfully, but both reads raise ORA-25716. The
[initial failed comparison](oracle-initial-view-comparison.json) and
[isolated observation](oracle-declared-view-observation.json) are retained.
The declared-column joined-wildcard form remains unqualified against Oracle;
these results do not establish the cause or generality of that source error.
Native Rust integration tests cover the joined-wildcard view after database reopen.

A [copy of the stopped application database](application-projection.json) now
returns exactly 81 columns for the actual background join, with labels and values
equal to the concatenated qualified source projections. Ordinary PostgreSQL-driver
loaders decode all values successfully, including the timestamp. The completed
order, its two-unit reservation and inventory quantities survive reopen on this
binary. This is a bounded read check, not a rerun of background accounting or
qualification of its memory behavior.

Four new SQL integration tests cover optimized join reordering, empty metadata,
view reopen, repeated labels, repeated stars, CTEs, array/JSON set-returning paths,
and typed positional sorting. A wire regression checks prepared Describe and
binary timestamp/numeric values in both native and Oracle preview profiles.

Compiler version remains 26 and durable metadata format remains 5. NATURAL/USING
merged-column semantics, all lateral and quoted-identifier variants, every
volatile/SRF ordering combination, legacy view metadata, background memory,
complete application workflows and the wider compatibility programme remain open.
This is development source/binary qualification, not a published release or a
verified production deployment.

Local validation passes **1,501 Rust test executions in 21 batches**, with three
existing ignores, plus 58 Python tests. Formatting, licenses and the dependency
boundary checks pass. Independent clock controls retain [75/75 Oracle](clock-oracle-controls.json)
and [12/12 PostgreSQL](clock-postgres-controls.json). The
[validation record](validation.json) records source/binary provenance and resources.
The guarded build/test run peaked at 4.79 GiB process-group RSS and retained at
least 20.4 GiB host available RAM. Cleanup reclaimed 4.69 GiB of completed test
executables; the task target remains approximately 7.5 GiB. Frozen binaries and
reusable dependencies were preserved.
