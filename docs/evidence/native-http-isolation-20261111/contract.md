# HTTP idempotency and authored transaction isolation

A native idempotent HTTP callable opens its deduplication transaction before the
behavior program. It must supply the minimum isolation of the signed program's
outer transaction blocks. Starting every wrapper at Read Committed rejects a
valid Serializable or Repeatable Read command before its body can run.

The host follows statically resolved local action/function calls and examines
outer transaction declarations through branches, loops and resilience blocks.
It conservatively selects the strongest such outer declaration for the wrapper.
It stops at transaction boundaries: an explicitly Read Committed block containing
a stronger nested block still fails the existing strengthening guard and rolls
back. No guards, resource policies, signed capabilities, request fingerprints,
identity scope, receipt writes or commit/rollback boundaries are removed.

The inference does not execute expressions, use request-selected isolation or
follow foreign service authority. A cross-application orchestration that needs a
stronger transaction must declare the appropriate outer transaction at its HTTP
entry point. Undeclared callables retain the existing Read Committed default.

Qualification uses Node with officially compiled signed fixtures, authenticated
native HTTP writes and replay, negative nested-isolation and role tests, plus
the ERP comparative statement capture and exact posting-proof reconciliation.
The former 477 HTTP failure is preserved separately. A compiler preflight is not
runtime qualification. The selected ordinary-native deployment and strict Cell
signed-SQL restriction remain distinct. No Rust tests are requested or run.
