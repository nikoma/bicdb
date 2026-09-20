# BicDB / Carrier / Hub integration status — 2026-09-07

Transaction-scoped verified delegation and the compatibility changes are being
integrated into main. Integration into main does not certify production deployment.

Verified on the isolated remote build host: BicDB SQL tests (307 passed, 3 ignored),
pgwire tests (89 passed), workspace check including all targets, and Carrier
migration transaction-wrapper tests (2 passed). Earlier focused Carrier runtime
and generator checks are recorded in the delegation documentation.

A fresh Hub database completed all migrations. The full acceptance run has not
completed application-role grants, live multi-user API checks, encrypted backup
restore verification, or restart durability as a complete sequence. Social feed,
post, comment, and reaction flows still need end-to-end verification. Do not
claim Hub is production-ready on BicDB or that a user-capacity target is proven.

GitHub-hosted BicDB checks were unavailable for this validation run.
Hub CI failed its migration safety gate on the pre-existing generated migration
0115: its DROP POLICY statements lack a matching approval record. This was already
in the base main source; it remains a real CI blocker, not a passing test. Carrier
GitHub checks were queued when inspected. No checks or protections were disabled.

Continue the remaining integration checks and fixes on main or merge temporary
working branches back into main promptly. Preserve RLS and signed operation guards.
