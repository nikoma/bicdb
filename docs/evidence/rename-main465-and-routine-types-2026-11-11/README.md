# Main integration and routine type dependencies

Private source `7138ffa974f89ba31606d385af5a36ba44c529bb` passes ten focused
commands and 132 tests after merging delivered main `ac5e64ee` (1.0.465-beta).
The only merge conflict combines generated Boolean balancing with TABLE query
lowering at their shared parser entry. The joint regression executes TABLE CTE
bodies with 21, 100 and 1,000 appended filter terms while retaining the original
parser-safety and generated-filter assertions. All three native PostgreSQL 16.13
reference cases pass. The remaining checks cover version identity, delivered
read-committed schedules and row reservations, native/host scopes, rename ACLs,
rename recovery, views/composites, snapshots/read-only transactions and native
replication. This is focused integration, not the full workspace.

The next tests-only control `f229a4ba` fails while creating a routine with table
row type array arguments/results: `array element type source is not registered`.
It does not reach its rename assertions. Native PostgreSQL passes both commit
and rollback reference cases, retaining argument/return OIDs while routine
signatures follow the renamed row type. Reusing the old relation spelling does
not rebind those existing signatures. Temporary native schemas were removed;
protected application schemas were not changed.

Source `5e74ff58a6befce1f6e5e83ac6eb43dfa83969ba` passes all five focused
commands (52 tests). Routine declaration parsing now accepts custom array types
through the existing custom-type path while preserving registered built-in
array validation. Repeated dimensions have one PostgreSQL array type identity.
The pure routine-header rename transform is shared with user-type rename. The
rename dependency plan captures each exact routine storage row before effects,
and the journal-covered save retains its OID and overload key. It avoids
creation-default ACLs and generic session undo for those existing routine rows.

The original routine control now passes for both stores, commit/rollback, old
name reuse and reopen. Focused regressions also cover ACLs, TABLE/Boolean
parsing, views, pending composite rows, rename units, routine typmods, enum
rename, schema rename, snapshots and read-only behavior. The recorded routine
test at this revision establishes metadata/OID behavior. Actual BicDB routine
execution, expanded routine process cuts and broad SQL suites require separate
later evidence. Native routine execution references are included as references
only; their passing results are not BicDB execution results.

Complete SQL scoped admission, metadata row resources, logical type/routine/
Describe visibility and durable pending-journal admission remain open. The
separate unrelated-writer SQL producer regression is still unresolved. These
results do not authorize removing global checks and do not establish VM,
power-loss, final application or production qualification. The source versions
here are private 1.0.465-beta revisions; later main versions need separate
integration validation. No binary, release or deployment was produced. Original
application assertions and the ten-second timeout are unchanged.

Receipts preserve commands, revisions and outcomes. `test-observations.json`
retains test summaries and the original failure excerpt; `sha256s.json` binds
all included artifacts.
