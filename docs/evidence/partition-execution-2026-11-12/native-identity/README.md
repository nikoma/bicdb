# Native authority for generated primary-key updates

Test-only source `3545a445` over `5aa92dce` exposed two failures among four
executed tests. A generated primary-key UPDATE attempted Native DELETE with an
UPDATE grant. An UPDATE without a Native grant correctly refused the write but
reported XX000 rather than 42501. The original mutation-authority test and the
new signed same-leaf UPDATE control passed. The original failed reports and
assertions are retained.

The first implementation, `a05c66fb`, passed **11 Core grant tests and 57 SQL
integration tests**, zero failures or ignored. The SQL cases include generated
keys, protected routing without grants, a legitimate signed UPDATE, physical
key removal, partition operations, JSONB keys, triggers and original concurrent
signed-upsert controls. The positive fixture explicitly allows Native `id`
changes as well as its SQL key/base columns; this never supplies DELETE or INSERT
authority. The original expected result and one-audit assertion were preserved.

`Transaction::update_identity_with_grant` is a Native operation within one
physical collection. It checks actual actor-bound UPDATE authority, both source
and destination addresses, explicit `id` permission, Native source/destination
row locks, a Native-read preimage, tenant/workspace scope, immutable fields,
optimistic version and grant budgets. It buffers the old-address deletion and
new-address insertion in the same Native transaction, consuming one logical
UPDATE and producing one audit containing both addresses. A Native staging
failure rolls back writes, locks and audit and restores that action's grant.
It refuses occupied destinations, including those hidden by a stale snapshot.
It grants no authority for another collection or replica origin.

The SQL integration uses this operation only for protected, same-collection key
replacement. It retains BLOB-owner and index-projection handling, validates the
full UPDATE constraints, and relies on the existing outer statement rollback
for late SQL failures. MutationDenied now maps to SQLSTATE42501. Cross-collection
moves retain their existing physical capability checks; this change does not
invent a destination capability from the source grant.

Source `e77cc2065` additionally binds the source to Native-observed versions and
reads its current locked preimage. A caller-supplied future statement snapshot
cannot override those checks, and a source observation never authorizes the
destination's version. Test-only `0031d197` adds late SQL CHECK failure rollback,
including old record hash, old/new IDs, audit/write queues, indexes and reopens.
At exact `0031d197`, all **12 Core grant tests and 58 SQL integration tests
passed**: seventy total, zero failed or ignored. This includes both original
regressions, the future-snapshot defense and late SQL constraint rollback.

A complete Core/SQL/protocol run for exact `0031d197` is queued behind that focused
batch and the useful older `5aa92dce` SQL/protocol run. It requires an actual
passing focused receipt before starting, and builds/pins current external Core
helpers first. Previous Core, protocol and VM results cover their recorded
sources; they do not automatically qualify this new Native API. Independent
fault/VM evidence for the authorized identity-change path remains required.
The source is private and unmerged; there is no new delivery version, standard
binary, release or deployment. The full application/HA/portable-origin campaign
remains active.

`raw-manifest.json` binds the original reports/logs and controller specifications.
All three focused/baseline reports are terminal and archived. Logs are losslessly
gzip-compressed. `manifest.json` binds the
stored files. All Core and SQL regression sources are tracked in this branch.
