# Combined rename, catalog admission and partition qualification

The isolated composition merges partition source `b2d996d8a` and hardened
rename/catalog source `e897c1c73`. Prior source-scoped evidence remains
historical. Package version 1.0.489-beta is inherited; no final delivery version
is reserved. Standalone Native identity PR165 at source490 is separate.

Conflicts retain both Native instance/actor and image admission checks, exact
index identity, child-owned schema/key metadata, catalog read scopes, and
retryable partition/rename/creation/index rollback. Bounded snapshot budget
emissions carry the actual captured reader requirements; the added regression
checks every batch at four different scan budgets.

Composition `c727a6fade74c070384a9191fbf9ac0a455111c2` passed the synchronized
29-package version check and Core, SQL and protocol all-target compilation.
Compilation does not establish regression or application success.

Test-only source `54bf4151bc888ab2d3015b5e1316ba5ab8374694` failed its
required live replica-state positive: actual disk requirements were installed
while the live partition flag remained false. This was the first EmbeddedMemory
iteration; no Paged/reopen baseline outcome is claimed. Its failing log is kept.

Fix `1348dcc58e3f007d4de481b20172019f0fd86728` publishes the live partition
requirement atomically after durable capability installation and schema
invalidation. Local partition/application-scope enables also invalidate schema
compatibility. The binding fixture recomputes its real verified schema before
acquiring a fresh capture; no authority is fabricated.

Nineteen distinct focused tests passed (21 invocations, two repeats), zero failed
or ignored. They cover actual source/standby frames, all three semantic flags,
live Native state, exact acknowledged row hashes and two reopens; signed schema
and shared snapshot controls; protected bindings, bounded scans and installer
refusal; format, replication and snapshot reader fences. Frozen old wire
decoders are unit controls, not a claim that an old external binary ran here.

Source `e2a49d96dba1e07a5985e60fcf996890647b0fbc` adds a SQL partition
replication/read/local-promotion positive and reruns the unchanged original
promoted undecided-rename positive. Both are pending at this checkpoint. Native
portable collection birth, accepted provenance and its read-only resolver
remain unimplemented. Names, aliases, SQL OIDs and donor UUIDs cannot substitute.

Final-source broad suites, fault/VM evidence, original full Oracle/PostgreSQL
inventories, UI400/encrypted backup/full restore, physical HA and automatic
capacity qualification remain required. No current-composition production CLI,
release or deployment is produced. Experimental production behavior remains
disabled, there is no new cloud spend or promotion, and the full goal is active.
