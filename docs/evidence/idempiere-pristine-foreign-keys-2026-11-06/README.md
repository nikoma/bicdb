# Pristine source-declared foreign-key migration

The original-source schema target completed all **3,826 foreign keys** through
the public migration phase. Before/after full-column multiset fingerprints match
for all **888 affected tables**, containing **181,367 target rows**. The phase's
capability probe passed and its final journal records `migration_completed` and
`success`. [summary.json](summary.json) retains report, declaration, executable,
source-tool and snapshot digests without publishing application row values.

| Preserved source state | Relationships |
| --- | ---: |
| VALIDATED | 912 |
| NOT VALIDATED | 2,914 |
| DEFERRABLE INITIALLY DEFERRED | 3,826 |
| ON DELETE CASCADE | 447 |
| ON DELETE SET NULL | 170 |
| NO ACTION | 3,209 |

The timing row covers all relationships; deletion rows form a separate partition.
Source `NOT VALIDATED` becomes `NOT VALID`, retaining future-write enforcement.
Definitions preserve ordered child/parent columns, referenced primary/unique
keys, actions, timing, validation and declared enforcement. Table enforcement
switches are checked separately. Missing relationships receive native DDL and
post-install catalog readback; completed/unjournaled entries are accepted only
through exact source/catalog checks. No orphan repair or implicit trigger
enabling was performed.

The native development executable was built from
`5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`; final resumed tooling was frozen from
`2312aceb00b4f26451fc25e521233ff25026eb0f`. Memory guards interrupted earlier
attempts; each resumed the preserved target only after audited clean shutdown,
journal checks and repeated baseline fingerprints. The final completed phase
finished its fingerprint checks and exited zero. Earlier interruptions remain
in the private audit history rather than being relabeled successful attempts.

Use the [migration instructions](../../oracle-foreign-key-migration.md) to
reproduce on a quiesced disposable target with a retained pre-upgrade backup.
This result proves preservation of the target baseline while installing source
relationships. It does **not** establish equality of all 915 tables with the
Oracle export, source/target orphan-count reconciliation, execution/rights of
all routines/views, complete original application tests, cutover/recovery or
production qualification. Those remain separate gates; the accepted running
application was not upgraded by this migration.
