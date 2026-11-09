# Oracle case profiles and native indexes

Oracle profiles use frozen, versioned UPPER/LOWER maps from the saved native
Oracle 23.26.3 reference. AL32UTF8 and AL16UTF16 are distinct contracts: their
saved maps differ at twenty uppercase inputs and two lowercase inputs. SQL
national literals, national table columns, derived queries, views and CTEs must
retain national provenance before ordinary text transport erases it. PostgreSQL
and legacy native ASCII index tags keep their existing behavior.

Profile initialization enables the sticky `oracle_simple_case_v1` storage
capability before publishing case-profile metadata. An older reader lacking the
capability must refuse the store. SQL refuses an Oracle database until startup
has published its case readiness record. This is a startup operation on a
healthy, quiescent database, before sessions are served.

For each legacy case-expression B-tree, startup records its original definition
and a unique shadow definition before allocating the shadow. It builds and
verifies the shadow, then uses native atomic catalog/generation publication to
replace only case-family semantics. The original definition, collection,
uniqueness policy, predicates and unrelated literals remain protected by native
publication checks. Existing physical generations remain available to retained
readers. Publication retries retain the durable original and shadow identities.
Only an owned, unpublished paged build can restart after an operator repairs
conflicting rows; its checkpoint clock is preserved across restarts. An invalid
ownership record, conflicting shadow, changed source or uniqueness collision
prevents readiness. Repair does not silently discard rows or weaken uniqueness.

After every pending ownership record and legacy index is resolved, startup writes
the readiness marker. A crash after native publication and before journal removal
retries the same publication idempotently. A crash before readiness leaves SQL
fenced until startup resumes. New SQL indexes select their versioned native case
family from durable column provenance. National declarations carry fenced Oracle
profile metadata version 9, separate from routine-local national buffer metadata.

Focused source `9f3780a3` passes nine integration tests: eight profile/case
regressions plus the saved native national-assignment fixture, including both
storage modes, unique-collision repair and resume, native index keys, rollback,
reopen, view/CTE/derived aliases and national `%TYPE` routine buffers. The scalar
checks compare all twenty-two saved character-set differences. Earlier source
`55ac5135` passed the SQL library with 818 passes, zero failures and four existing
ignores; the later anchor changes require their own broad qualification. A prior
combined compile was interrupted at its 12 GiB cgroup limit before tests ran;
subsequent builds use one compiler job and a monitored 16 GiB limit.

Qualification is recorded by source revision. The recovered native foundations
have their own older mapping/publication evidence under
[oracle-simple-case-v1](evidence/oracle-simple-case-v1/); those results do not
qualify this integrated source. Focused integrated results and failures are kept
in the task's source-scoped qualification ledger. Broad SQL/core suites, frontend
checks, final-head CI, remote main verification, and final artifact/application/VM
qualification are distinct gates.

The first draft of the new national SQL test incorrectly expected different
Deseret mappings. Both saved native references uppercase Deseret identically;
that failure is retained as a test-fixture error and is not an engine defect.
The corrected cases use actual native differences, including U+019B, and compare
all twenty-two differences directly with the saved vendor JSON.

Exclusion-index case migration requires a separately qualified native rebuild and
is refused. This change does not claim generic writable views, universal
transactional catalog support, a complete Oracle dictionary, full application
acceptance, or a production deployment.
