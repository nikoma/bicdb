# Automatic partition admission: private development evidence

This batch is unmerged and inherits version 1.0.487-beta. It is not a standard
binary, release, deployment, complete partition implementation, application
acceptance, portable-origin receipt, distributed sharding or HA qualification.
See the adjacent legacy-upgrade evidence for the genuine old482 producer, six
immutable source stores, fourteen independent ACKs and the original eight
mandatory failures.

| Actual source | Result | Scope |
| --- | --- | --- |
| `fcf2617b` | Default8 and explicit8 pass | The original default row/policy/bounds requirements and actual old-reader controls now pass. Fourteen original ACKs and fixture hashes remain intact. |
| `fcf2617b` | 63 focused pass, 1 fail | Core5, bounded Native batch1, SQL unit4 and executed integration targets. Local child trigger writes omitted certificate publication; expected42710 became55000. Seven requested integration targets were not run after Cargo stopped at that failure. Startup integration compiled. |
| `08cbbfc0` | Default8 and explicit8 pass; SQL92 pass, 1 fail | Local trigger save/delete/undo and LOB schema changes now share atomic certificates. Original authorization setup exposes implicit `id` PK promotion that is invalid for its partition key. All eighteen requested SQL targets collected with no-fail-fast. |
| `08cbbfc0` | Extra10 pass, 4 fail | Both modes: text, AST, actual-old-binary-created procedure, read-only/shared refusal and invalid startup cases pass. COPY writes into the parent heap; valid startup tries to reuse the parent's copied PK name for children. |
| `8ed4a67b` | Default8, explicit8, extra entry/startup14 pass | COPY routes each physical leaf and child PK identities no longer reuse the parent's physical index. Text/AST/COPY/procedure first prove no read-only mutation and shared exclusive fallback; startup construction refuses ambiguity before returning a server. Applicable old readers refuse with byte identity. Both modes and two reopens. |
| `8ed4a67b` | SQL96 pass, 1 new-fixture failure | COPY atomic routing and actual-PG trigger order/visibility pass. The postcondition SELECT in the new forced-RLS COPY test has no SELECT policy. No production assertion was removed. |
| `c3588bf8` | COPY3 pass; source-scoped SQL97 total | Test-only verification policy after the allowed/refused COPY calls. Production, examples and workspace identity exactly match8ed. Reuse the other94 passing SQL checks; do not add repeated COPY passes again. |

Six Native focused checks at fcf remain applicable because Core/Page/workspace
source is byte-identical: four required-reader fence tests, one process-local
cache identity/complete-window test, and one bounded-batch memory-envelope test.
The latter reads one row under64KiB from a collection that cannot fit that budget
as a whole. It covers EmbeddedMemory and ServerPaged; it is not a general corpus
performance or hard-memory-limit qualification.

The qualified implementation/test source is c358; subsequent documentation-only
heads reuse it after an exact crates/workspace identity check. The broad Core,
SQL and pgwire unit/integration batch
is running at that exact source (`bicdb-partition-admission-broad`); no broad pass
is claimed. Application1464/unchanged10s waits, UI400 backup/restore, independent
ACK/process/fault/VM, Native protected authority, standby/HA and P1/P2 gates remain
separate. P2 remains disabled. No standard server or CLI was built by this batch.

`qualification-summary.json` records exact sources and the two exported debug
probe paths/hashes. `raw-manifest.json` verifies every fetched raw byte stream;
stdout/stderr/build logs are losslessly compressed. `reproduction/` retains the
exact source-pinned build specs and controllers, including failed stages.

The PG16.13 controls use only a rolled-back owned scratch schema in384MiB,
no-network/tmpfs containers, all removed. The first control hit the temporary
initialization server during its shutdown and ran no SQL; its terminal cleanup
and original incomplete receipt remain recorded. The corrected controls wait
for initialization completion and pass. Their actual output separately proves
child PK/UNIQUE names, nullable/nonunique visible `id`, and COPY physical trigger
order/visibility. Full non-PK UNIQUE and child-schema/FK lifecycle still need work.

The first fcf explicit-run receipt inherited obsolete wording saying admission
was unimplemented. Its independent default8 run proves otherwise. The raw receipt
is preserved; this note and the summary correct its scope. Certificates acquired
a `catalog_digest` on this private branch; interim private artifacts are not a
supported public format release. Cache stamps and snapshot/digest certificates
do not authenticate Native backing birth or accepted replica provenance.
