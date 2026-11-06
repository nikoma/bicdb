# Pristine post-FK source-value reconciliation

The original-source iDempiere target passed the public importer's `verify`
phase after installation of all 3,826 source-declared foreign keys. All **915
tables and 185,471 rows** match the retained Oracle export within its supported
value encoding. The completed verification subprocess exited zero.
[summary.json](summary.json) records snapshot, report, tool and executable
provenance without publishing rows or connection details.

Verification authenticates each source row file against its manifest digest,
projection width and row count. It reads every exported target column and
compares sorted canonical row hashes, preserving duplicate multiplicity without
depending on heap order. Numbers use decimal values without float conversion;
strings retain whitespace, bytes retain their contents and timestamps use the
export codec. Table projection, character-unit declarations and supported
nullable-unique policies are checked as part of the phase. Before publication,
all 915 saved target digests were independently compared with newly computed
digests of the authenticated source files.

The development executable was built from
`5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`; this completed phase used tooling
frozen from `2312aceb00b4f26451fc25e521233ff25026eb0f`. A later memory-guard
interruption occurred during orphan inventory, after value verification had
completed. That interruption does not qualify the unfinished inventory. The
resumed database must repeat its value checks before application qualification.

Reproduce with the [application import guide](../../../examples/oracle/idempiere/README.md)
using `--phase verify` against a quiesced disposable target and the same retained
snapshot. On `--resume`, completed verification entries are re-read and compared
with their saved digests; a journal prefix alone is not accepted as value proof.

This establishes equality with the **saved export**, not a continuously current
Oracle database or a change-capture position. It does not establish support for
types excluded by export preflight, original time-zone region identities, full
Oracle metadata equivalence, routine/view execution and privileges, orphan-count
reconciliation, all original application workflows, recovery or production
qualification. The accepted running application was not upgraded. Earlier
evidence retains its original scope; this checkpoint adds source-value equality
to the separate [completed FK migration](../idempiere-pristine-foreign-keys-2026-11-06/README.md).
