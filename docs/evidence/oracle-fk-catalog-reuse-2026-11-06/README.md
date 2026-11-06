# Foreign-key preflight catalog reuse

The migration importer reuses its complete, validated initial catalog for
pending-key presence checks and matching unjournaled installations. Every new
DDL still receives an individual post-install catalog readback before it can be
checkpointed. Quiesced writers/schema changes, exact source/target journal
identity, enforcement checks and before/after row fingerprints remain required.
See the [operator contract](../../oracle-foreign-key-migration.md).

All **15 tests passed: five pure and ten native, with zero skips**. Tests use the
existing native executable built from `5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`;
[validation.json](validation.json) pins its digest and the Python source commit.
This change does not build or upgrade the SQL engine or application deployment.

| Native fixture | Individual constraint lookups after preflight |
| --- | ---: |
| Three fresh source keys | 3 post-install lookups |
| All three keys already checkpointed | 0 |
| Resume with one missing and one matching unjournaled key | 1 post-install lookup |

The complete initial constraint catalog is still read in each case. The traced
three-fresh-key assertion failed against the prior implementation with **6
lookups instead of 3**, then passed after reuse. This is a query-count measurement,
not an elapsed-time or production throughput benchmark.

An additional failure control supplies a wrong post-install validation flag:
the importer refuses to certify or checkpoint the installed key. A fresh resume
then verifies the actual committed definition through the full catalog before
adopting it and installing the rest. Existing tests retain restart/recovery,
source timing/actions/validation, disabled-enforcement refusal, changed completed
key refusal, exact journal identity, row/projection protection, quoted names,
composite/unique parents, missing explicit child primary keys and orphan refusal.

The active pristine application import keeps its frozen earlier tool set. This
qualification does not claim its remaining FK/schema/data checks, application
tests, cutover or production acceptance are complete.
