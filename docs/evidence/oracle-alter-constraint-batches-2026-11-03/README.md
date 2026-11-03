# Batch preparation of ALTER constraint expressions

ALTER COLUMN TYPE validates each resulting record against the updated schema.
Previously, that loop prepared the same CHECK expression list separately for
every record, including cloning expressions already present in the parser cache.
It now prepares the list once for the complete row batch. Every row still
receives its column, NOT NULL and CHECK validation. Empty batches retain their
previous behavior, and the first row's column errors still precede malformed
stored CHECK expressions. Unique, exclusion and foreign-key checks are unchanged.

This is a general ALTER optimization. It applies equally to native PostgreSQL
type changes and the Oracle-preview declaration reconciliation path. It does not
relax constraints or introduce application-specific execution rules. The Rust
regression uses two CHECK constraints and 500 rows, observes exactly one
preparation, rejects a rewrite that makes the final row invalid, and verifies
that the failed operation restores the original type and every stored record.

The independent PostgreSQL fixture checks the same failure, descriptor and
value preservation, a subsequent successful rewrite, and retained CHECK and
NOT NULL enforcement through the PostgreSQL driver. Provision two matching
schemas on the dedicated reference and target databases, then run:

```sh
python examples/oracle/reference/postgres_alter_checks.py \
  --schemas alter_a alter_b --output /private/alter-checks.json
```

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately. The target must confirm the
native PostgreSQL profile. The fixture creates a randomly named table in the
first supplied schema and removes that table on both databases. All nine cases
match PostgreSQL 18.4. The preparation-count assertion is a regression guarantee;
no application wall-clock speedup is claimed at this checkpoint.

Local validation passes **1,326 Rust tests**, with three existing ignores. The
full independent PostgreSQL corpus, including the new fixture and retained
qualified-view checks, passes **266/266**. The retained Oracle corpus preserves
**1,475** passing positive/error contracts and one separately counted negative
national-type alias boundary. Nine source-based character reconciliation checks,
six reader checks and three importer checks pass on the same candidate. Original
minimal-dependency counter, record and sequence fixtures retain 14/14, 8/8 and
9/9 matches.

The isolated application definition inventory remains **60/62**. Only 1/13
NULL-input procedure diagnostics completes; this is not execution equivalence.
Oracle dictionary queries remain **0/11**, with separate character assignment
checks at 2/2. Five existing NVL/conversion mismatches remain explicit. Dictionary
visibility/default provenance, hierarchy and collections, constraints and full
application workflows still require implementation and qualification.

A fresh full application-copy character reconciliation trial uses this candidate
and persistent disk, starting from the untouched snapshot. It is pending at
this checkpoint. The earlier RAM-backed trial is separate evidence and does not
qualify persistent storage. Neither trial updates the existing running database.

Builds use one compiler, disabled incremental compilation and an active minimum
8 GiB available-RAM guard. Completed test-executable cleanup reclaimed
3,229,565,920 bytes. Temporary RAM output staging has been restored to disk with
content verification; reusable inputs and required readers were preserved. The
Oracle build cache is 6.6 GiB. A development example binary is built; no release
or end-to-end application deployment is claimed.
