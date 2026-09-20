# Jepsen tests for BicDB

Checks BicDB's transactional guarantees under fault injection, using
[Elle](https://github.com/jepsen-io/elle)'s list-append workload over the
PostgreSQL wire protocol.

## What is checked

BicDB documents `READ COMMITTED` and snapshot transaction behaviour and
explicitly does **not** claim serializability, so that is what these tests
check. Pointing Elle at serializability would produce a wall of true but
unclaimed violations; the checker targets the guarantee the database actually
offers.

## Running

Every node is an ordinary local process, so no SSH, containers, or cloud
infrastructure are needed. The test runs with `:ssh {:dummy? true}` and
`jepsen.bicdb.db` shells out directly.

```sh
cargo build -p bicdb-cli                    # BICDB_BIN defaults to target/debug/bicdb
./run.sh --node n1 --time-limit 60 --concurrency 5
./run.sh --node n1 --time-limit 60 --concurrency 5 --crash   # with SIGKILL faults
```

Requirements: JDK 21 (the Jepsen dependency tree needs
`java.util.SequencedCollection`), Leiningen, and `gnuplot` for the performance
graphs.

Results land in `store/`, including Elle's cycle explanations under
`store/<test>/<run>/elle/`.

## What it has found

- **A primary key was not enforced within a transaction.** Uniqueness
  validation consulted committed state only, so a row inserted earlier in the
  same transaction was invisible; a second `INSERT` of the same key passed
  validation and silently replaced the first, losing its data with no error.
  Elle surfaced it as a G0 write cycle. Fixed, with regression tests in
  `crates/bicdb-sql/tests/transaction_uniqueness.rs`.

## Notes for extending this

- Writes are only accepted by the consensus leader. A follower refusing a write
  is a legitimate outcome and is recorded as `:fail` (it definitely did not
  happen), not `:info`.
- Classify every error you can as a definite failure. An `:info` operation is
  one the checker can conclude almost nothing from; an early run had 590 of
  1325 operations indeterminate purely because the nemesis was being handed
  workload transactions (fixed with `gen/clients`).
