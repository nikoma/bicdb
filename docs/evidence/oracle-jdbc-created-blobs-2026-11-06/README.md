# Client-created JDBC BLOB values — 2026-11-06

`Connection.createBlob()` now constructs an empty mutable client value in the
explicit Oracle adapter. One-based writes and binary streams can grow it to
64 MiB. Reads, partial streams, binary search, truncation and idempotent `free()`
are implemented. Invalid ranges and capacity overflow fail before mutation;
closed connections reject creation. Binding stores a copy through the existing
binary-value path. This follows the [JDBC factory contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/Connection.html#createBlob()).

The [machine-readable record](summary.json) identifies exact adapter and
unchanged backend revisions and artifact hashes. All 25 adapter unit tests,
four provider preparation checks and 238 native JDBC checks passed. Each of
extended, simple and extendedCacheEverything passed 79 checks, including
create/write/bind/free/read round trips with NUL and high-bit bytes. One
PostgreSQL-profile rejection check passed. All four disposable servers exited
zero and cleaned up their probe tables. The server was reused from its prior
backend qualification; Rust suites were not rerun for this Java-only change.
An active resource guard observed at least 21 GiB available memory with no
full memory pressure. Two earlier unit runs exposed incomplete mock connection
setup; both failures were preserved privately and fixed before this result.

This is not a writable persistent locator. Mutating a newly created value does
not update a row, and BLOBs retrieved from result sets remain explicitly
read-only. Transaction-bound locator mutation, complete Oracle BLOB metadata,
streaming result reads, full application acceptance, release publication and
production qualification remain open. The live application retest continued
with its earlier, separately recorded adapter artifact.
