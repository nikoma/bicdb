# JDBC BLOB binary values and read snapshots

The explicit Oracle preview adapter now maps positional BLOB stream bindings to
the stock PostgreSQL driver's BYTEA stream setters. Blob objects and Blob-valued
typed setters bind binary values; null and empty values remain distinct. Named
callable parameters and unrelated setters retain their existing driver paths.
This fixes the storage choice that previously reached the unsupported PostgreSQL
OID large-object API. It does not implement PostgreSQL large objects.

BYTEA `getBlob` and typed Blob getters return materialized read-only snapshots.
Mutation methods reject with `0A000`, preserving the distinction between a
binary value and a writable Oracle locator. Buffered Blob object binds have a
64 MiB limit. Persistent writable locators, temporary-LOB creation, full metadata,
CLOB/BFILE and streaming result reads remain open. The
[adapter contract](../../../examples/oracle/jdbc/README.md) describes the boundary.

Validation used adapter source `3db8c5d885c34bd3c7c3d5ddd33e77fcaeb35f58` and the
already qualified backend `a97545fc25234c89a4361e0dbfe8c98a02cb3e94`:

- 19 unit tests pass, including all prior lexical/parser cases.
- Four provider preparation/compilation tests pass.
- 71 native checks pass in each of extended, simple and extended-cache-everything
  query modes; the PostgreSQL-profile rejection control also passes: 214 total.
- Both staged provider variants compile; the staged provider passes its exact
  numeric and anonymous-block transport checks.
- All disposable probe servers exit zero and their temporary databases are
  cleaned. The OSGi manifest declares the standard serial-rowset package import.

[summary.json](summary.json) pins artifacts, counts and resource observations.
Server source did not change, so the prior backend regression results were not
rerun or relabelled. Application OSGi runtime acceptance with this adapter remains
a separate gate. The earlier failed attachment retest used the previous adapter;
these checks do not change its outcome or qualify full application behavior.
