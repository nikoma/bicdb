# Isolated application previews

`bic preview SOURCE NAME` copies an application's release and current public SQL
data into a fresh, inactive application. The command runs against a standalone
application supervisor. Native state allocations are operator-owned and are not
created by this command.

```sh
bic preview counter counter-preview
bic identity counter-preview
bic list
bic start counter-preview
curl http://127.0.0.1:8788/apps/counter-preview
```

To explicitly activate the copy in the same command:

```sh
bic preview counter another-preview --start
```

Starting a preview runs the copied code against the copied public data. Runtime,
isolation, resource and compute-placement settings remain those of the release.
A microVM preview needs its own operator-provided allocation under its new name;
a remote preview uses the configured worker fleet. Activation failure retains an
inactive application for inspection. Use the ordinary stop, start, deploy,
rollback, backup and logs commands for its subsequent lifecycle.

## What is copied

The copy contains public SQL table schemas, records, referenced large-value
blobs, public sequence state, secondary and vector indexes, and applied application
migration receipts. Indexes are rebuilt after the data arrives; vector indexes
retain their distance metric and tuning. Referenced blobs are
streamed into the target's own storage and use its encryption context. Sequence
high-water marks prevent copied serial keys from being reused.

Migration statements are not executed during creation. Their saved names and
hashes must exactly match the source release's fully applied history. An
incomplete or changed migration history refuses the copy. Starting the preview
uses those receipts, so seed INSERTs and migration defaults do not run again.

The database node ID, signing key and execution capabilities are new. Managed
jobs, delivery attempts, outboxes, dead letters, effect receipts and peer grants
start empty. Selective synchronization is disabled in the preview release;
creating a copy does not connect it to the source's peers. Operator credentials,
roles, grants, provider settings, native replication bindings and historical
event logs are not imported. Public table values are copied as application data.

The source's active requests and job handlers drain before capture. New routed
requests receive `503` while the snapshot is in progress. Database operations
remain quiesced for a consistent copy across tables. The parent resumes serving
after creation, including on a failed copy. The operation keeps its management
and source gates until the copy finishes even if the requesting client disconnects.

## Publication and supported schemas

A preview is assembled under an unpublished staging directory and published by
one directory rename after data, release and catalog writes are synced. An
existing application or directory is refused. Failed copies leave no published
preview, and never replace the source or an existing target. Creation materializes
data; its time and storage use grow with the copied dataset.

This workflow supports unpartitioned public SQL tables owned by the application,
with foreign keys contained in the copied table set. Protected collections,
row policies, other namespaces, partitions and databases with user types,
routines, triggers or views require a separate authorized export and are refused.
Rows are transferred in batches of at most 128 records and 8 MiB; an inline
record larger than 8 MiB is refused. Large-value blobs are transferred separately
in bounded chunks. Historical state, database branches and physical replication
positions are not a shared lineage between the two applications.

`preview.json` records the source release and copy counts for local operator
inspection. Preview creation requires the management token. Application-scoped
runtime and peer tokens do not authorize it.

## Verification

`cargo test -p bicdb-app-host --test preview` checks independent identities,
isolated jobs and effect receipts, migration history, serial counters, indexes
and rejected protected schemas. The deployment suite exercises the CLI and
explicit activation against a running supervisor. Core `logical_copy` tests
verify streamed blobs after removing the source, including transfers between
databases with separate encryption keys and supported storage modes.
