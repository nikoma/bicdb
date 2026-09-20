# MVCC version-chain diagnosis and recovery

BicDB `server_paged` records are reached through a B-tree head locator and an
MVCC version chain. A malformed chain is a localized logical fault. It does not
by itself mean that the page file, neighboring records, or the whole database
is corrupt.

Since `1.0.0-beta`, BicDB reports two dedicated chain errors:

- `VersionChainCycle`, including the real head, repeated locator, and steps;
- `VersionChainLimitExceeded`, including the real head, next locator, and
  one-million-version guard.

These errors are deliberately not classified as checksum, torn-page,
short-read, invalid-file, or page-type corruption.

## Safe operator workflow

Stop every process using the database first. BicDB's directory lock will reject
a second process, and the repair API also refuses to run with active
transactions in its process.

The chain-specific CLI validates the database's durable storage mode and opens
only its existing `paged/store.pages`; it refuses a missing file instead of
creating storage. It does not materialize collections or open SQL, FTS, vector,
or application state, so an affected row cannot block the recovery tool during
normal database startup.

Inspect the affected record:

```bash
bicdb integrity chain-inspect /srv/pubmed \
  --collection pubmed_state \
  --record-id current
```

The output includes an exact `page:slot:generation` head token. Preserve the
inspection output with the incident record.

Reconstruct the control record from an independent durable authority. For a
PubMed importer this should be its successfully published import-file ledger,
not a guess based on the last attempted file. Write the complete reconstructed
`bicdb_core::Record` as JSON and run a dry repair:

```json
{
  "id": "current",
  "vector": null,
  "metadata": {
    "last_published_update": 1336
  },
  "timestamp": null,
  "payload": null
}
```

```bash
bicdb integrity chain-repair /srv/pubmed \
  --collection pubmed_state \
  --record-id current \
  --expected-head 123:4:77 \
  --record-json ./reconstructed-pubmed-state.json
```

The dry run changes nothing. Review the collection, record ID, exact head, fault
type, and replacement file, then repeat with `--apply`.

```bash
bicdb integrity chain-repair /srv/pubmed \
  --collection pubmed_state \
  --record-id current \
  --expected-head 123:4:77 \
  --record-json ./reconstructed-pubmed-state.json \
  --apply
```

The repair:

1. compares the current head with `--expected-head`;
2. re-inspects the chain while holding the page writer lock;
3. refuses a healthy, vacuum-terminated, malformed-value, missing, or changed
   chain;
4. publishes the supplied value as a fresh one-version chain through WAL;
5. checkpoints it immediately; and
6. runs a full B-tree and reachable-version-chain verification.

The faulty chain becomes unreachable. No unrelated key, document row, or
posting is logically rewritten; only the ordinary heap, B-tree, metadata, and
WAL pages needed to publish the replacement are changed.

Run the verifier independently at any time:

```bash
bicdb integrity chain-verify /srv/pubmed
bicdb integrity chain-verify /srv/pubmed --json
```

The standard `bicdb verify`, `bicdb check`, and
`bicdb integrity check` commands also include this page-backed B-tree and MVCC
chain report for `server_paged` databases.

Only resume daily synchronization after this verification and the
application-specific ledger/status comparison both pass.

## Library API

Applications with an authoritative recovery ledger can use:

- `BicDb::inspect_paged_record_chain`;
- `BicDb::repair_paged_record_chain`;
- `BicDb::verify_paged_storage_integrity`.

`repair_paged_record_chain` requires the exact `TupleLocator` returned by a
fresh inspection and a replacement `Record` with the same record ID. It is an
offline recovery API, not an upsert shortcut.
