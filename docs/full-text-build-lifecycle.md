# Full-text build lifecycle and recovery

BicDB owns the authoritative lifecycle of a large full-text build. Supervisors
must derive decisions from BicDB's checkpoint and published generation rather
than maintaining a second completion marker.

## Invariants

1. The published generation remains readable while a replacement tokenizes,
   merges, retries, or is abandoned.
2. A build checkpoint advances only after its referenced run files are durable.
   Temporary or corrupt runs are never adopted.
3. Generation publication is atomic. A crash before the swap serves the old
   generation; a crash after the swap serves the new generation.
4. Reconciliation is idempotent. Repeating it resumes durable work, completes
   interrupted publication, or reports `already_published`.
5. Direct/sealed ingestion is never finalized by inference. BicDB reports
   `awaiting_input` until the producer explicitly calls
   `finish_full_text_tokenization`.
6. A checkpoint without its catalog definition is visible as `blocked` with
   reason `catalog_definition_missing`; it is not silently omitted.

## Lifecycle API

`BicDb::full_text_build_lifecycle(index)` returns a versioned, serializable
report containing:

- lifecycle state and stable `reason_code`;
- the next safe action;
- whether a valid generation is currently serving;
- staged and published physical generation identities;
- durable document/run progress and checkpoint modification time;
- progressive document coverage; and
- the published generation's document count and manifest digest.

`BicDb::full_text_build_lifecycles()` discovers both catalogued indexes and
checkpoint-only workspaces. It is the fleet inventory surface.

`BicDb::reconcile_full_text_build(index, budget_documents)` performs the next
safe operation:

| Durable state | Reconcile behavior |
| --- | --- |
| no workspace, published generation | `already_published` no-op |
| row-backed tokenization | process at most the document budget |
| direct/sealed tokenization | `awaiting_input` no-op |
| merge checkpoint | resume finalization |
| publishing checkpoint | finish or confirm the atomic publication |
| missing catalog definition | `blocked` no-op |

The document budget bounds row scanning and tokenization. Final merge work is
restartable and bounded-memory but may take longer than one reconciliation
call; an interrupted call is safe to repeat from its durable merge checkpoint.

Long-running or multi-tenant supervisors should call
`BicDb::reconcile_full_text_build_governed(...)`. It applies the same
idempotent state machine after obtaining an index-build permit from BicDB's
resource governor.

## Operator commands

```bash
# One index
bicdb index fts-status /srv/bicdb web_fts --json

# Fleet inventory
bicdb index fts-status /srv/bicdb --all --json

# One bounded recovery step; safe to repeat after timeout or restart
bicdb index fts-reconcile /srv/bicdb web_fts \
  --budget-documents 1000000 \
  --json
```

SQL clients can read the same authoritative report:

```sql
SELECT bicdb_fts_build_status('web_fts');
```

Mutation remains an explicit core/CLI operation so an ordinary read query
cannot accidentally start a corpus-scale build.

## Supervisor loop

A service manager needs no phase-specific shell logic:

1. Read `fts-status --all --json`.
2. For each report whose recommended action is `reconcile`, invoke one bounded
   reconciliation step under the configured resource governor.
3. For `append_documents`, keep or restart the owning producer.
4. Alert on `blocked`; include the stable reason code and complete JSON report.
5. Continue serving the generation identified by `published` throughout.

The supervisor may crash between any two steps. The next run reads the same
durable facts and reaches the same decision.

## Crash acceptance matrix

Release tests cover interruption during tokenization, primary-key merge,
legacy impact merge, publication entry, and the narrow window after atomic
publication but before workspace cleanup. Recovered results must match a clean
build, retain the same already-published physical generation where applicable,
and repeated reconciliation must perform no additional build.
