# Durable server-paged integrity maintenance

Status: shipped in BicDB 1.0.73-beta.

`BicDb::start_paged_integrity_maintenance` starts one checksummed,
resource-governed integrity sweep. The supervisor owns both bounded page-engine
cursors and advances at most one step per call:

```text
structural B+ tree verification
        |
        | valid completion checkpoint
        v
reachable MVCC-chain verification
        |
        v
terminal valid or invalid evidence
```

The MVCC phase cannot run until a complete, valid structural report has been
atomically published. A structural fault terminates immediately and retains
classified evidence. MVCC faults are accumulated through the rest of the key
sweep so the final report describes all keys reached by that sweep rather than
only the first damaged chain.

## Durable state

`maintenance/paged/integrity.json` contains:

- immutable store and operation IDs;
- the complete immutable structural and MVCC resource envelopes;
- current phase and both exact cursors;
- monotonic accepted-step totals;
- the last bounded report for each phase;
- start, update, observation, and next-attempt times;
- consecutive failure and bounded error state;
- operator pause state;
- completion and aggregate validity;
- a monotonic state sequence; and
- a SHA-256 over every field above.

The integrity and vacuum supervisors share
`maintenance/paged/store-identity.json`. Missing identity cannot be recreated
over either existing schedule. State reads reject symlinks, non-regular files,
empty or oversized files, changing file length, unknown JSON fields, a foreign
store ID, invalid phase combinations, and checksum mismatch.

Every transition is written through the existing atomic file publication path.
If a process stops after a read-only verification step but before checkpoint
publication, the prior cursor remains authoritative and that exact step is
replayed. Totals include only reports whose cursor was published atomically, so
lost checkpoints neither skip work nor double-count accepted evidence.

## Resource governance and scheduling

`PagedIntegrityScheduleLimits` freezes:

- `BTreeVerifyLimits`;
- `VersionChainVerifyLimits`;
- successful-step spacing;
- resource-saturation retry delay;
- exponential failure backoff and maximum consecutive failures;
- maximum serialized state bytes; and
- an explicit memory, I/O, CPU-slot, and I/O-token demand.

The supervisor validates that its demand covers the declared verification
envelopes and bounded state. Each page-engine call must acquire the
anti-entropy background lane before touching a page. Saturation checkpoints a
future due time without invoking either verifier. The permit is released before
the resulting schedule is serialized.

Repeated I/O or engine failures use bounded exponential backoff and eventually
pause. Operators may pause or resume only the exact active operation ID;
starting a replacement operation fences stale workers. A report that cannot
advance its cursor under the immutable envelope pauses instead of hot-looping.

## Public operations

`BicDb` exposes:

- `start_paged_integrity_maintenance`;
- `paged_integrity_maintenance_status`;
- `tick_paged_integrity_maintenance`;
- `pause_paged_integrity_maintenance`; and
- `resume_paged_integrity_maintenance`.

Embedded-memory mode rejects all of these operations without creating paged
maintenance state.

## Evidence meaning

`PagedIntegritySchedule.valid` is derived from accepted totals. It is true only
when no structural fault, invalid head, malformed version, MVCC cycle, or chain
safety-limit exhaustion was observed. Callers cannot set it independently.

This is an online sweep, not a frozen snapshot. Each structural step is stable
under its finite read lock and each MVCC key is re-resolved under the lock, but
writes may occur between checkpoints. Keys inserted behind a published cursor
belong to the next sweep. Continuous certification therefore schedules
repeated completed sweeps; it must not describe one online sweep as a
point-in-time backup certificate.
