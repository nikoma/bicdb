# Bounded server-paged MVCC integrity verification

Status: shipped in BicDB 1.0.71-beta.

`BicDb::verify_paged_version_chains_step` verifies reachable MVCC version
chains without turning database size or one pathological row into an unbounded
maintenance operation. It is the production-sized counterpart to the older
exhaustive `verify_paged_storage_integrity` diagnostic.

## Resource envelope

Every call receives immutable `VersionChainVerifyLimits` covering:

- logical keys examined;
- aggregate version headers examined;
- versions examined in one chain;
- fixed MVCC-header bytes read;
- cooperative duration;
- retained fault-sample count and serialized bytes; and
- the maximum serialized cursor-key size.

All fields have finite non-weakenable ceilings. Invalid limits and oversized
input or output cursor keys fail before publishing progress. A step reserves
enough remaining version and byte budget for the complete per-chain allowance
before starting the next key, so it never strands half of a key behind a cursor
that claims the key was completed.

The engine reads only the fixed 30-byte MVCC prefix from each stored version.
An overflow-backed multi-megabyte row therefore costs one bounded prefix read,
not a full payload materialization. Cycle detection is allocation-free for
ordinary chains and bounded by the configured per-chain version ceiling for a
pathological chain.

## Cursor and restart behavior

`VersionChainVerifyCursor.next_key` is inclusive: it is the first key that has
not been examined. `None` starts a sweep. A completed report also returns the
default cursor, so callers must stop when `complete` is true.

The cursor, limits, stop reason, and report are strict JSON contracts that
reject unknown fields. A host may atomically checkpoint the
returned cursor and resume it after process or database restart. Replaying the
same cursor rechecks a key; it cannot skip an unacknowledged key.

The structure read lock is held for one key at a time. Before walking a chain,
the key is resolved again under that lock so a range cursor cannot supply an
obsolete head after a concurrent update. Keys inserted behind an already
published cursor are intentionally verified by the next sweep; an online sweep
is not falsely described as a transactionally frozen database snapshot.

## Result meaning

A step reports examined keys, versions and header bytes; classified faults;
bounded samples and dropped-sample count; elapsed time; exact next cursor; stop
reason; and completion.

`version_chains.valid` means that the keys covered by that step had no detected
invalid head, malformed version, cycle, or per-chain limit violation. A caller
may claim a store-wide chain result only after every step in one sweep completes
and all step results are valid.

## Remaining boundary

This API bounds reachable MVCC-chain verification. BicDB 1.0.72-beta adds the
separately bounded structural tree verifier documented in
[`paged-btree-integrity-maintenance.md`](paged-btree-integrity-maintenance.md).
BicDB 1.0.73-beta composes both cursors under the checksummed, atomic supervisor
documented in
[`paged-integrity-maintenance.md`](paged-integrity-maintenance.md). It owns
phase, immutable limits, store and operation identity, accepted totals,
resource admission, retry/pause state, and terminal evidence.
