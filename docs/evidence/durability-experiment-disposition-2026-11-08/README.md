# Disposition of recovered durability experiments

The historical investigations below have different outcomes. A completed
measurement, an unpromoted design and a qualified production repair are not the
same state. This note records what can be closed as superseded and what still
requires work. It does not approve a production experiment or claim that the
storage campaign is finished.

## Completed or superseded investigations

| Work | Evidence and disposition |
|---|---|
| [PR40, WAL-amplification study](https://github.com/nikoma/bicdb/pull/40) | The historical trace identified probe-dirtied heap candidates and fine-grained FSM updates. That concrete low-risk opportunity is delivered by merged [PR54](https://github.com/nikoma/bicdb/pull/54), commit `5ccda588`. Preserve the study as historical evidence and close the stale instrumentation PR as superseded; it is unnecessary to merge its old WAL hooks to retain the result. The old absolute amplification figures predate PR54 and do not measure current main. |
| [PR47, outside-write-lock commit-sync design](https://github.com/nikoma/bicdb/pull/47) | The serial application investigation is complete, without solving its target failures. Distinct experimental source variants C/D are recorded in merged [PR107](https://github.com/nikoma/bicdb/pull/107); they show no material benefit for those serial streams. Close the old design PR as superseded for that application problem. This does not establish that early lock release cannot benefit other concurrent workloads, nor approve its committing/publication architecture. |
| [PR82, initial structural-cut design](https://github.com/nikoma/bicdb/pull/82) | The approved direction was explored on disposable stores. Its proposed individual xid-zero images are superseded by [draft110](https://github.com/nikoma/bicdb/pull/110): one fresh internal transaction makes the complete page closure atomic, with a durable higher xid marker before streamed version-bearing images. Close the obsolete proposal, preserving its discussion and the exact replacement link. Neither this disposition nor draft110 authorizes promotion. |

## Necessary unfinished work

| Work | What remains |
|---|---|
| [PR43, unlocked WAL sync](https://github.com/nikoma/bicdb/pull/43) | Keep open and off. Historical concurrent measurements are separate from the serial application result. Current-main integration, a quiet source-paired concurrent benefit measurement and actual hard-VM-reset qualification remain before a separate promotion decision. The old report's missing CLI backup build and source revision limits must not be represented as passing current checks. |
| [PR48, page-delta redo](https://github.com/nikoma/bicdb/pull/48) | Keep open, disposable-only and off. This introduces WAL kind 5 plus a sticky experimental store format; merging even disabled code was explicitly a separate decision. Stabilize the no-steal safety baseline first, then prove the combined eligibility/epoch rules, execute old-binary refusal, fuzz delta payloads, run actual hard VM resets and quiet paired performance. Historical byte reductions and replay tests do not qualify current recovery, backup/archive behavior or application acceptance. No production-format transition is delivered. |
| [PR81, no-steal interim](https://github.com/nikoma/bicdb/pull/81) | Insufficient as a final repair: its default redo-backed acknowledged-large W1 case still steals. Preserve that reproducer and the refusal/no-partial-effects evidence; do not merge it as complete data safety. Its mechanisms are incorporated into gated draft110. |
| [Draft110, refined structural cuts](https://github.com/nikoma/bicdb/pull/110) | Remains draft/off. Original-source passes are source-scoped evidence, not final qualification. The 1a9 VM run retained all 2,089 acknowledged rows/indexes/atomic sets but reused SQL native transaction IDs, blocking promotion. The next source batch includes the logical-ID floor repair PR114 and upgraded heap guards PR54; current-source broad, independent, explicit SERVER_PAGED control, paired optimized performance, hard-VM-reset/backup-restore and rollout gates remain. |

For PR43/48, leaving the gate off is an architectural constraint, not a claim that
all work is finished. Format and concurrency experiments cannot be promoted by
relabeling a historical report or removing a failed assertion. No old source's
success is reported as a check of a changed head.

## Preserved PR40 provenance

[The original 494-line report](originals/pr40-page-wal-write-amplification.md) is
copied byte-for-byte from PR40 head
`37ffe7aa093d89ac0c471539354a8324fb2c5bd2`, path
`docs/page-wal-write-amplification.md`. `provenance.json` records its SHA-256.
The report's relative source references and reproduction commands refer to that
historical tree, not to current main. Archived source permalinks:

- [Original report](https://github.com/nikoma/bicdb/blob/37ffe7aa093d89ac0c471539354a8324fb2c5bd2/docs/page-wal-write-amplification.md)
- [Default-off observation trace](https://github.com/nikoma/bicdb/blob/37ffe7aa093d89ac0c471539354a8324fb2c5bd2/crates/bicdb-page/src/wal_amplification.rs)
- [Probe](https://github.com/nikoma/bicdb/blob/37ffe7aa093d89ac0c471539354a8324fb2c5bd2/crates/bicdb-core/examples/wal_amplification_probe.rs)
- [Gate/WAL-byte equivalence test](https://github.com/nikoma/bicdb/blob/37ffe7aa093d89ac0c471539354a8324fb2c5bd2/crates/bicdb-page/tests/wal_amplification_off.rs)

The observation code and probe are preserved by those immutable references;
this evidence change adds no instrumentation to the production path.

## Independent application evidence and delivered repairs

Merged PR107, commit `6f75a352`, independently reconciles all 70 diagnostic runs:
[report](../background-job-commit-experiment-2026-11-08/README.md),
[raw allowlisted run records](../background-job-commit-experiment-2026-11-08/runs.json).
Both target background-job tests failed in every variant. The 6,615 other passing
invocations are repeated invocations, not distinct tests or storage qualification.
B/C/D show no material serial benefit. E (transaction-log-only durability)
reduced COMMIT time but still failed both targets; that supplies no reason to
remove a durability barrier. No target failure or upstream assertion was changed.

The separate narrow production repairs are delivered: durable replication
[PR105](https://github.com/nikoma/bicdb/pull/105), commit `8b1e1d47`, and stale
allocator/BTree crash-state repair [PR108](https://github.com/nikoma/bicdb/pull/108),
commit `45f9995c`. Every actual-head check passed for each. PR105 merged before
its final checks completed, a gate miss retained in the recovery log; those
checks subsequently passed. PR108 merged only after all checks passed, independent
review, full page tests and saved-state reproduction. Neither completes the
unfinished structural or delta qualification.
