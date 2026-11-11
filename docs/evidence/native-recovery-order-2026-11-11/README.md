# Native recovery order and bounded-history refusal

These are **local private-prototype component experiments**, not current-main,
CI, cloud, physical-failure, SQL, activation or production qualification. The
package version is `1.0.471-beta`; the exact revisions below distinguish the
runtime and test changes. All successful data acknowledgments came from real
fsynced RF3 services. Data delivery failures were injected in process. Metadata
used real durable stores and challenged quorum acquisition through a simulated
member transport; the two ordering witnesses did not exercise TLS.

| Cohort | Exact source | Actual outcome |
| --- | --- | --- |
| Initial retained-prefix attempt | `9eace333c198e28e5416fb2943760e6286d5be59` | **FAIL**: an intended noncontiguous historical-certification refusal set store-wide persistence uncertainty before any catalog mutation. The subsequent prefix certification correctly refused uncertain durability. |
| Baseline with protocol re-fsync | `021ba5c42d9acd025760cb9496ff9b03f98d5f18` | **1 PASS**: actual same-session `fence_epoch` obtained fresh metadata and re-fsynced the catalog, then authentic command 1 was exported, recertified and applied before uncertain command 2. |
| Validation preflight repair | `7d2f84a50d4aeaeee211ec4d54120f5c539a8ba4` | **22 distinct PASS**: intended gap refusal preserves catalog bytes and live durable observations; retained-prefix recovery works without re-fsync. Includes five persistence/state, nine loopback TLS and seven quorum controls. |
| Large independent baseline | `e68fa21fc4bfc52b53294e73dde57e42eb36876f` | **FAIL at first fence response**: source native/resolved 1025 plus raw CQ 1026 produces 1025 irreversible obligations, exceeding the fixed 1024 observation limit. Later compacted-gap checks were **not reached**. |

The production fix is separately retained as `8061b7cc50c28875cdd5a74fbaa8865fd72baad6`.
It moves pure validation ahead of the first mutation/uncertainty flag. Every
actual persistence ambiguity still fails closed. Its passing suite does not
rewrite either baseline failure or qualify the later large case.

## What the successful small experiment establishes

While n1 receives no data messages, n0 acknowledges command 1 with actual old
quorum n0+n2. A real rejection after admission of command 2 leaves an uncertain
call, actual raw CQ 2 and native pending 2; that call never enters the external
ACK ledger. Data delivery to n2 then stops and n1 returns at raw/native 0.
Metadata participant n2 remains reachable: this is a data-RPC partition, not a
VM stop.

Sending command 2 directly to n1 correctly refuses its missing prefix. There
is nevertheless an existing safe path for this **retained** gap:

1. Export actual resolved command 1 without requiring source NativeReady.
2. Independently recertify and apply command 1 through the recovery protocol.
3. Recertify and apply uncertain command 2 using the actual current quorum.

Both available nodes finish at actual native/resolved 2 with equal logical
history, and the acknowledged value reconciles. Neither `Applied` labels nor
quorum votes were supplied by the test. Generic repair stays refused by the
owned recovery freeze. This disproves an unconditional small-gap circularity
claim; it does not prove automatic orchestration, activation or new writes.

## The actual large boundary

The retained raw root contains 1025 separately fsynced external ACK entries,
actual source native/resolved 1025, pending 1026, and command 1026 in CQ state.
The raw range log compacts through 1. The first requested epoch fence is n0.
It persists an ACTIVE refusal but cannot return its typed response:

```
cluster error: range recovery: malformed fenced image-history obligations
```

The independently checksum-verified post-exit n0 catalog retains **1024 Applied
entries plus one CQ entry**. The unavailable n2's raw catalog has the same
counts following the existing coordinator catch-up path; its final native
lineage was not measured here. n1's in-process raw progress is 0 and its actual
initial native genesis is 0; no n1 fence request was reached. Its missing raw
catalog file is reported as absent, not interpreted as an observed watermark.

The first refusal is the bound in
`distribution_range_recovery/image_history.rs::RecoveryImageHistoryObligationsV1::require_shape`,
called by
`distribution_range_consensus/native_ready_catalog.rs::observe_recovery_image_obligations`.
The latter exports all actual retained CQ/Applied entries after the durable
fence. Nothing was truncated and no protocol limit was raised to force the
experiment past this boundary.

The intended subsequent investigation remains open: repair export after 0
would encounter actual compaction through 1; a complete-image source currently
requires a fully resolved NativeReady cut covering the observed tail. This
experiment did **not** reach those operations and does not prove a general
recovery cycle. A bounded complete-obligation export repair and an independently
scoped prefix-repair protocol, if needed, require their own implementation and
actual tests. No irretrievable loss, Azure cause or current-main causal claim is
made.

## Reproduction and custody

Exact test sources, production preflight patch, command/environment receipts,
stdout/stderr, active resource-monitor samples and the separate 22-test summary
are retained here. `MANIFEST.json` hashes every evidence file. Run:

```
python3 scripts/verify-native-recovery-order-evidence.py
python3 -m unittest discover -s scripts/tests -p test_native_recovery_order_evidence.py
```

On the exact source cohort, the retained-prefix filter is
`actual_raw_behind_zero_recovers_retained_prefix_before_uncertain_tail_without_source_ready`;
the repaired version is
`actual_gap_refusal_preserves_durable_observations_and_retained_prefix_recovers`.
The large filter is
`actual_compacted_prefix_1025_and_uncertain_1026_refuse_raw_zero_repair`.
Use the recorded release command, features and profile overrides. Test sources
must be paired with their actual runtime revision; source snapshots alone do
not replace the repository dependencies.

Every long build used jobs 1, incremental compilation off, active RAM/pressure
and disk monitoring, and forced compilation of the intended worktree. The
large test ran 356.16 seconds (424.20 seconds including compilation), with no
guard interruption and at least 21,494,304,768 available bytes. This duration is
a functional experiment observation, not a throughput result.

The complete stopped large fixture, actual metadata stores, birth/config files,
ACK ledger and raw logs are in restricted durable custody under
`private/native-recovery-order-e68fa21f`: **91 files / 152,842,336 bytes**,
canonical manifest SHA-256
`7426449ea05b610832b99ab6673fa552b0be5f409e0c6cf9da10b285b5062cf2`.
Source/destination hashes match, files/directories were fsynced and restricted
to 0600/0700. Test-generated database identity keys remain private. No global
credentials, compiler artifacts or binaries are included. The original and
custody roots have not been reopened; future diagnostics must use a new copy.
The two earlier small-run temporary DB roots were not retained: their raw logs
and source receipts are preserved, and no cold-DB custody claim is made for them.
