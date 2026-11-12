# Registry, routing, expiry and owned-metadata isolation receipts

These are source-separated focused CI results for the native prototype. They
are not a full-suite pass or a single coherent-head result. All recorded source
packages are `1.0.489-beta`; this receipt makes no current-main or release claim.

| Scope | Exact source | Run / Core job | Actual result |
| --- | --- | --- | --- |
| Registry continuity/publication | `29bfd6c9af7652a8e40a97e46b23d4629777ece9` | [43308056358](https://github.com/nikoma/bicdb/actions/runs/43308056358) / 131496514002 | **5 passed**, 0 failed, 945 filtered; 7.61 s |
| Routing/discovery | same `29bfd6c9…` | [43308063984](https://github.com/nikoma/bicdb/actions/runs/43308063984) / 131496564730 | **3 passed**, 0 failed, 947 filtered; 5.71 s |
| Actual late Ready deliveries | `ded3c412cf25c12f3c6fe524d22aac02310a1c0b` | [43303076063](https://github.com/nikoma/bicdb/actions/runs/43303076063) / 131479146693 | **1 passed**, 0 failed, 947 filtered; 3.20 s |
| Corrected owned-member/catch-up waits | `6e00ba519715ee0ca392357394f2b0a9749a1358` | [43310478213](https://github.com/nikoma/bicdb/actions/runs/43310478213) / 131505005808 | **4 passed**, 0 failed, 947 filtered; 2.30 s |

Workspace/all-target checks and isolated consumer compilation without Core
defaults passed for registry29b, routing29b and expiryDED3, plus the older DED3
broad run's separate compiler job. **Corrected6e compiler job131504948931 remains
pending at this checkpoint**; its Core runtime pass is not an all-target or
consumer pass. Its terminal result will be appended separately. These are
compilation results, not extra runtime test passes. Counts from different heads
must not be reported as a full suite at the newest head.

The five registry tests exercised actual publication-error/retry/cold no-grant,
original cancellation/retained old Store-slot refusal, legacy `None` continuity
refusal, continuity of a previously added member, and writer replacement. The
replacement negative now captures the genuine preparation before replacement;
it checks the earlier actual ingress refusal and direct kernel rejection,
BOTH original and replacement generations' native current/history inventories,
full ACK row and R/H/B, and replacement-hook survival after old cleanup.

The three routing tests exercised actual wrong service/stopped owner, replaced
Store with retained old object, and healthy metadata discovery while all three
actual DATA service mutexes remained held. The holder's new bounded rendezvous
does not change MQ/request TTLs or release a subset early to obtain success.

The expiry test held two genuine post-mTLS Ready replies from n0 at R1 until
their own child deadlines expired. Both returned expired, the original parent
remained live, and no Ready quorum or native/catalogue change was admitted.
This is one actual TLS control, not general Ready/CurrentJoint qualification.

The four corrected owned controls cover HOT strict owned-member and HOT
Registered catch-up validation: each tests release of the actual final MStore
mutex and original cancellation while it remains held. They retain real fresh
OWN MQ, actual promoted factory/StoreUUID, the same sealed proof and actual
post-factory revalidation; native commit sequence, rows and namespace IDs remain
unchanged. The cold paths use the same reviewed helper but **no cold contention
fault-control execution is claimed**. Catch-up status is necessary diagnostic
DATA; it never replaces strict local voting/control readiness.

## Preserved cancellations and the exact fixture fix

The older broad host run43303070364 / Core131479132858 at `ded3c412…` selected60
names. Raw output contains **57 individual `ok` records and one ignored internal
child helper**, but no completed suite summary. Exactly these two selected names
lack completion:

- `native_owned_metadata_validation_original_cancel_exits_while_actual_final_store_held`
- `native_owned_metadata_validation_waits_for_actual_final_store_then_keeps_same_proof`

The owned isolation run43308048131 / Core131496490839 at `29bfd6c9…` selected4
names and recorded **zero completed controls**, also without a suite summary.
The two additional names were the analogous Registered catch-up controls.
Both runs were deliberately canceled by their owner after a **definite test-only
mutex reentrancy** was found: `metadata.lock().propose(..., &config(&f), ...)`
holds the receiver guard while `config(&f)` locks the SAME metadata mutex.
This blocks before the promotion body/control cut. It is not an observation
being relabeled as a hang, nor evidence of a production MStore protocol defect.

Correction `6e00ba519715ee0ca392357394f2b0a9749a1358` captures the actual fixture
configuration BEFORE the receiver guard. The actual promotion, authorities,
original budgets and assertions remain unchanged. The corrected four-test
execution above is distinct from the canceled attempts.

The broad run's workspace/consumer job131479079371 had already passed. The
canceled four-test run's workspace job131496447289 was canceled during all-target
compilation; its isolated consumer step was **skipped**, not passed. Complete
selected names, partial records, raw hashes and all step states are retained
in `receipt.json`. There is no broad pass or pass for the previously blocked
controls in either canceled run.

## Custody and checks

Full raw logs and source-verified indexes remain in
`/tmp/bicdb-native-foundations-ci`. Exact filenames, uncompressed byte counts,
SHA-256 values, source/run/job identities and six private focused-source ZIP
receipts are in `receipt.json`. ZIPs are restricted under the source-specific
`/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-*` and
`native-owned-validation-6e00ba51` directories. No native DBs, private TLS keys,
new process failure, engine replay or original-root fsync is introduced here.

```sh
python3 docs/evidence/native-registry-isolation-controls-2026-11-12/verify.py --tamper-tests
python3 docs/evidence/native-registry-isolation-controls-2026-11-12/verify.py --raw-dir /tmp/bicdb-native-foundations-ci --verify-private-zips --repo /root/bicdb --tamper-tests
```

These offline checks verify retained bytes and receipt consistency, not
independent execution authenticity. Original first-aac4/1 and whole CLI5/1,
source56f whole CLI3/3 with its uncertain `{}` write, original1d strict failure,
and earlier3ee evidence remain immutable and separately scoped. Nothing here
reconciles the uncertain write, changes the cold leader-null chronology, proves
all CLI assertions at a later source, demonstrates native data relocation or
five DATA-serving nodes, or qualifies a VM/physical deployment.
