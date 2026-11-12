# Generic repair invalidates native proof: actual baseline failure

**Expected-RED regression actually fails** at private source
`035fd22f40028a4d7c23e4a086af19791674677f` (`1.0.471-beta`). The test reached its
intended assertion, with no earlier fixture failure. This is a native replica
catch-up safety/availability integration defect, not proof of lost data or a
current-main/Azure causal explanation.

The fresh RF3 fixture uses actual fsynced stores, real coordinator decisions
and real service replies. Before any transaction, a legitimately recovered
coordinator with a delivery wrapper replaces the unused bootstrap admission.
No gate/admission was cleared, no recovered epoch was seeded, and no ACK was
fabricated. The wrapper drops **only Apply1 to n2**, after its actual CQ1, while
all other operations delegate to the actual embedded services.

The first origin call is acknowledged. n2 still has known native genesis0,
factory initialization valid, raw last1/resolved0 and actual CQ1; record1 is
absent there. The embedded bare native Apply limitation is explicit: other
bare Apply calls are refused rather than reported successful.

The second real origin admission invokes ordinary `repair_voter`. Its actual
resolved-command repair to n2 returns **success at raw resolved1**, and the
record is physically copied. Yet the target now reports:

| Observation | Before repair | After second origin |
| --- | --- | --- |
| Native applied | known 0 | **unknown** |
| Native invalidated | false | **true** |
| Factory initialization proof | valid | **unavailable** |
| Raw last/resolved | 1 / 0 | 2 / 1 |
| Record1 | absent | present |

The source has actual native2 and both origin records; both calls were
acknowledged. The regression requires a successful repair to establish actual
native1, and fails on `None` versus `Some(1)`. Its original assertions/source
are retained unchanged. A future explicit unsupported refusal may be a safety
control only if it preserves the original native/raw/row state; that would not
prove successful automatic native catch-up. In this fixture, a rejection-only
patch may also prevent the second ACK because both bare native follower Apply
paths are unsupported. Do not relabel that as availability success.

The source path is
`distribution_data.rs::apply_range_write_repair` →
`apply_range_write_suffix` → `apply_admitted_mutations_with_mutation`, followed
by raw `mark_applied`. The production `ApplyRangeWriteRepair` handler in
`distribution_transport.rs` invokes that same generic path. This handler
reachability is **source inspection**, not an executed production/TLS result.
The in-process test establishes the actual storage effect of that path.

Raw Applied is not a native history floor or readiness proof. An authenticated
native Apply/catch-up implementation must preserve real birth, original command,
history, current authority and local WAL/prefix receipt invariants. It cannot
repair this by trusting raw counters, disabling invalidation or suppressing the
unknown observation. Native leader/read/StrongGet qualification remains open.

The exact test ran 5.23 seconds (88.08 seconds including compilation), with no
resource guard interruption and minimum available RAM 18,187,968,512 bytes.
This is a functional observation, not performance or RTO qualification. Run the
recorded exact filter and command against its source; source snapshots alone
do not supply all repository dependencies.

The complete stopped three-DB root and run logs are in restricted durable
custody at `private/native-generic-repair-035fd22f`: 55 files / 686,899 bytes,
manifest SHA-256
`4aedc3ce0768b4575406791bc01d526f130b0d3c8b2b048e0f7f253345050877`.
Source/destination hashes match; files/directories are fsynced and restricted
0600/0700. Generated database identity keys stay private. No global credentials,
binaries or caches are included. Original and custody roots have not been
reopened. Public evidence includes the actual report, exact source, logs and
active resource samples, but excludes private database keys.

Verify this baseline without executing BicDB:

```
python3 scripts/verify-native-generic-repair-baseline.py
python3 -m unittest discover -s scripts/tests -p test_native_generic_repair_baseline.py
```
