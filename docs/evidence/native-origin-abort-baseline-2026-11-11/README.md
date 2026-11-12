# Native origin after prepare abort: actual baseline failure

**The positive liveness regression fails** at exact private source
`eafa6949a78ea4e97f1ea3271ff97f2de27d20c6` (`1.0.471-beta`). This identifies a
missing native-abort integration in the private native-image path. It is not a
current-main/Azure cause, acknowledged-data-loss or production qualification
claim. The actual original assertions are retained unchanged.

The fresh actual RF3 fixture begins with known native genesis0. Both remote
prepare deliveries are deliberately unavailable. The first real origin call
fails prepare quorum 1/2 and is **not acknowledged**. Actual source state is:

- raw index1 is `Aborted`, with raw last/resolved1;
- native applied/base remains known0 and its receipt is unchanged;
- no native Pending exists and the attempted record is absent.

Both remote data deliveries are restored without changing epochs, counters,
admission or gates. The second real transaction still refuses, before it can
prepare index2:

```
native command cannot extend this actual native base/configuration
```

The source remains raw resolved1/native0. No second ACK occurs. The source has
not become unknown or lost an acknowledged record; it cannot continue because
the raw abort consumed a range index while native history did not.

The source-level boundary is `NativeRangeCommandIntent::validate_native_base`,
which correctly requires exact next native index. The coordinator's
`abort_prepared` records only raw abort state. In this source,
`NativeLogicalHistoryEntryV1::canonical_aborted` is **cfg(test)-only** and has no
production native-abort-prefix producer. Removing the contiguous-prefix check
or setting a native counter would hide the defect, not repair it.

The regression requires a real second ACK after quorum restoration. On future
success it also reads the actual immutable native history and requires canonical
Aborted at index1, followed by native2. That later assertion was **not reached**
in this baseline; no fabricated Aborted history was inserted by the test.

A correct repair needs an actual no-data/rollback outcome and native receipt,
not an empty replacement transaction. Raw abort can occur inside admission
before outer SQL failed-attempt cleanup finishes. Native range index and local
DB visibility sequence are different domains; a no-data outcome may preserve
the previous visibility bound. Remote abort catch-up, historical abort recovery,
current NativeReady, subsequent replica HA and physical qualification remain
separate gates.

The single test ran 1.49 seconds (82.07 seconds including compilation), with no
resource-guard interruption and minimum available RAM 21,149,204,480 bytes.
It used actual fsynced stores and in-process delivery faults, not TLS or host
power loss. The raw command/source/environment, output and active resource
samples are included.

The complete stopped three-DB fixture and raw logs are in restricted durable
custody under `private/native-origin-abort-eafa6949`: 52 files / 405,693 bytes,
manifest SHA-256
`c737ba3f64c9629150c3340506627468880f83aea037e471ea681b63faabcf90`.
Source/destination hashes match; files/directories were fsynced, restricted to
0600/0700, and never reopened. Test-generated database keys stay private. No
global credentials, binaries or compiler caches are included.

Offline verification preserves failure and scope:

```
python3 scripts/verify-native-origin-abort-baseline.py
python3 -m unittest discover -s scripts/tests -p test_native_origin_abort_baseline.py
```
