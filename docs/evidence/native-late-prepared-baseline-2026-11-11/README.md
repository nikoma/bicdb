# Late Prepared follower: retained activation baseline failure

**Actual local candidate liveness failure**, at exact private revision
`e3572fc655fe0dff9e634f56c5c01019ab8de173` (`1.0.471-beta`).
This is not a current-main/Azure cause, production qualification, physical
failure, new application ACK, StrongGet or incoming Activate RPC result.
The test itself remains unchanged and requires successful late completion;
its recorded result is **FAIL**, not a passing refusal test.

The actual RF3 fixture acknowledged one record, then ran historical recovery
and prepared all three native replicas at R=1. The configuration's actual data
leader is n0; its independently elected durable metadata leader is n1. These
are explicit test topology preconditions, not a failover experiment.

The test used real loopback mTLS participant RPCs and actual native/catalogue
proofs to complete n0 and n2. Their local Runtime activation APIs each collected
real completion evidence and returned `ActivatedAnchor`. Both retained the same
logical R/H; their local birth/receipt/serving hashes correctly differ. n1
remained physically `Prepared` at the same R/H, with its normal mutation gate
closed. An Activated node correctly refused to answer NativeReady.

The test stopped only its n2 server endpoint, leaving real fresh metadata
quorum n0+n1 available. The incoming authenticated Complete request to n1 then
failed:

```
closed current native-ready majority unavailable
n0: ... no actual recovery session for native readiness
```

The available pair is Activated n0 plus Prepared n1. The baseline requires a
fresh nativeReady quorum or a completed/activated decision quorum; this pair
provides neither, even though n0 holds an actual durable activated decision.
This is the **first observed failure boundary**. It is an availability/phase
integration defect, not a false-Ready acceptance or proof of irretrievable loss.
The original acknowledged record still matches on both available nodes.
Any later distinct durable-decision refinement must preserve this baseline and
receive its own source-pinned proof tests; no API/DTO alone closes the finding.

At this exact revision, two completion/activation smoke controls and three
existing Complete TLS controls separately passed. Their success does not turn
the late-follower test green. The failed test ran 7.59 seconds; this is not an
RTO qualification. Raw commands, source/environment hashes, stdout/stderr and
active RAM/disk/pressure samples are included. No build was interrupted by the
resource guard.

The complete stopped raw three-DB root and these run inputs/logs are in
restricted durable custody at `private/native-late-prepared-e3572fc6`:
65 files / 1,071,545 bytes, manifest SHA-256
`35f0cce9233a063c9df3d5cf9578da836280d20135761366e4ec3ba8fc0bf54e`.
All source/destination hashes match; files/directories were fsynced and restricted
to 0600/0700. Test-generated database keys remain private. No global credentials,
binaries or compiler caches are included. Original and custody DB roots have
not been reopened.

Use the recorded exact source and `activation_tests::` filter to reproduce.
`MANIFEST.json` hashes all public evidence files. Verify the bundle with:

```
python3 scripts/verify-native-late-prepared-baseline.py
python3 -m unittest discover -s scripts/tests -p test_native_late_prepared_baseline.py
```
