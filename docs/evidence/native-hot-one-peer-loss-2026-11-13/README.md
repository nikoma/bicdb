# One unavailable RF3 peer: required HOT recovery failed

**FAILED**, source `ae58458836a070975f1643333c104d0bddce7cc2`, [run 43345593406](https://github.com/nikoma/bicdb/actions/runs/43345593406). Core job `131624117250`: 0 passed, 1 failed, 980 filtered, 21.74 seconds. All-target compilation and isolated consumer job `131624086315` passed. This is three actual initial NativeHosts with mTLS in one Core process, not CLI, separate processes or VMs.

The genuine prerequisites executed before the required failure. Old command 1 received an external ACK and exact full-value Strong reply. Command 2 reached actual Core `CommitOutcomeUnknown`, sequence `None`, transaction 11; the actual release callback returned successfully and its normal APP handler exited. The origin remained native R1 with Pending2; both peers independently proved native R2, typed local B2, identical H0/H2 and both full records. This is committed materialization evidence, not an inferred user ACK for command 2.

The test held only the real controller state mutex while all three process-owned workers were already started. Inside the original common 20-second health window it stopped/drained n1, observed actual `ConnectionRefused` after 13.901439 ms, and obtained a fresh actual surviving HostMQ (term 1, read index 1, unchanged C1). It then released only the scheduling mutex. There were no direct recovery or manual maintenance calls.

All 15 observed automatic worker attempts stayed at Fence position 0 targeting n1; every one failed with actual connection refusal while more than 4.9 seconds of its finite attempt remained. The last diagnostic still had the original registration live and origin native R1/Pending2. The outer loop's requirement to visit every voter prevents progress despite the available majority; none of Certify/Apply/Prepare/Complete/Activate/Publish executed in this witness.

The two health receipts were at 7.680990351 and 15.333326103 seconds. Each unconditionally attempted old-value Strong, command-2 Strong, next Append and next Strong. All six post-loss read APIs timed out; both next Append APIs returned recovery-incomplete errors without ACK. There was no successful absence or wrong value. The loop stops before another round when fewer than eight seconds remain. **This does not establish continuous outage for the full 20 seconds, a hard logical-call IO bound, or an RTO of 21.74 seconds.** The required healthy read/write gate failed; safe refusal is not success.

The full 1,062-row fsynced journal records 12 API intents/outcomes and 277 actual transport attempts: 15 Append (1 ACK, the first actual Unknown, 13 recovery-incomplete replies), 49 Strong (1 pre-fault exact value, 45 typed unavailable replies, 3 incomplete/error exchanges), 162 bootstrap-snapshot discoveries (112 actual successful snapshots, 50 without complete replies), 50 topology discoveries without complete replies, and one successful actual HostMQ acquisition. Numeric request IDs are transport-local; correlate attempt UUID plus logical context. `receipt.json` preserves every complete decoded request/reply, all transport phases/error chains and all logical outcomes. The original ZIP preserves complete raw frames without rewriting the first Unknown.

Artifact `14002265636`: 258,094 bytes, SHA256 `a9c29226e14f1362780d748b9950e8bd934a18d1e13150df0d83f6906f71e1fb`. Original journal: 3,409,270 bytes, SHA256 `61a81a1eedc0dba96a70a83f25d34ffc5ab4fdab1f94ad73e88f08fecee9db70`. Executed test binary was 137,716,736 bytes, recorded SHA256 `7723c4faa066ecd15442d34fe5f7c31da0178373d9163634958fac9ab382df67`; binary custody is not retained. Original artifact private custody is `/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-hot-one-peer-loss-ae584/14002265636.zip`. Cleanup reported no errors.

The ZIP and raw logs contain code/log evidence only. The original root `/tmp/.tmpO2EuME` is a retained path statement, **not portable DB/WAL/catalogue custody or engine replay**. Direct bootstrap's raw frame is not covered; controller-internal requests have phase diagnostics, not a complete global wire journal. No rollback, permanent data loss, physical qualification or Azure causality is claimed. Earlier all-three-reachable success remains scoped to its original sources; this failure is preserved unchanged.

Run offline without opening any engine:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-hot-one-peer-loss-2026-11-13/verify.py --tamper-tests
```

The verifier checks the original ZIP/raw hashes, exact source and manifest, complete journal-to-derived-attempt parity, actual source births, prerequisites, health clocks, failed classification and eight semantic negative controls. It verifies retained bytes and internal consistency, not independent execution authenticity.
