# Post-joint recovery: target fence refuses the current normal purpose

Classification: **failed availability acceptance at the target fence**.
Source `b277260b0b114d1f892ecad8d2e5e7a03ba7da5a`,
[run 43228215328](https://github.com/nikoma/bicdb/actions/runs/43228215328),
Core job 131216860500: zero passed, one failed, 889 filtered, 64.40 seconds.
All-target and isolated-consumer compilation passed.

The test reaches actual three-member release, new-target ACK2 and Strong R2.
Initial metadata leadership is a declared fixture precondition; subsequent
quorums, data writes and native persistence are actual component execution.
It then attempts transaction 3: all NEW members protect its start, two followers
refuse Prepare, and the source transaction aborts without ACK3. All three slot3
records show `ever_commit=false`; only the source n3 has a stored no-effect
receipt at this observed stage. All NEW native histories remain R2, with local
visibility positions n1=4, n2=4, n3=2. These local positions are not range R.

Fresh actual NEW metadata authority selects range1/epoch4, leader n3, voters
n1/n2/n3, term1/read-index5 and NormalRecovery. Actual fence RPCs to n1/n2
succeed. The local target n3 returns:

    cluster node n3 rejected RPC cluster_data_operation_failed: cluster error: normal recovery cannot supersede a handoff recovery

The test fails at `post_joint_no_data_tests.rs:195`. Copied raw catalogs show
n1/n2 holding the new NormalRecovery fences at epoch4; n3 retains its original
CommittedHandoff fence at epoch2. Candidate/promise resolution, NoData apply,
canonical Abort3, Ready/Complete/Activate and the next ACK/Strong are **not
reached**. Three eventual business ACKs would include a command at range index4;
the test's ACK4 label does not mean four successful transactions.

This is a deterministic missing recovery contract after handoff. It is not
proof of a false successful read, lost acknowledged data, physical RTO or the
original Azure mechanism. A new qualified recovery purpose must validate the
actual protected NEW domain and preserve every old handoff promise/Commit
obligation; deleting the guard or substituting a purpose is not a repair.
The next smallest diagnostic successor must capture the exact fence refusal
and partial before/after state before failing the same positive acceptance.
Later NoData/prefix/readiness/generation seams remain separate open gates.

## Immutable evidence and derivative limits

The unchanged artifact13949222282 is retained in restricted experimental
custody: 315,490 bytes, SHA-256
`b14a311810460048a09ae9195256bafdfbca2753019e8e87e670b7b7492d84b7`.
Its 78 manifest-listed files (5,167,564 bytes) match exact hashes/lengths/file set,
including three separately captured stage reports. The companion receipt and
summary retain source, request role, error, command identity and selected slot
and fence fields without republishing generated database key material.
Raw complete job-log SHA-256 is
`0a37647b42bdafaacc42b4a0e1c75a3dc93b398f4e0ca7ef43be1d987cb34865`.

Copies were observed stable with actual engine owners alive. These are not
stopped-owner images, atomic power-loss snapshots or independent engine replays.
The JSON summary reads recorded reports and catalog bytes; it does not validate
native page-WAL, quorum signatures or live read behavior after partial fencing.

## Later exact diagnostic: same positive recovery acceptance still fails

Source `31cdbb83d6972357a64f6078ccca7d3c981d9c55`,
[run 43237027479](https://github.com/nikoma/bicdb/actions/runs/43237027479),
Core job 131247014520: **zero passed, one failed**, 899 filtered, 56.14 seconds;
all-target and isolated-consumer compilation passed. The test now records the
exact destination, original request/metadata purpose and partial fence state
before failing the same positive acceptance. It still reaches n3's exact
NormalRecovery-over-handoff refusal. The protected prefix and original ACKs
are unchanged; successful NoData apply, Ready and ACK4 are still not reached.
A safe refusal is not successful recovery.

Unchanged artifact13953157621 is retained in restricted custody: 350,094 bytes,
SHA-256 `80c9c77895729d1624e1b418ef0b515d9fb00b1b9788a50172fcecf256ff7235`.
All 81 manifest-listed files (5,517,882 bytes) verify against lengths and hashes,
including the new refusal and partial-progress reports. The file set is exact.
[The derived custody receipt](diagnostic-custody-receipt.json) does not expose
generated key material. Owners were alive; no engine replay, stopped-owner
snapshot or physical power-loss claim follows. Complete raw-job-log SHA-256:
`a956b144d0f2c992621cc33734f8de1f428088973b80ad519c20f365ece2fb0a`.
