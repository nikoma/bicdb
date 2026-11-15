# Native7e99 failure: final-image diagnostic XID expectation

Source: **1.0.520-beta / `7e99c9c96970b78a44402c975627c64e001adffa`**,
actual CI merge checkout `4c46808e3bbef80e5792cce2cbd9de8203839bbe`, tree
`8022e9f6e6d51f9eb4691ca877c328229b29c3b1`. Native job132883862065 in
run43737698638 failed: **396 passed, 55 failed, 1 ignored**, 452 distinct cases.
The remaining-workspace job was still running when this note was prepared;
its useful results were not cancelled. This is not a complete-run acceptance.

The full original Native log is 70,455,181 bytes, SHA-256
`b59d517171ecf60921177338f5593bef6422806caeecdb72ecd005f2a866c8db`;
its gzip is 5,595,906 bytes, SHA-256
`7b7df7c518c498dd6409604b548791ae9fd62c1620865c829d3b906557d9542c`.
The original failed source and evidence remain unchanged.

## Finding and correction

`native_first_actual_final_wal_cancel_retains_xid_and_blocks_next_mutation`
failed at `first_owned_tests.rs:1494`: `actual_commit_returned` was the JSON
number20, while the assertion expected boolean true.

The producer in
`crates/bicdb-core/src/db/range_image_native/registered_remote_image/final_publication.rs`
declares `commit_returned: Option<u64>` and deliberately serializes that actual
returned XID. The existing registered final-image control in
`target_final_image_tests.rs` also reads it as a positive integer. Changing the
producer to a boolean would discard useful evidence and break that contract.

The correction changes this single expectation to require that numeric value
to equal the actual protected committed XID already read by the same control.
It preserves the positive-XID check, current/archive equality, actual native
transaction-status Committed check, exact post-commit state, original WAL fault
cut and all later next-mutation/SDK assertions. It changes no production code,
request budget, caller token, IO or native permission.

| Evidence item | Classification |
| --- | --- |
| Original Native job and complete final-WAL scenario | Failed |
| Selected post-WAL cut, positive XID, current/archive equality and Committed assertions preceding1494 | Passed within that original scenario; not whole-scenario acceptance |
| Boolean expectation against intentional numeric diagnostic | Confirmed test-contract defect |
| Corrected exact-XID expectation | Source correction only; runtime not tested |
| Assertions after1494 in that scenario | Not reached in the original failed run |
| Current physical HA/RPO/RTO/scale-out qualification | Not established by this control |

The smallest reproducer is the existing case on the exact failing source:

```sh
cargo test -p bicdb-core --lib \
  native_first_actual_final_wal_cancel_retains_xid_and_blocks_next_mutation \
  -- --nocapture --test-threads=1
```

That command is a reproducer, not a newly executed test result. After the
runtime fix batch stabilizes, the corrected case must reach every original
remaining assertion under its original windows. Formatting and diff checks
alone cannot establish that outcome.

## Other actual Native failures remain independent

The55 failures comprise34 FIRST and21 hot-path cases. Twenty hot cases fail
at actual catalogue Open (`persist_local_catalogue_activation`), before the
new first-Policy stage marker; no such marker was emitted. The large-head
fault control reports its intended fault did not fire. FIRST failures include
service/metadata contention, an already-active maintenance attempt, original
source-start revalidation errors and a real installed-versus-current epoch
policy mismatch. Error text alone is insufficient to equate those causes.

In particular, current Node1 retains epoch1 while actual allocation advances
the selected range to epoch2 with unchanged leader/voters. The stock SDK append
in the cross-protocol tail then fails its strict writer-policy check. The
range check and original selected/untouched application assertions must remain;
a lawful actual-owner policy continuation is required. A later SourceDescribe
refusal after a failed SourceStart must also not be reported as successful
pending progress.

These failures require their own source/trace evidence and corrections. This
one assertion repair does not resolve them, establish data loss, explain the
historical Azure incident, qualify the new uncompiled source-retirement units,
or demonstrate automatic four-to-five convergence or throughput gain.
