# Oracle BLOB read-version checkpoint

This checkpoint implements internal origin-local read histories and their
transfer through SQL session caches. Stored-column identity and locator mutation
are not connected to this layer yet. It does not establish a deployed release,
complete application acceptance or general Oracle compatibility.

The [native acquisition observations](source-acquisition-observations.json)
record 11 cases repeated twice on Oracle 23.26.3.0.0. All 22 matched, including:

- ordinary versus FOR UPDATE acquisition after an append and subsequent rollback;
- separate pending alias snapshots and common writer commit state;
- ordinary snapshots surviving owner replacement/deletion committed by their
  own transaction, contrasting with the earlier locked-locator observations;
- readable bytes surviving rollback of a newly inserted value;
- reuse of old locators in a later transaction, whose rollback restores the
  current transaction's baseline. This last group remains a future binding
  requirement, not an implemented cross-origin write interface.

The probe creates only a nonce-owned table, drops it and checks that it is gone.
It performs no BicDB comparison. Its appended diagnostic mutation occurs after
capturing d/e/x; the final row observations include that diagnostic attempt.
The shared CLI wrapper also reran the ownership probe: all 16 observations
matched after the wrapper refactor.

Reproduce with a dedicated non-administrative reference schema and
`python-oracledb`:

```sh
python3 examples/oracle/reference/persistent_blob_acquisition.py \
  --oracle-config /path/to/reference.json \
  --output /path/to/acquisition-observations.json
```

See the [layer contract](../../oracle-lob-read-versions.md) for implemented
scope and remaining integration. The [validation record](validation-results.json) distinguishes
the SQL library run, focused follow-up tests and protocol/workspace checks;
these are local development evidence, not production qualification.

The full SQL library run passed 668 tests with three existing ignored benchmarks
before the history-only retention follow-up. The final focused run passed 15
tests; final protocol runs passed 45 Oracle and 177 general tests. Workspace
checks passed. One broader run was stopped by its memory guard; its completed
Oracle suite was retained and the remaining protocol/workspace checks resumed.
Native history tests disable fsync and establish transaction/read-version
behavior, not a durable LOB storage contract.
