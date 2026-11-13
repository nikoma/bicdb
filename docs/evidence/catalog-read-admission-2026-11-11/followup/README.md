# Catalog reader follow-up evidence

This directory preserves eight source-pinned batches, including failing
controls. `runs.json` identifies their revisions and outcomes. JSON receipts
and specifications are original bytes. Gzip logs retain the complete output;
`manifest.json` includes both compressed and uncompressed hashes.

| Source | Observed scope |
| --- | --- |
| `658e0b99` | Six commands passed: version consistency, focused Core/SQL checks, Core/SQL libraries and general SQL. |
| `3901da53` | Both procedure-loop controls failed after COMMIT with an expired catalog owner. Native PostgreSQL independently returned the expected two rows in each control. |
| `05b3d442` | Per-expression owner selection fixed those controls; 39 focused SQL, 891 SQL library and 419 general SQL tests passed. Core code was unchanged from the earlier qualified batch. |
| `26393121` | Three readonly-default controls passed in both stores. The suspected default-helper dependency gap was not demonstrated; no speculative fix was made for it. |
| `77be52b4` | Real TCP protocol control: affected cached Describe stayed fenced, but three unrelated-query cases failed. |
| `ea7c8611` | Expanded TCP control: one passed, six failed, including unrelated declared types. Failed cases stop at their first reached store; the logs establish the actual reached scope. |
| `08442cac` | Point admission for cached OID identities allowed six cases; an unrelated table-row cast still failed because its declaration resolver scanned the complete schema catalog. |
| `b1e5b872` | The shared declaration resolver now admits the selected stored schema key. All nine wire/API controls and 27 existing catalog/default/owner/routine tests passed, across both stores where the fixtures iterate them. |

Built-in numeric OIDs use the immutable type registry after checking recovery
health. Declared OIDs are classified using private generation-validated
directories, then the selected stored catalog key is admitted before metadata
is exposed. Public catalog scans retain whole-catalog admission. An unknown OID
still requires conservative whole-catalog admission and cannot convert a
reservation refusal into an ordinary unknown-type result. Name resolution uses
the existing quoted-identifier parser; quoted table names containing dots have
a separate wire regression.

The final controls retain affected table and standalone-wrapper refusals, cached
Describe fencing, and recovery after scope release. The nine tests include a
direct OID API control for affected and unknown identities, in addition to the
TCP cases. They preserve fsync and the original application assertions and
timeouts.

This is private reader qualification. Native CollectionMeta, namespace UID,
WAL/image formats and the global producer/recovery gates were not changed by
the OID batch. The final SQL metadata changes still require broad validation
after integration with the public isolation and namespace-identity foundations.
These results do not qualify a complete transactional rename producer,
distributed DDL, an application deployment or production HA.
