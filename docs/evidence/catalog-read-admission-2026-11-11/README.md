# Catalog read admission and exact record reservations

This is private implementation evidence, not qualification of public source
1.0.471-beta or a completed SQL rename producer. The conservative SQL/native
owner and recovery quiescence gates remain enabled. Original application
assertions, the ten second method timeouts and fsync guarantees are unchanged.

All commands ran on the S3 build host, in the same source checkout path, with
one Cargo job, incremental compilation disabled, the serial source lock and
active memory monitoring. Each receipt identifies its exact source and command
scope. The full logs and submission specifications are retained. Negative
tests stop at their first failed storage-mode cell; they are not evidence that
their later ServerPaged cells executed. Positive tests exercise the modes
declared in the individual test source.

| Exact source | Result and scope |
| --- | --- |
| `d1c7e16abea4911de99280bc2d8924b2eb529d3b` | Integration of frozen public candidate `25778e3c` into the private rename branch: all five commands passed, 98 test executions plus the version check. Includes the existing 20 SQL rename library tests, with 58 child-process cuts and 116 reopens, and native replication/frame checks. These are process exits, not hard VM or power-loss tests. |
| `2970861b7bfaae7bedd18ba44e5dec497953b99f` | Tests-only reader control: six actual failures. Cold and warm point SQL, catalog scans and public Describe exposed reserved schema metadata. Literal SQL and unrelated native acknowledged writes completed before each refusal assertion. |
| `86f12f19feae2f0c83547c636bf9660e89930fc6` | All three commands passed, 50 focused tests. Read-only native owner guards, metadata checks before caches and an inference denial latch restore the missing reader fence. Read identity grants no internal mutation authority. |
| `73b120fc68f416ffbf97be464b49ba68dc7b65f8` | Tests-only resource characterization: five actual failures. Its local adapter deliberately maps requested records to the old whole-collection API; that API never promised record-level behavior. Failures show unrelated-record blocking, disjoint-owner blocking and undeclared single/batch effects admitted by that adapter. |
| `c5edfd4954277efbbefd64b6806470503722c5d3` | All three commands passed, 40 focused tests. The adapter now uses the new exact-record API; assertions are preserved. Full relation and exact metadata record reservations are atomic, point observations escalate conservatively after 128 keys, backing identity is pinned and internal record effects are checked before binding or row locking. Native ordinary business access retains its existing collection-level bindings. |
| `47f1b0384ca54210b81ce67a64ebfed1a2309d0c` | Tests-only SQL owner control: two passed, three failed. Cold/warm unrelated query and Describe availability already passed. Imported owners could not read their own reserved metadata, SQL observations did not remain attached to their transaction, and one SQL batch retained the wrong owner context around COMMIT. |
| `a339fa54498ac001fd9835ad09525f0522b24971` | Both commands passed, 53 focused tests. SQL text/AST statement boundaries use the actual live native read owner; anonymous frames mask an ended enclosing owner. Utility statements avoid acquiring a query snapshot. No mutation delegation or global gate relaxation is introduced. |

`manifest.json` hashes the 21 original receipt/log/specification files. Results
from an earlier source remain evidence for that source. In particular, earlier
whole SQL library/general-suite results are not broad qualification of the new
reader, record or SQL statement-context changes.

Still open: the complete SQL producer and durable dependency inventory,
implicit/deferred and routine owner transitions, all metadata families and
cached wire Describe paths, precise privilege/type/routine availability,
namespace lifetime and portable backing identity integration, durable scope
reconstruction on restart/standby, and broad final integrated validation.
Focused API tests do not establish those properties, distributed SQL,
application acceptance or production HA readiness.
