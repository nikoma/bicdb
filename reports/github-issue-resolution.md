# GitHub issue resolution

The fourteen open issues examined in this work were handled sequentially. Each
runtime change has focused regression coverage and was merged to `main` through
the linked pull requests. Tests ran on the existing build VM with two Cargo jobs;
no Rust compilation ran on localhost.

| Issue | Result | Pull requests |
| --- | --- | --- |
| #879 | CTE authority follows lexical scope and avoids quadratic sibling-scope copying. | #880, #883 |
| #874 | Handled exceptions discard broker events produced by the failed statement. | #881 |
| #875 | Savepoints and handled exceptions restore deferred hooks as well as writes, locks, and broker events. | #882 |
| #868 | Trusted identities do not bypass authored trigger authorization. | #884 |
| #861 | Role deletion checks ownership and privilege dependencies; schema authorization preserves the requested owner. | #885 |
| #878 | Locking queries evaluate output expressions only for selected, locked rows. | #886 |
| #877 | A skipped join candidate releases its partial locks while preserving prior locks and writes. | #887 |
| #740 | Comma/CROSS joins do not lose rows through a NULL index prefix for an unjoined relation. | #888 |
| #867 | TLS server endpoint binding uses the certificate signature's digest, including RSA-PSS parameters. | #889 |
| #872 | SCRAM channel binding has explicit require/prefer/disable policy and validated interoperability tests. | #890 |
| #864 | Package ingestion bounds encoded input, decoded content, module keys, and module counts before oversized allocation. | #891 |
| #189 | Bench tooling is optional in the CLI; benchmark and comparison features opt it in. | #892 |
| #873 | An optional authenticated operator API manages live login identity/credential records, with bounded input, private operation records, serialized persistence, and documented session semantics. SQL credential mirroring cannot override its persistent host-authority boundary. | #893 and its credential-boundary follow-up |
| #518 | Procedural migration coverage, whitespace-aware function privilege parsing, and successful fresh provisioning/API startup. | #894, #895 |

Affected core, SQL, pgwire, application-runtime, and CLI suites passed at their
respective milestones. See [startup verification](issue-518-startup-verification.md)
for the full external migration and optimized Node API smoke results. That test
applied 80 migrations, applied zero on repeat, and started the API in 9.03 seconds
with the original 60-second allowance. It used isolated test data and unchanged
application artifacts, not a production deployment or a full application behavior
suite.

GitHub-hosted checks were unavailable for these validation runs. This is separate from the VM test results. Formatting and diff
checks passed; the production-name guard retained its 50 pre-existing matches
and gained none from these changes. No release was published or deployment
changed as part of this issue work.
