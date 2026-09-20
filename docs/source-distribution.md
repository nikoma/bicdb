# Source distribution notes

The source snapshot contains the current BicDB license and scope notices,
engineering documentation, tests, and selected historical benchmark evidence.

## Historical evidence and examples

Host addresses, infrastructure hostnames, SSH account names, and machine-local
paths in published documentation and benchmark metadata have been replaced with
example identifiers. Addresses in `192.0.2.0/24` and hostnames under `example.com`
are placeholders, not accessible benchmark services. Configure your own hosts,
accounts, and paths before using the commands. Numeric measurements and test
outcomes are preserved; historical artifact paths are not download locations.
Original evidence is retained privately. Anonymized files differ from the original
artifacts, so hashes of original artifacts do not authenticate rewritten files.

Historical investigations describe the revision and test conditions recorded at
the time. They are not blanket claims about the current release. Unavailable hosted
CI remains identified as unavailable, separately from tests that actually ran.

## Excluded development material

Local database directories and backups (`testdb/`, `testdb-full.bicbackup`) are
excluded and ignored. Tests should create their own temporary databases or use
explicitly documented fixtures. Superseded source-release and Apache-readiness
assessments are archived privately; current terms are in [LICENSE](../LICENSE)
and [LICENSE-SCOPE.md](../LICENSE-SCOPE.md).

Public localhost TLS test keys remain in their documented fixture directory.
They are intentionally public test data and must never secure a deployed service.

These changes affect the source snapshot, not existing Git history, release assets,
or deployments. A new repository should be populated from reviewed tracked files,
not by copying a working directory containing private or untracked artifacts.
