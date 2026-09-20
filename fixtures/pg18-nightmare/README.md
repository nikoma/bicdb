# pg18-nightmare fixtures

These JSON fixtures feed `bicdb compat nightmare`. Fixtures with
`expectation: "expected_difference"` are intentional gaps and must include an
`expected_difference` explanation. All other fixtures are parity fixtures: any
PostgreSQL/BicDB behavioral difference is reported as a failure and gets a SQL
repro under the gauntlet report directory.

Product and industry-specific compatibility fixtures belong in integration
repositories. This public corpus contains only database-level PostgreSQL
compatibility cases.
