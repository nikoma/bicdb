# Inventorying upstream application tests

Use the compiled test JAR from the pinned application build to inventory direct
JUnit annotations without initializing application classes or executing tests.
The JDK `javap` tool must be installed. The inventory tool makes a private JAR
snapshot, checks an optional expected SHA-256 before inspection, verifies exact
class coverage, and removes its temporary snapshot/disassembly afterward.
Reports contain names, annotation kinds, hashes and counts; they do not contain
method bytecode or constant-pool string values.

```sh
python3 examples/oracle/idempiere/upstream_test_inventory.py \
  --jar /private/upstream-tests.jar \
  --expected-sha256 "$TEST_JAR_SHA256" \
  --report /private/upstream-test-inventory.json
```

`--prefix` defaults to `org.idempiere.test.` and can select another package.
`--javap` can select a specific installed JDK tool. Reports cannot overwrite the
input through the same path, a symlink or a hardlink. A checksum mismatch,
missing class, duplicate identity, malformed metadata or tool failure stops the
inventory instead of producing a successful partial result.

The report identifies direct JUnit 4/5 tests, parameterized/repeated tests,
test factories and templates. It retains explicit disabled annotations and
class hashes. This is candidate discovery, **not an execution plan or a pass
count**: inheritance, composed annotations, runtime conditions, parameterized
invocations, generated tests, application fixtures and external dependencies
still require framework discovery and execution. An inventory does not qualify
SQL behavior, application workflows, an Oracle migration or production use.

The [pinned application inventory](evidence/idempiere-upstream-test-inventory-2026-11-05/README.md)
provides a reproducible starting point for tracking remaining application classes
alongside business workflows and recovery/cutover checks.
