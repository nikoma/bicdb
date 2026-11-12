# Custody wrapper repair

The first full Core wrapper built its exact e8c426a7 helpers successfully, then
failed to parse the Python custody command because its string literal contained
a raw newline. No Core tests ran in that attempt. The dependent debug probe
wrapper failed its prerequisite before any build commands, and the original
95-test controller stopped before application launch. These are controller
failures, not product test or application outcomes.

All three original units were verified terminal with MainPID zero before new
wrappers were submitted. The corrected v2 Core command is syntax-checked before
submission and writes the same custody object with json.dump. It retains the
same product source, tests, assertions and waits. Its helpers and custody steps
have passed and the full Core tests are running; no passing broad outcome is
claimed here. Probe/application v2 controllers depend on that actual terminal
Core success. Original failed receipts/logs and the exact repair specs are kept.
