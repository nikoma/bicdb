# Raw migration batches and record arguments

The EHR email capability exposed SQL compatibility failures in anonymous-block
batches, multi-statement function privilege grants, and function row-source
arguments taken from a PL/pgSQL record. The record regression also exposed
one-column INTO values losing their declared record shape.

The fixes keep raw recognizers confined to one statement, resolve record fields
through the existing routine-variable lookup, and preserve the object shape of
a declared record even when its SELECT has only one column. Scalar INTO targets
retain positional assignment. The record regression alternates north/south/north
to detect captured tenant values.

The final three regressions were run with all four source fixes removed:
**0 passed / 3 failed**, with the original SQL diagnostics. Restoring the fixes
produced **3 passed / 0 failed**. Both actual command logs and compiled SQL/test
artifact hashes are retained. The SQL unit suite passed **717 tests**, with four
pre-existing ignored tests. Eight focused integration suites and the pgwire unit
suite passed; exact counts and commands are in qualification.json and logs.

Validation used source base 978e6dd; subsequent main changes through f13ccb0
were Oracle examples and documentation, with no Rust changes. Builds ran with
incremental compilation disabled and active RAM/disk headroom monitoring.
Completed test executables were hashed and retired, preserving reusable build
libraries and frozen runnable candidates. This is engine validation, not evidence
of an EHR production cutover.

The separate pgwire protocol suite also passed **183 tests**; this is additional
to the **132** pgwire unit tests in qualification.json.
