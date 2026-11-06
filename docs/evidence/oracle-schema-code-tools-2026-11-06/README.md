# Stored-code migration tooling qualification

The public tools now plan and install standalone source routines and views,
verify catalog definitions on resume, reject unjournaled or changed objects,
and accept source-bound static view dependency reports. The dependency capture
command checks Oracle routine/view source against the saved snapshot before
reading dictionary edges; cycles, missing same-schema views, and remote links
are rejected. Target view types and execution remain separate acceptance gates.

[Validation](validation.json) records the tested files, engine, and check counts.
The native test uses a synthetic source graph to put a parent view before a child
listed first in the manifest, verifies a numeric result, restarts the database,
resumes installation, and rejects later routine and view changes. Its disposable
database stops cleanly and is removed. It does not use a live Oracle source.

The broader Python suite initially failed two Java compilation tests because
the isolated checkout lacked the JDBC SDK jar. After an offline Maven package
(10 tests passed), all 122 enabled Python tests passed; 29 requiring separately
configured native/source services were skipped. The native installation test
was run separately with the qualified compatibility binary under a memory guard.

Two observed limitations remain explicit: this candidate rejects `WITH READ
ONLY`, and accepting a child view before its parent exists can retain unresolved
column types. Dependency ordering prevents that fixture's premature creation;
it does not qualify general invalidation/rebinding or complete view semantics.
No production allocator, deployment, ownership, full application, or Oracle
conformance claim follows from these checks.
