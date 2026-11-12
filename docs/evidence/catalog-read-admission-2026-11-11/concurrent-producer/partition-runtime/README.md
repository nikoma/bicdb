# PostgreSQL partition runtime comparison

This is an actual data-path diagnostic on private source `a68fe5f37d697555d7c337005aac4785e5887f70`, using the pinned debug server whose SHA-256 is recorded in result.json. It uses a fresh paged store with fsync enabled, PostgreSQL mode, no experimental page flags and no protected application fixture. All three owned server lifecycles ended cleanly.

Creating a range parent and child succeeds, but the parent scan omits an acknowledged child row. Inserting through the parent stores the row in the parent instead of the matching child. An explicit out-of-bound child insert is accepted; its surrounding transaction is rolled back. DEFAULT attachment is rejected with 42601. Separate parent/child rows remain separate through two clean reopens. This is a runtime defect, not merely a missing parser production.

The independent PostgreSQL 16.13 control checks parent union, child routing, out-of-bound rejection23514, DEFAULT routing and the default-partition catalog OID. It uses only the owned Native control database and rolls back its new schema. The protected source database and frozen application export are not modified.

These observations do not certify final main or a full application run. Source revisions and hashes are explicit. The index and metadata fixes have not implemented partition execution. Correct support still requires transaction-aware descendant scans and write routing, bounds/type/overlap checks, namespace and ownership handling, attachment/detachment validation, accurate catalog metadata and durability/security coverage. Accepting DEFAULT as metadata alone would leave the observed data behavior incorrect.

The same runtime defects were independently reproduced on the actual public main482 standard release binary. See `actual-main482-result.json` and the source/hash entry in `summary.json`. This separates the existing partition capability gap from private rename/catalog changes. It does not qualify a later binary or implement routing.
