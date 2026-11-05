# Original receipt and shipment posting tests

The original upstream `org.idempiere.test.base.InOutTest` class passed all five
tests, with zero errors, failures or skips, on an isolated BicDB copy using the
application's Oracle provider and PostgreSQL driver:

- material receipt posting;
- material shipment posting;
- shipment reposting;
- customer-account freight rules;
- shipment/receipt credit checks.

The posting cases exercise the upstream expected accounting-entry assertions,
posted flags and cost-of-goods/product-asset debit/credit checks. These are the
original test assertions, not newly invented application-specific expected data.
The fixture uses the pinned original test bundle without source changes.

Before Java starts, the public importer `blob-types` phase reconciles all 16
source BLOB columns. Complete affected-table fingerprints are preserved, as are
the original 62 routines and 160 views. The migration report and importer hash
are recorded separately from the original JUnit results. The owned JVM and
database both exit 0. The accepted application database is not used for test
mutations; the test runtime has loopback networking only.

This is one original test class on the recorded source snapshot and binary,
not the whole application or production qualification. In particular, the
3,826 source foreign keys have not yet been installed in this application copy;
their deferred timing and validation state remain a separate acceptance gate.
The later InvoiceTest attempt was stopped by its memory guard before Java
started, so it supplies neither an application pass nor an assertion failure.
The [programme](../../oracle-compatibility-programme.md) remains open.
