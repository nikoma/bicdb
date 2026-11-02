# Source foreign-key timing and validation inventory

The upgraded Oracle release-13 source has 3,826 foreign keys. All are enabled,
deferrable and initially deferred. Of these, 912 are validated and 2,914 are
not validated against pre-existing rows. Delete actions are 447 CASCADE, 170 SET
NULL and 3,209 NO ACTION. All parent constraints resolve within the exported
schema; one foreign key has multiple columns. The [metadata inventory](source-states.json)
contains counts only, without source rows or connection configuration.

The target still has zero migrated foreign keys. This inventory does not
qualify referential integrity. Native durable ForeignKey metadata currently
has no constraint-timing fields, so importing these declarations requires
transaction-timing work as well as migration tooling.

Required acceptance before counting the migration complete:

1. Persist timing and validation state, with a compatibility rule that prevents
   older writers from silently changing the deferred enforcement contract.
2. Check newly written keys at the correct statement or transaction boundary,
   including child-before-parent transactions, SET CONSTRAINTS, savepoints,
   failed commits and recovery. Cover PostgreSQL and Oracle error behavior
   independently.
3. Preserve the meaning of source NOT VALIDATED state. Establish an inventory
   of existing orphan rows and enforce future writes without treating that
   state as disabled enforcement. Validation and any data repair need separate,
   reproducible evidence.
4. Qualify CASCADE, SET NULL and NO ACTION with nullable/composite keys,
   concurrent parent/child changes, privileges, cancellation and rollback.
5. Import every source relationship, reconcile names, referenced columns,
   catalogs and state, and run application workflows with enforcement enabled.

Data import, accepted routine definitions and matching view results do not
satisfy these referential-integrity gates. The complete compatibility and
migration programme remains open.
