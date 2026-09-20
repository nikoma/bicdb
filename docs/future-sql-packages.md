# SQL Packages — Initial Implementation Specification

Status: queued as future-enterprise roadmap item 26. This document is a design
target, not a statement of current BicDB capability.

## Goal

Add SQL packages as namespaces and public contracts for related constants,
functions, procedures, types, cursors, and private implementation helpers.
Priorities are familiar package syntax, explicit public/private boundaries,
deterministic dependency tracking, schema-bundle compatibility,
distributed-safe semantics, reliable replacement, package privileges, and no
implicit mutable session state in the initial generation.

## DDL surface

Accept `AS` and `IS` in specifications and bodies:

```sql
CREATE [OR REPLACE] PACKAGE package_name AS
    package_item;
END [package_name];

CREATE [OR REPLACE] PACKAGE BODY package_name AS
    package_body_item;
END [package_name];

DROP PACKAGE [IF EXISTS] package_name;
DROP PACKAGE BODY [IF EXISTS] package_name;
```

Dropping a body removes only its implementation. Dropping a specification also
removes its body and fails when dependents exist unless an explicit, supported
cascade mode was selected.

## Public specification

The specification may initially declare immutable constants, functions, and
procedures. Reusable public types and cursors are admitted only after their
underlying BicDB features are stable first-class objects. A non-constant
package variable is rejected.

```sql
CREATE PACKAGE billing AS
    c_default_currency CONSTANT TEXT := 'USD';
    FUNCTION calculate_total(p_invoice_id IN UUID) RETURN DECIMAL;
    PROCEDURE finalize_invoice(p_invoice_id IN UUID);
END billing;
```

Every public name is part of the package contract. Members declared only in the
body are private.

## Body and visibility

The body contains public implementations plus private functions, procedures,
constants, and supported local types. Private members resolve inside the body
but are never callable or grantable outside it.

External calls are package-qualified:

```sql
SELECT billing.calculate_total($1);
CALL billing.finalize_invoice($1);
SELECT billing.c_default_currency;
```

Within a body, unqualified resolution order is local variables and parameters,
private members, public members, then schema-level objects under existing BicDB
rules. Explicit qualification always works. Identifier folding and quoting use
normal BicDB behavior.

## Signatures and overloading

Initial package generations do not support overloading. Duplicate public names
are rejected regardless of parameter types. Every public declaration has
exactly one matching body implementation. Matching covers member kind, name,
parameter count/order/names, parameter directions, parameter types, and
function return type. Missing, duplicate, or conflicting implementations fail
compilation before publication.

## Constants

Public and private constants are immutable. Initializers are restricted to
literals and deterministic compile-time expressions. Table reads, current
time, randomness, network access, nondeterministic functions, and mutable
session state are rejected.

## Transaction and distributed semantics

Routines execute inside the caller's transaction and never silently create an
independent transaction. Errors follow existing procedural SQL behavior.
Invisible retries are forbidden unless proven safe by declared BicDB retry
semantics.

Initial packages are stateless between calls:

- no mutable package variables;
- no initialization block;
- no connection affinity;
- no process- or node-local hidden state; and
- no behavior change after routing, retry, failover, or session migration.

Immutable definitions and constants may be cached. A future state model must be
explicitly declared as `NONE`, `TRANSACTION_LOCAL`, or `SESSION_LOCAL`, with
`NONE` remaining the default.

## Catalog and hashes

Persist package specifications, bodies, and members as logical catalog objects.
Specification metadata includes package/schema identity, name, owner, source,
normalized form, specification hash, public contract hash, timestamps,
validity, and compatibility version. Body metadata includes source, normalized
form, body hash, compiled artifact, timestamps, and validity. Member metadata
includes identity, kind, visibility, ordinal, parameters, return or constant
type/value, signature hash, and dependency set.

The public contract hash is derived only from the specification. Private body
changes never alter it.

## Atomic replacement

Body replacement compiles and validates a private generation while the current
body remains active. Successful publication is atomic. Failure leaves the old
body usable and does not invalidate callers when the public contract is
unchanged.

Specification replacement computes a new contract hash, detects incompatible
member changes, checks dependents and schema-bundle policy, and never publishes
a partial contract. The preferred path stages specification and body in one
signed schema bundle and activates both atomically. If replaced separately, a
new specification invalidates its old body until a matching body is installed.

## Schema-bundle integration

Packages are first-class executable-schema records. A bundle may contain:

```text
packages/
  billing.spec.sql
  billing.body.sql
```

Validation parses all sources, matches public implementations, resolves
dependencies and privileges, checks contract compatibility, rejects forbidden
distributed state, and deterministically accepts or rejects dependency cycles.
Package rollout participates in the same signed, bounded, resumable, atomic
fleet activation as the rest of the executable schema.

## Dependencies

Track public contract dependencies separately from private implementation
dependencies. Record callers of the specification and body dependencies on
tables, views, types, routines, and packages. Body-only replacement with an
unchanged contract does not invalidate callers. Public signature changes and
referenced object changes invalidate or block dependents deterministically.

## Privileges and execution security

Support package-level execution authority:

```sql
GRANT EXECUTE ON PACKAGE billing TO billing_operator;
REVOKE EXECUTE ON PACKAGE billing FROM billing_operator;
```

The privilege covers public routines only. Private members cannot be granted
directly. The initial rights mode is explicit in the package contract; a
definer mode may be the first supported mode, with current-user mode added
later. Ownership or definer execution never implies row-security bypass.

Every call preserves trusted tenant, workspace, actor, authentication, role,
scope, transaction, trace, audit, row-policy, field-policy, and mutation-grant
context. Routine arguments cannot replace trusted tenant authority.

## Diagnostics and introspection

Compilation errors identify the package/member, source location, expected and
actual signature, missing dependency, visibility violation, duplicate, or
unsupported state. Runtime stacks retain package and member frames.

Expose supported catalog views equivalent to:

```sql
SELECT * FROM information_schema.packages;
SELECT * FROM information_schema.package_members;
SELECT * FROM information_schema.package_dependencies;
DESCRIBE PACKAGE billing;
```

Introspection includes schema, name, owner, validity, specification/body hashes,
public member count, dependency count, and timestamps.

## Required certification

Tests cover:

- specification/body parsing with `AS`, `IS`, and optional `END` name;
- clear malformed-input diagnostics;
- missing/mismatched/duplicate implementations;
- private helper resolution and external rejection;
- mutable state and overloading rejection;
- public function, procedure, constant, nested helper, and package calls;
- transaction rollback and row-security enforcement;
- atomic body replacement and failed-replacement preservation;
- contract-compatible and incompatible specification changes;
- atomic schema-bundle publication;
- restart, stable hashes, reloadable compiled bodies, and safe failed-generation cleanup;
- execution privilege, private-member, trusted-context, rights-mode, and audit behavior.

## Explicit initial deferrals

- mutable session package variables;
- initialization blocks;
- overloaded members;
- remote package calls;
- autonomous transactions;
- vendor-specific pragmas; and
- edition-specific package state.

## Acceptance program

```sql
CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    salary DECIMAL NOT NULL
);

CREATE OR REPLACE PACKAGE emp_management AS
    c_tax_rate CONSTANT DECIMAL := 0.15;
    FUNCTION get_annual_salary(p_emp_id IN INTEGER) RETURN DECIMAL;
    PROCEDURE update_salary(p_emp_id IN INTEGER, p_new_sal IN DECIMAL);
END emp_management;

CREATE OR REPLACE PACKAGE BODY emp_management AS
    FUNCTION lookup_salary(p_emp_id IN INTEGER) RETURN DECIMAL AS
        v_salary DECIMAL;
    BEGIN
        SELECT salary INTO v_salary
        FROM employees WHERE employee_id = p_emp_id;
        RETURN v_salary;
    END;

    FUNCTION get_annual_salary(p_emp_id IN INTEGER) RETURN DECIMAL AS
    BEGIN
        RETURN lookup_salary(p_emp_id) * 12;
    END;

    PROCEDURE update_salary(p_emp_id IN INTEGER, p_new_sal IN DECIMAL) AS
    BEGIN
        UPDATE employees SET salary = p_new_sal WHERE employee_id = p_emp_id;
    END;
END emp_management;
```

Public function, procedure, and constant calls must work. An external call to
`emp_management.lookup_salary` must fail because it is private. Replacing only
the body must preserve the public contract and become visible atomically after
successful validation.
