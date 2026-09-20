# The ambient-authority bug family

> **Missing security information must never be read as authority.**

Four findings in this codebase (H-14 through H-17) turned out to be one bug,
written four times. Each is a value that carries security meaning, where the
*absence* of the value was interpreted as the *strongest* possible answer.

This document exists so the fifth one is found by pattern rather than by luck.

## The four confirmed instances

| | The value | Absent meant | Fixed in |
|---|---|---|---|
| H-14 | `Option<&SecurityContext>` on the broker gate | trusted embedded host | #637 |
| H-15 | `AuthenticationStrength` | `Internal` — the strongest variant | #638, #639 |
| H-16 | `ViewSchema::owner` | bootstrap-role definer | #640 |
| H-17 | `RoutineSchema::owner` | bootstrap-role definer | #641 |

Each was verified with a working exploit before it was fixed, and each fix
carries a regression test that fails against the pre-fix source.

## Why it keeps happening

The defaults are not careless. Every one of them is *correct in the place it
was written*, and wrong somewhere else that reads the same value.

`ViewSchema::owner` is the clearest case. A view persisted before ownership
tracking records no owner, and the codebase documents the convention: such a
view is "treated as owned by the bootstrap role". Read by an **ownership**
check, that is fail-closed — only a superuser may alter or drop it. Read by the
**definer-execution** path, the identical default is fail-open — the view body
runs with superuser authority over every underlying table.

Same field. Same default. Opposite safety, depending on which side of the
system is asking.

## Three rules

**1. Absence of identity must never grant more than an unprivileged identity.**

If an unidentified caller can do something an identified-but-unprivileged
caller cannot, the ladder is inverted. That inversion is the signature of this
family. H-14 was found exactly this way:

```
identified caller, wrong role   ->  denied
unidentified caller             ->  published, and consumed
```

**2. Provenance and authorization are different dimensions. Model both.**

Do not infer "where did this call come from" from "what is this principal
allowed to do". `Option<Identity>` cannot answer both questions: when `None`
means *both* "no authenticated identity" *and* "trusted in-process host", the
network path inherits the host's trust. Name the provenance instead:

```rust
enum BrokerCaller<'a> {
    TrustedHost,                                               // stated, never inferred
    Sql { context: Option<&'a SecurityContext>, superuser: bool },
}
```

A superuser check is a fine *authorization* fact about a known identity. It is
not a substitute for knowing where the request arrived from.

**3. Execution provenance is not authentication strength.**

A user's request does not become internally authenticated because an in-process
worker is what replays it. Running inside the server is not the same as being
authenticated as the server. Where the originating strength is known, carry it;
where it is not, use the weakest value and say why — never upgrade by default.

## The heuristic that finds them

> When one default is read by **both** an ownership/authorization check **and**
> an execution-identity path, it is almost certainly fail-closed in one and
> fail-open in the other. Check both readers.

H-17 was found by applying this to H-16 within minutes: views and routines both
have owners, and both execute as them.

## The technique that finds the rest of them

When a value is being read two ways, **change its type so every reader must
declare which meaning it wants.** This converts a search problem into a compile
error.

`RoutineSchema::owner` was a bare `String` defaulting to the bootstrap role.
Reading the code identified two SECURITY DEFINER execution sites. Making the
field an `Option<String>` with two named accessors —

```rust
fn owner(&self) -> &str;                  // bootstrap default: ownership, display
fn definer_owner(&self) -> Option<&str>;  // Option: execution identity
```

— surfaced a **third** site the reading had missed. Patching only the two known
call sites would have shipped a fix that looked complete and was not.

## Choosing the fallback

Fail-closed does not have to mean fail-hard. For both owner findings, an
unrecorded owner falls back to **invoker semantics** rather than to a refusal:

- it can never grant more than the caller already has, so the escalation is
  fully closed, and
- a legacy object keeps working for callers who could reach the sources
  themselves, so an upgrade does not break every view and routine at once.

Prefer the weakest *working* identity over an error, where one exists.

## Hunt list

Grep for these shapes across auth, application runtime, SQL, broker, sync, event replay,
background jobs, extension execution, and restore/maintenance paths:

```
None            => trusted
default()       => privileged
unwrap_or(Internal)
unwrap_or(true)
missing tenant  => global
missing actor   => system
missing role    => admin/bootstrap
```

### Known live instance, currently safe by construction

`session_user_from_gucs` falls back to `current_role_name()`, which is
`BOOTSTRAP_ROLE_NAME` — a superuser. It is safe today only because pgwire seeds
both identity GUCs eagerly *and* `discard_all` restores them: a two-part
mechanism where either half alone is a P0.

Any new code path that builds a session with empty GUCs inherits superuser.
This has bitten once already (`ts_rewrite`, #599) and came close a second time
during the H-14 fix, where an early draft made every application session a broker
superuser for exactly this reason.

## Writing the tests

Every fix in this family should carry, at minimum:

1. the exploit, asserted refused, **verified to fail against the pre-fix
   source** — otherwise it is an assertion, not a regression test;
2. a guard that the legitimate case still works (an owned view still executes
   as its owner; a granted role can still publish);
3. a guard that the fail-closed sibling reading did not loosen (an ownerless
   view is still superuser-only to drop).

Where a gate is hard to reach from a test, **mutate it to allow everything and
confirm the suite fails.** A gate no test can break is a gate no test is
checking.
