# Explicit launcher installation diagnostic

The [validation record](validation-results.json) compares three initialization
modes against the same product, staged provider and prepared configuration:

| Mode | Original provider retained | Staged provider retained |
| --- | --- | --- |
| Private configuration, shared default installation | Yes | Yes |
| Same, with configuration cascading disabled | Yes | Yes |
| Private configuration and explicit private installation | No | Yes |

Each actual Equinox launcher ran with `-initialize`, `-clean`, a 256 MiB maximum
heap and start level 1. No application was run. Afterwards, a direct
`EclipseStarter.startup` inspection reopened each retained framework cache at
level 1, without `-clean`, and enumerated provider bundles by symbolic name.
All six JVM runs exited successfully. The public `runtime_installation.py`
helper supplied the explicit private installation arguments for the third mode.

An earlier direct-starter experiment on fresh configurations loaded only the
staged provider in both default and noncascading modes. That narrower experiment
did not reproduce the actual launcher's behavior. The actual-launcher comparison
and retained bundle enumeration establish the relevant difference; this record
does not claim a particular internal launcher method caused it.

Five helper regression tests passed, covering directory placement, existing
installation preservation, symlink resolution, missing inputs and unchanged
product/configuration contents. The probes ran in separate network namespaces
with memory guards and no database server. Full business tests, class loading
at application start levels and clean provider packaging during those tests
remain unqualified. No provider warning was filtered or suppressed, and no
engine change or accepted runtime upgrade was made.

The separate nine-test customer-invoice attempt was stopped by its memory guard
before JUnit produced a completed result. It is an interrupted attempt, not a
passing class or a SQL test failure. Its disposable database was removed only
after verifying the test processes and open database file handles were gone;
568,130,956 bytes were reclaimed, retaining logs and configuration. Retry it on
the completed foreign-key schema with the explicit private installation and
sufficient memory headroom.
