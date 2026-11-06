# Private iDempiere runtime installation

A staged Oracle SQL provider uses a new bundle version and PostgreSQL transport.
When qualifying that provider, give the Equinox launcher both a private
installation directory and a private configuration directory. Overriding only
`-configuration` retained the product's original provider alongside the staged
one in the pinned reference build. Setting `osgi.configuration.cascaded=false`
did not remove it. Explicitly overriding `-install` removed the original provider
from the initialization probe's retained bundle inventory.

After preparing the private `config.ini`, obtain the launcher arguments:

```sh
python examples/oracle/idempiere/runtime_installation.py \
  --product /external/built-product \
  --configuration /private/runtime/configuration \
  --installation /private/runtime/installation
```

The command prints a JSON argument array. Add those arguments to the Java
launcher invocation after its launcher JAR, alongside the intended application
and workspace options. It creates the empty installation directory. It does not
modify the shared product or configuration, start a JVM, or configure the
database. Existing nonempty installations, overlapping directories, and paths
resolving inside the shared product are refused, including through symlinks.
Use a fresh private installation for a new qualification attempt.

The prepared configuration must use absolute file locations for the framework,
framework extensions, simple configurator and chosen bundles. Keep its bundle
list, p2 data area and workspace private as well. The helper checks directory
placement and the presence of `config.ini`; it does not parse or validate all
Equinox configuration properties. Keep application resources such as
`IDEMPIERE_HOME` and `jetty.home` pointed at the intended product resources, with
private writable application state and `jetty.base`. Preserve the explicit
Oracle preview profile and PostgreSQL transport selection.

Retain JVM class-load evidence that `DB_Oracle` and the adapter came from the
expected staged artifacts. Check the full application startup log and selected
bundle inventory; a successful initializer does not establish that every service
starts or that business workflows pass. The [bounded initialization record](evidence/idempiere-runtime-installation-2026-11-05/README.md)
distinguishes those gates. The accepted application runtime has not been changed
by these initialization experiments.


## Private application-home resources

The product's OSGi bundles and application-home resources are separate inputs.
Some original workflows resolve files under `IDEMPIERE_HOME`, including the
server-feature `data/import` directory. Prepare a new private home from the
pinned original checkout before running those workflows:

```bash
python3 examples/oracle/idempiere/prepare_runtime_resources.py \
  --upstream /path/to/idempiere \
  --upstream-commit da0730aec42f3e9f5f671e81a70ff71e24d5e534 \
  --home /tmp/idempiere-private-home \
  --report /tmp/idempiere-private-resources.json
```

Use `-DIDEMPIERE_HOME=/tmp/idempiere-private-home` for the test JVM while
retaining the prepared OSGi installation, configuration and bundle locations.
The home must not exist. The report must be new and outside the checkout and
home. The tool checks tracked resource content, copies it exactly, rejects
symlinks/special files and verifies source/target hashes. It excludes generated
untracked seed archives, so it does not initialize or replace a database. Review
the [qualification scope](evidence/idempiere-private-runtime-resources-2026-11-06/README.md)
separately from original application acceptance.
