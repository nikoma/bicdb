# Team Board: Rails 8.1.4 + BicDB

A runnable team issue tracker using the **standard Active Record PostgreSQL
adapter**. There is no custom adapter, monkey patch, SQL-query rewrite in the
application, PostgreSQL server, Redis server, Elasticsearch service, or external
message broker. The `pg` gem supplies the PostgreSQL client protocol.

The example uses Rails **8.1.4**, the latest stable release checked against the
[RubyGems registry](https://rubygems.org/gems/rails/versions/8.1.4) on 2026-11-01,
and Ruby **3.4.12** in Docker. Gem versions are locked in `Gemfile.lock`.

## Run with one command

From the repository root, with Docker Compose installed:

```sh
docker compose -f examples/rails/team_board/compose.yml up --build
```

Open **http://localhost:3000** and sign in with:

- Email: `demo@example.test`
- Password: `demo-password-please-change`

The first build compiles BicDB from this checkout and installs the locked Rails
dependencies; allow substantial time for the initial Rust build. Subsequent
builds reuse compiler and dependency caches. The launcher creates the databases,
applies the schemas, seeds the application, and starts Rails and Solid Queue.
`Ctrl-C` stops the demo. Database and attachment volumes persist across starts.
Use `TEAM_BOARD_PORT=3001` before the command to select another web port.

BicDB is built from **this source checkout** so the demo includes its Rails
compatibility fixes. An older downloadable BicDB release may predate those fixes.
This is a local development demo with public sample credentials; the Compose
configuration publishes only the web port, bound to the host's loopback interface.

## What to try

1. Open a project board and search for `accessibility` or `backup`.
2. Create an issue with labels, an assignee, a due date, and an attachment.
3. Add comments and move the issue between backlog, active, review, and done.
4. Generate a report. A Solid Queue worker aggregates the issues and comments,
   stores the result through Solid Cache, and sends a Turbo update through
   Solid Cable. The report appears without refreshing the page.
5. Restart the demo and confirm that issues, attachments, and pending jobs remain.

| Rails feature | Example coverage |
| --- | --- |
| Active Record migrations | References, foreign keys, unique/check constraints, GIN index, migration rollback/reapply |
| Associations | Workspaces, memberships, projects, issues, labels, comments, polymorphic activity |
| PostgreSQL types | JSONB, text arrays, timestamps, dates and binary values |
| Transactions | Multi-table rollback, savepoints and optimistic locking |
| Search | PostgreSQL full-text expressions and a GIN index |
| Active Job / Solid Queue | Durable reports, concurrent worker processes, enqueue after commit |
| Solid Cache | Serialized report payloads and expiry |
| Action Cable / Solid Cable | Turbo updates backed by durable database messages |
| Active Storage | Database metadata and local attachment files |
| Authentication | Password digests, sessions, CSRF protection and workspace-scoped access |

Queued reports also recheck persisted status while waiting for their live update,
so a job that finishes before the browser subscribes cannot leave the page stuck
showing “queued.” This uses a normal Rails endpoint and Stimulus controller.

The domain models and controllers live in `app/`, schema changes in `db/migrate/`,
and the complete database configuration is in [`config/database.yml`](config/database.yml).
All four logical databases—primary, queue, cache, and cable—run in one BicDB
process. Attachment bytes live on the local filesystem, as configured by Active
Storage's disk service.

## Verify it

Run the full acceptance campaign against the built image:

```sh
docker compose -f examples/rails/team_board/compose.yml run --rm web bin/verify
```

The verification script starts a separate, fresh BicDB cluster and:

- Runs `db:prepare` twice, reverses all application migrations, and reapplies them.
- Runs the standard `bin/rails test` suite with two parallel test workers.
- Checks relational integrity, array/JSONB values, full-text changes, optimistic
  locking, savepoints, rollbacks, attachments, login, forms and workspace isolation.
- Confirms that a rolled-back transaction does not enqueue its deferred job.
- Queues 24 reports, kills BicDB with SIGKILL before starting workers, reopens it,
  and verifies that pending jobs and a binary cache value survived.
- Starts two real Solid Queue worker processes and requires both to make progress
  with overlapping execution. Each report must complete once in this campaign,
  with correct aggregates, cache values and persisted Cable broadcasts.
- Uses real HTTP requests for CSRF-protected login, search, assets and issue
  creation, then receives a worker's Turbo update over a real WebSocket.

It fails on any missing invariant and stops every process it owns. Logs and a
machine-readable `report.json` are written to the printed evidence directory.
A `--work-dir PATH` argument selects a new directory for retained evidence;
container-local evidence disappears when an ephemeral `--rm` container exits,
so mount a host directory when retaining a Docker run's full logs.
The [Rails CI workflow](../../../.github/workflows/rails.yml) runs the same script.

The application uses after-commit enqueueing with separate primary and queue
databases, following normal Rails configuration. It does not assume an atomic
commit across those databases or promise exactly-once execution across arbitrary
worker crashes. The pending-job restart test kills BicDB before workers claim
those jobs.

## Native development

With Ruby 3.2 or newer, Bundler 2.6+, and the native headers needed by the gems:

```sh
cd examples/rails/team_board
bin/dev
```

The launcher installs gems and builds the current BicDB source with the repository's
Rust toolchain. To use an executable already built from this checkout:

```sh
BICDB_BIN=/absolute/path/to/bicdb bin/dev
```

The native launcher chooses an available database port and prints it. In another
terminal, set `BICDB_PORT` to that value before using `bin/rails console` or other
Rails commands. Set `PORT` to change the web port. The managed launchers remove
inherited Rails database-URL overrides so setup and rollback cannot be redirected
to an unrelated database.

Run native qualification with `BICDB_BIN=/absolute/path/to/bicdb bin/verify`.
The sample application is [Apache-2.0 licensed](../../../LICENSES/Apache-2.0.txt);
BicDB retains its separate [source-available license](../../../LICENSE).
The demo qualifies its recorded workload; it is not a certification of every
Rails application or a production deployment configuration.
