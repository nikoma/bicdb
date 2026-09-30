# Native scripting and mail queue integration

The repository owner authorized implementation, validation, merging to main,
and pushing to GitHub. Work proceeds in this order. Checked items require
executable evidence; protocol support does not imply all Redis functionality.

- [x] Shared bounded Lua 5.1 runtime, sandbox, JSON, host callbacks.
- [x] Native database Lua API using ordinary transactions and access checks.
- [x] Durable Redis hashes, sorted sets, streams, server time and script cache.
- [x] Atomic Lua EVAL/EVALSHA with KEYS/ARGV, call/pcall and restart tests.
- [x] Run actual mail queue and send policy contract tests against BicDB.
- [x] Integrate BicDB durability checks and deployment into the mail platform.
- [x] Merge and push verified Lua/queue work in both repositories (BicDB
      `ac25db0`, mail `0c4f416`; six-service container/restart smoke passed).
- [x] OXC TypeScript compiler, bounded QuickJS VM, and shared Lua/JS host methods.
- [x] Add TypeScript compilation to stored versioned JavaScript and a bounded
      JavaScript runtime sharing the Lua workflow host and authorization model.
- [x] Durable jobs triggered after booking commits; pinned active script version.
- [ ] Trusted tenant/permission context, parameterized SQL and atomic transactions.
- [ ] External HTTP outside database transactions, destination allowlists,
      scoped secret access, deadlines, response limits and durable retries.
- [ ] EHR eligibility scenario: unique operation keys, concurrent retries,
      revision/stale-result protection, five-table atomic update and outbox.
- [x] Activate revised workflow while running; verify old/new job version behavior.
- [ ] Merge and push verified TypeScript/workflow work; resume mail enterprise
      priorities in importance order through the full readiness backlog.

The EHR example is a requested acceptance scenario, not existing syntax. HTTP
and secret access must not be added to transactional RESP queue scripts.
