# PostgreSQL Nightmare Gauntlet

The `pg18-nightmare` suite is documented in
[`../NIGHTMARE_GAUNTLET.md`](../NIGHTMARE_GAUNTLET.md). Use:

```bash
./scripts/pg18-nightmare.sh
cargo run -p bicdb-cli --features bench -- compat nightmare ./tmp/pg18-nightmare
```

It reuses the existing PostgreSQL 18.4 Docker oracle, writes JSON and Markdown
reports, and writes failing plus minimized SQL reproductions under the selected
report directory.
