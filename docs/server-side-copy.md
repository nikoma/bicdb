# Server-side COPY (files and programs)

`COPY table FROM 'file'`, `COPY table|(query) TO 'file'` and
`COPY ... FROM/TO PROGRAM 'command'` read and write files on the database
server, or run shell commands there. BicDB runs as a shared cloud service,
inside desktop applications and embedded in other programs, where the SQL
superuser is often not the person who owns the host. So server-side COPY is
**off by default** and needs both an operator setting and a SQL privilege.

COPY FROM STDIN and COPY TO STDOUT (and psql's `\copy`) are not affected by any
of this: anyone with the table privileges can use them.

## Operator settings (server start only)

| `bicdb serve` flag | Effect |
| --- | --- |
| `--copy-server-files-dir DIR` (repeatable) | Allow COPY to read and write regular files below `DIR`. Without any, `COPY ... FROM/TO 'file'` is refused. |
| `--allow-copy-program` | Allow `COPY ... FROM/TO PROGRAM`. Independent of the file setting: allowing files never allows programs, and the other way round. |

Every `DIR` must be an existing absolute directory. It is resolved once at
startup (symbolic links in it are resolved then). A statement may name the
directory by its resolved path or as it was given on the command line.

These are not SQL settings. `SET`, `ALTER SYSTEM`, `ALTER ROLE ... SET` and
routines cannot see or change them, and neither can a superuser. PostgreSQL
puts the equivalent decision (which roles may touch server files) in the
hands of whoever holds superuser, because there the superuser and the host
administrator are the same person. In BicDB they often are not, so the outer
limit sits with the process that starts the server, like PostgreSQL's
postmaster-only (server-start) parameters.

When a setting is off, COPY fails with SQLSTATE `0A000`
(`COPY to or from a file is not allowed in this environment`, or `... an
external program ...`). PostgreSQL uses the same code and wording when its
operator turns a statement off (`ALTER SYSTEM is not allowed in this
environment`). The choice is deliberate: no SQL privilege can change the
answer, so `42501` (insufficient privilege) would point users at a GRANT that
cannot help.

## SQL privileges (as in PostgreSQL)

The effective role (after `SET ROLE`) must have the privileges of:

- `pg_read_server_files` for COPY FROM a file;
- `pg_write_server_files` for COPY TO a file;
- `pg_execute_server_program` for COPY FROM/TO PROGRAM.

A superuser has all three. Otherwise COPY fails with PostgreSQL's error,
for example `42501 permission denied to COPY from a file` with the DETAIL
`Only roles with privileges of the "pg_read_server_files" role may COPY from a
file.` The role check comes first, so a role PostgreSQL would refuse gets
PostgreSQL's error whatever the server allows.

## What the file confinement guarantees

- The path must be absolute (COPY TO a relative path fails with PostgreSQL's
  `42602 relative path not allowed for COPY to file`; BicDB also refuses a
  relative COPY FROM path, which PostgreSQL would resolve against its data
  directory).
- A path containing `..` is refused, even one that would land back inside.
- The path must lie below an allowed directory, compared by path components.
- Below the allowed directory, every component is opened with
  `openat(..., O_NOFOLLOW)` relative to the previous directory descriptor. No
  symbolic link below the allowed directory is ever followed, including one
  created or swapped in between statements or during the open itself. The
  allowed directory itself is opened with `O_NOFOLLOW` too.
- The file must be a regular file (a directory gives PostgreSQL's `42809 "..."
  is a directory`; a FIFO, socket or device is refused), and it must have a
  single hard link, since a second link could name a file outside the
  directory.
- COPY TO creates a missing file with mode 0644 (less the server's umask) and
  truncates an existing one only after these checks.

Refusals by these rules fail with `42501` and the message
`could not open file "..." for reading|writing: <reason>`. Missing files and
OS errors use PostgreSQL's messages and codes (`58P01` no such file, `42501`
permission denied, ...).

Not covered: the allowed directory's ancestors and the directory itself are
the operator's to protect (BicDB resolves them once at startup). Any OS user
or process that can write into an allowed directory can place files there for
COPY FROM to read.

## COPY PROGRAM

- The command runs as `/bin/sh -c '<command>'`, like PostgreSQL, as the
  server's OS user, in its own process group, with `/` as working directory.
- Its environment is empty apart from `PATH=/usr/local/bin:/usr/bin:/bin`, so
  credentials and keys in the server's environment are not inherited.
- COPY FROM PROGRAM reads the command's standard output; COPY TO PROGRAM writes
  to its standard input. Its standard error goes to the server's standard error.
- Cancelling the query (or reaching the server query timeout) kills
  the whole process group; the child is always reaped.
- A non-zero exit fails the statement with PostgreSQL's error:
  `38000 program "..." failed` and a DETAIL such as `child process exited with
  exit code 3`, `command not found` or `child process was terminated by signal
  9: Killed`. Rows read before the failure are not kept.

## Where server-side COPY is available

Only as a top-level statement of a client session over the PostgreSQL protocol
(simple and extended query). COPY inside a function, procedure, trigger, `DO`
block or script runs in the SQL engine, which has no file or program endpoint,
so it is refused there whatever the settings; a routine (including a
`SECURITY DEFINER` one) cannot be used to reach the server's files. The
embedded SQL API has no file endpoint either.

## Audit

Each server-side COPY that passes the gates writes one line to the server's
standard error before it runs:
`bicdb: server-side COPY FROM|TO file|program "<path or command>" by role
"<role>" (session user "<user>", connection N)`.
