#!/usr/bin/env python3
"""Run PostgreSQL 18.4's own regression suite (src/test/regress) against a server.

This reproduces what `pg_regress` does for `make installcheck` with the schedule
`parallel_schedule`, except that every test runs serially, in schedule order:

* every test file runs as `psql -X -a -q -d regression < sql/<test>.sql > results/<test>.out 2>&1`
  using psql 18.4 (from the stock `postgres:18.4` image, so output formatting is that of the
  release whose expected files we compare against);
* the environment is the one pg_regress sets: PGTZ, PGDATESTYLE, LC_MESSAGES=C, PGOPTIONS
  `-c intervalstyle=postgres_verbose`, PG_ABS_SRCDIR / PG_ABS_BUILDDIR / PG_LIBDIR / PG_DLSUFFIX
  (read by the tests through `\\getenv`);
* the database `regression` is created fresh with the same CREATE/ALTER DATABASE commands;
* results are compared with expected/<test>.out and every alternative expected/<test>_N.out
  (a test passes when any of them matches exactly); the diff against the closest one is kept.

Outputs (in --out): results/<test>.out, diffs/<test>.diff, status.tsv, summary.json, summary.md.

Targets:
  --target ref      start a stock PostgreSQL 18.4 container (pgregress-ref, 127.0.0.1:57434, 1G)
  --target bicdb    start `<bin> serve --port <p> <fresh dir>` under systemd-run (MemoryMax, Nice)
  --target conn     use an already running server (--host/--port/--user/--admin-db)

Adaptations applied only for BicDB are listed in ADAPTATIONS below and written to summary.json.
Expected output is never edited.
"""
import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.environ.get("PG18_REGRESS_CACHE", "/root/pg18-regress-cache")
PG_VERSION = "18.4"
IMAGE = "postgres:18.4"
SRC_URL = "https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2"
CLIENT = "pgregress-client"

# Adaptations the harness makes when the target is BicDB. Each one is reported.
ADAPTATIONS = {
    "superuser": "tests run as BicDB's bootstrap superuser `bicdb` (PostgreSQL reference: `postgres`).",
    "client_side_copy": (
        "single-line server-side `COPY ... FROM/TO :'var'` statements (fixture loads in test_setup and "
        "elsewhere) are sent as psql `\\copy` with the variable resolved, because BicDB's COPY reads "
        "only STDIN / writes only STDOUT; the echoed line in the result file is restored to the "
        "original statement, so expected output is compared unchanged. Server-side COPY file I/O "
        "itself is reported as its own work-queue cluster."),
}

SET_RE = re.compile(r"^\\set\s+(\w+)\s+(.*)$")
COPY_FILE_RE = re.compile(r"^(COPY|copy)\s+(.*?)\s+(FROM|from|TO|to)\s+:'(\w+)'(.*?);\s*$")


def resolve_psql_value(expr, env, vars_):
    """Value of a simple `\\set` right-hand side: concatenation of :var and 'literal' pieces."""
    out = []
    for m in re.finditer(r":(\w+)|'((?:[^']|'')*)'|(\S+)", expr):
        if m.group(1):
            if m.group(1) not in vars_:
                return None
            out.append(vars_[m.group(1)])
        elif m.group(2) is not None:
            out.append(m.group(2).replace("''", "'"))
        else:
            out.append(m.group(3))
    return "".join(out)


def adapt_client_side_copy(sql_text, env):
    """Rewrite single-line server-side file COPY into psql \\copy. Returns (text, [(adapted, original)])."""
    vars_ = {"abs_srcdir": env["PG_ABS_SRCDIR"], "abs_builddir": env["PG_ABS_BUILDDIR"],
             "libdir": env["PG_LIBDIR"], "dlsuffix": env["PG_DLSUFFIX"]}
    lines = sql_text.split("\n")
    pairs = []
    for i, line in enumerate(lines):
        m = SET_RE.match(line)
        if m:
            v = resolve_psql_value(m.group(2), env, vars_)
            if v is not None:
                vars_[m.group(1)] = v
            else:
                vars_.pop(m.group(1), None)
            continue
        m = COPY_FILE_RE.match(line)
        if m and m.group(4) in vars_ and "'" not in vars_[m.group(4)]:
            new = f"\\copy {m.group(2)} {m.group(3)} '{vars_[m.group(4)]}'{m.group(5)}"
            lines[i] = new
            pairs.append((new, line))
    return "\n".join(lines), pairs


def restore_echo(result_path, pairs):
    if not pairs:
        return
    with open(result_path, "rb") as f:
        lines = f.read().decode("utf-8", errors="surrogateescape").split("\n")
    queue = list(pairs)
    for i, line in enumerate(lines):
        if queue and line == queue[0][0]:
            lines[i] = queue.pop(0)[1]
    with open(result_path, "wb") as f:
        f.write("\n".join(lines).encode("utf-8", errors="surrogateescape"))


def sh(cmd, check=True, **kw):
    r = subprocess.run(cmd, shell=isinstance(cmd, str), text=True, capture_output=True, **kw)
    if check and r.returncode != 0:
        raise SystemExit(f"command failed ({r.returncode}): {cmd}\n{r.stdout}\n{r.stderr}")
    return r


def ensure_source():
    regress = os.path.join(CACHE, "src", f"postgresql-{PG_VERSION}", "src", "test", "regress")
    if not os.path.isfile(os.path.join(regress, "parallel_schedule")):
        os.makedirs(os.path.join(CACHE, "src"), exist_ok=True)
        tarball = os.path.join(CACHE, f"postgresql-{PG_VERSION}.tar.bz2")
        if not os.path.isfile(tarball):
            sh(["curl", "-sfL", "-o", tarball + ".part", SRC_URL])
            os.rename(tarball + ".part", tarball)
        sh(["tar", "-xjf", tarball, "-C", os.path.join(CACHE, "src"),
            f"postgresql-{PG_VERSION}/src/test/regress"])
    return regress


def ensure_regress_so(regress):
    """Compile regress.c (the suite's C helper functions) against the 18.4 server headers."""
    lib = os.path.join(CACHE, "lib")
    so = os.path.join(lib, "regress.so")
    if os.path.isfile(so):
        return lib
    os.makedirs(lib, exist_ok=True)
    script = (
        "set -e; apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq ca-certificates gcc make >/dev/null 2>&1; "
        "echo 'deb [ signed-by=/usr/local/share/keyrings/postgres.gpg.asc ] "
        "https://apt-archive.postgresql.org/pub/repos/apt trixie-pgdg-archive main' > /etc/apt/sources.list.d/pgdg.list; "
        "apt-get update -qq >/dev/null 2>&1; "
        "apt-get install -y -qq postgresql-server-dev-18=$(dpkg-query -W -f='${Version}' postgresql-18) >/dev/null 2>&1; "
        "mkdir /b; cp /src/regress.c /b/; cd /b; "
        "printf 'MODULES = regress\\nPG_CONFIG = pg_config\\nPGXS := $(shell $(PG_CONFIG) --pgxs)\\ninclude $(PGXS)\\n' > Makefile; "
        "make >/dev/null 2>&1; cp regress.so /out/"
    )
    r = sh(["docker", "run", "--rm", "--memory", "2g", "--cpus", "2", "-v", f"{regress}:/src:ro",
            "-v", f"{lib}:/out", IMAGE, "sh", "-c", script], check=False)
    if not os.path.isfile(so):
        print("WARNING: could not build regress.so; tests that load it will fail", r.stderr[-500:])
    return lib


def parse_schedule(path):
    groups = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("test:"):
                groups.append(line[5:].split())
    return groups


class Target:
    def __init__(self, args, builddir, regress, libdir):
        self.args = args
        self.builddir = builddir
        self.regress = regress
        self.libdir = libdir
        self.kind = args.target
        self.host = args.host
        self.port = args.port
        self.user = args.user
        self.admin_db = args.admin_db
        self.unit = None
        self.datadir = None

    # -- lifecycle ---------------------------------------------------------
    def start(self):
        if self.kind == "ref":
            self.port = self.port or 57434
            self.user = self.user or "postgres"
            self.admin_db = self.admin_db or "postgres"
            sh(["docker", "rm", "-f", "pgregress-ref"], check=False)
            # Same absolute paths inside the container so server-side COPY and LOAD see the files.
            sh(["docker", "run", "-d", "--name", "pgregress-ref", "--memory", "1g", "--cpus", "2",
                "-e", "POSTGRES_PASSWORD=regress", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e", "POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8",
                "-v", f"{self.regress}:{self.regress}:ro", "-v", f"{self.builddir}:{self.builddir}",
                "-v", f"{self.libdir}:{self.libdir}:ro",
                "-p", f"127.0.0.1:{self.port}:5432", IMAGE,
                "-c", "max_prepared_transactions=2", "-c", "fsync=off"])
            # The bind mounts live under /root; let the server's `postgres` user traverse it.
            sh(["docker", "exec", "-u", "0", "pgregress-ref", "chmod", "o+x", "/root"])
        elif self.kind == "bicdb":
            self.port = self.port or 57455
            self.user = self.user or "bicdb"
            self.admin_db = self.admin_db or "bicdb"
            self.datadir = os.path.join(self.builddir, "bicdb-data")
            self.unit = f"pgregress-bicdb-{self.port}"
            sh(["systemctl", "stop", self.unit], check=False)
            shutil.rmtree(self.datadir, ignore_errors=True)
            self._start_bicdb()
        else:
            self.user = self.user or "postgres"
            self.admin_db = self.admin_db or "postgres"
        self.wait_ready(120)

    def _start_bicdb(self):
        sh(["systemctl", "reset-failed", self.unit], check=False)
        sh(["systemd-run", "--quiet", f"--unit={self.unit}", "--collect", "-p",
            f"MemoryMax={self.args.memory_max}", "-p", "Nice=15", "-p", "CPUWeight=20",
            self.args.bicdb_bin, "serve", "--port", str(self.port), self.datadir])

    def restart_after_crash(self):
        if self.kind == "bicdb":
            sh(["systemctl", "stop", self.unit], check=False)
            self._start_bicdb()
            return self.wait_ready(120)
        if self.kind == "ref":
            sh(["docker", "start", "pgregress-ref"], check=False)
            return self.wait_ready(120)
        return self.wait_ready(30)

    def stop(self):
        if self.kind == "ref" and not self.args.keep:
            sh(["docker", "rm", "-f", "pgregress-ref"], check=False)
        if self.kind == "bicdb":
            if self.unit:
                sh(["systemctl", "stop", self.unit], check=False)
            if not self.args.keep and self.datadir:
                shutil.rmtree(self.datadir, ignore_errors=True)

    def alive(self, db=None):
        r = self.psql_cmd(db or self.admin_db, "SELECT 1", timeout=20)
        return r.returncode == 0

    def wait_ready(self, secs):
        t0 = time.time()
        while time.time() - t0 < secs:
            if self.alive():
                return True
            if self.unit and time.time() - t0 > 5 and \
                    sh(["systemctl", "is-active", "--quiet", self.unit], check=False).returncode != 0:
                return False  # the server process exited (e.g. could not open its store)
            time.sleep(2)
        return False

    # -- client ------------------------------------------------------------
    def env(self, db):
        e = {
            "PGHOST": self.host, "PGPORT": str(self.port), "PGUSER": self.user, "PGDATABASE": db,
            "PGTZ": "America/Los_Angeles", "PGDATESTYLE": "Postgres, MDY", "LC_MESSAGES": "C",
            "LANG": "C", "PGOPTIONS": " -c intervalstyle=postgres_verbose",
            "PG_ABS_SRCDIR": self.regress, "PG_ABS_BUILDDIR": self.builddir,
            "PG_LIBDIR": self.libdir, "PG_DLSUFFIX": ".so", "PGAPPNAME": "pg_regress",
            "PGPASSWORD": "regress", "PGSSLMODE": "disable",
        }
        return e

    def docker_exec(self, db, argv, stdin=None, timeout=None, appname=None):
        cmd = ["docker", "exec", "-i", "-w", self.builddir]
        env = self.env(db)
        if appname:
            env["PGAPPNAME"] = appname
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        cmd += [CLIENT] + argv
        return subprocess.run(cmd, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              timeout=timeout)

    def psql_cmd(self, db, sql, timeout=60):
        try:
            r = self.docker_exec(db, ["psql", "-X", "-q", "-c", sql], timeout=timeout)
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess([], 124, b"timeout", b"")
        return r


def start_client(regress, builddir, libdir):
    sh(["docker", "rm", "-f", CLIENT], check=False)
    # Run psql as the image's `postgres` user (the reference server's uid) so files psql creates
    # (\o) can be overwritten by server-side COPY TO, as when pg_regress and the server share a user.
    sh(["docker", "run", "-d", "--name", CLIENT, "--network", "host", "--memory", "1g", "--cpus", "2",
        "--user", "postgres", "--entrypoint", "sleep", "-v", f"{regress}:{regress}:ro", "-v", f"{builddir}:{builddir}",
        "-v", f"{libdir}:{libdir}:ro", IMAGE, "infinity"])
    sh(["docker", "exec", "-u", "0", CLIENT, "chmod", "o+x", "/root"])


def create_database(t, log):
    """The statements pg_regress issues for installcheck (create_database in pg_regress.c)."""
    stmts = [
        'DROP DATABASE IF EXISTS "regression"',
        'CREATE DATABASE "regression" TEMPLATE=template0',
        'ALTER DATABASE "regression" SET lc_messages TO \'C\';'
        'ALTER DATABASE "regression" SET lc_monetary TO \'C\';'
        'ALTER DATABASE "regression" SET lc_numeric TO \'C\';'
        'ALTER DATABASE "regression" SET lc_time TO \'C\';'
        'ALTER DATABASE "regression" SET bytea_output TO \'hex\';'
        'ALTER DATABASE "regression" SET timezone_abbreviations TO \'Default\';',
    ]
    for s in stmts:
        r = t.psql_cmd(t.admin_db, s)
        out = r.stdout.decode(errors="replace").strip()
        log.append({"stmt": s, "rc": r.returncode, "out": out})
        print(f"  setup: {s[:70]!r} rc={r.returncode} {out[:200]}")
    if not t.alive("regression"):
        raise SystemExit("database regression is not reachable after setup")


def candidate_expected(regress, test):
    exp = os.path.join(regress, "expected")
    c = [os.path.join(exp, f"{test}.out")]
    for i in range(1, 10):
        p = os.path.join(exp, f"{test}_{i}.out")
        if os.path.isfile(p):
            c.append(p)
    return [p for p in c if os.path.isfile(p)]


def compare(regress, test, result, diffdir):
    cands = candidate_expected(regress, test)
    if not cands:
        return "no-expected", None, 0, 0
    best = None
    for p in cands:
        r = subprocess.run(["diff", "-q", p, result], capture_output=True)
        if r.returncode == 0:
            return "pass", os.path.basename(p), 0, 0
        d = subprocess.run(["diff", "-U3", p, result], capture_output=True, text=True,
                           errors="replace").stdout
        nlines = sum(1 for l in d.splitlines() if l[:1] in "+-" and not l.startswith(("+++", "---")))
        if best is None or nlines < best[1]:
            best = (p, nlines, d)
    p, nlines, d = best
    with open(os.path.join(diffdir, f"{test}.diff"), "w") as f:
        f.write(d)
    hunks = sum(1 for l in d.splitlines() if l.startswith("@@"))
    return "fail", os.path.basename(p), hunks, nlines


def run_test_file(t, args, regress, builddir, test, res):
    """Run one test file through psql exactly as pg_regress does. Returns (rc, note, pairs)."""
    sql = os.path.join(regress, "sql", f"{test}.sql")
    note = ""
    pairs = []
    if args.target == "bicdb" and not args.no_adapt:
        with open(sql, "rb") as f:
            text = f.read().decode("utf-8", errors="surrogateescape")
        text, pairs = adapt_client_side_copy(text, t.env("regression"))
        if pairs:
            os.makedirs(os.path.join(builddir, "adapted"), exist_ok=True)
            sql = os.path.join(builddir, "adapted", f"{test}.sql")
            with open(sql, "wb") as f:
                f.write(text.encode("utf-8", errors="surrogateescape"))
            note = f"adapted: {len(pairs)} COPY file statement(s) sent as \\copy"
    with open(sql, "rb") as fin, open(res, "wb") as fout:
        try:
            cmd = ["docker", "exec", "-i", "-w", builddir]
            env = t.env("regression")
            env["PGAPPNAME"] = f"pg_regress/{test}"
            for k, v in env.items():
                cmd += ["-e", f"{k}={v}"]
            # Merge stderr into stdout inside the container so messages keep their position
            # relative to the echoed statements (as `2>&1` in pg_regress).
            cmd += [CLIENT, "sh", "-c",
                    "exec psql -X -a -q -d regression -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on 2>&1"]
            rc = subprocess.run(cmd, stdin=fin, stdout=fout, stderr=subprocess.STDOUT,
                                timeout=args.timeout).returncode
        except subprocess.TimeoutExpired:
            rc = 124
            note = (note + "; " if note else "") + f"timeout after {args.timeout}s"
            sh(["docker", "exec", CLIENT, "pkill", "-f", "psql"], check=False)
    restore_echo(res, pairs)
    return rc, note, pairs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", choices=["ref", "bicdb", "conn"], required=True)
    ap.add_argument("--out", required=True, help="run directory (results, diffs, summary)")
    ap.add_argument("--bicdb-bin", default="/root/bicdb-handoff-prof-e/target/debug/bicdb")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int)
    ap.add_argument("--user")
    ap.add_argument("--admin-db")
    ap.add_argument("--memory-max", default="8G")
    ap.add_argument("--timeout", type=int, default=300, help="per-test timeout (seconds)")
    ap.add_argument("--only", help="comma-separated tests to run (still in schedule order; test_setup always first)")
    ap.add_argument("--keep", action="store_true", help="keep the server and its data after the run")
    ap.add_argument("--label", default="")
    ap.add_argument("--no-adapt", action="store_true", help="BicDB target: run the SQL files unchanged")
    args = ap.parse_args()
    global CLIENT
    # One psql container per run, so runs against different targets can proceed side by side.
    CLIENT = "pgregress-client-" + re.sub(r"[^a-zA-Z0-9_.-]", "-", args.label or args.target)

    regress = ensure_source()
    libdir = ensure_regress_so(regress)
    builddir = os.path.abspath(args.out)
    shutil.rmtree(builddir, ignore_errors=True)
    for d in ("results", "diffs"):
        os.makedirs(os.path.join(builddir, d))
    os.chmod(builddir, 0o777)
    os.chmod(os.path.join(builddir, "results"), 0o777)

    groups = parse_schedule(os.path.join(regress, "parallel_schedule"))
    only = set(args.only.split(",")) | {"test_setup"} if args.only else None
    with open(os.path.join(HERE, "scope.tsv")) as f:
        scope = {}
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            scope[parts[0]] = (parts[1], parts[2] if len(parts) > 2 else "")

    t = Target(args, builddir, regress, libdir)
    start_client(regress, builddir, libdir)
    t.start()
    setup_log = []
    create_database(t, setup_log)

    rows = []
    restarts = []
    started = time.time()
    try:
        for gi, group in enumerate(groups):
            for test in group:
                if only and test not in only:
                    continue
                res = os.path.join(builddir, "results", f"{test}.out")
                t0 = time.time()
                rc, note, pairs = run_test_file(t, args, regress, builddir, test, res)
                secs = time.time() - t0
                crashed = False
                hung = rc == 124 and args.target == "bicdb"
                if hung or not t.alive():
                    crashed = True
                    if hung:
                        # A statement that outlives psql keeps running (and holding locks) on the
                        # server and would make the following tests time out too: restart it.
                        note += "; server restarted to stop the runaway statement"
                        sh(["systemctl", "kill", "--kill-whom=main", "-s", "KILL", t.unit], check=False)
                        time.sleep(2)
                    else:
                        note = (note + "; " if note else "") + "SERVER CRASHED or stopped responding during this test"
                    if t.restart_after_crash():
                        note += "; restarted on the same store"
                    elif args.target == "bicdb":
                        # The store could not be reopened: keep it for evidence, continue on a fresh
                        # store with test_setup replayed so later tests still find their fixtures.
                        keep = f"{t.datadir}.unrecoverable-after-{test}"
                        sh(["systemctl", "stop", t.unit], check=False)
                        os.rename(t.datadir, keep)
                        t._start_bicdb()
                        t.wait_ready(120)
                        create_database(t, setup_log)
                        run_test_file(t, args, regress, builddir, "test_setup",
                                      os.path.join(builddir, f"test_setup.replay-after-{test}.out"))
                        note += f"; STORE COULD NOT BE REOPENED (kept at {keep}); continued on a fresh store with test_setup replayed"
                        restarts.append({"after": test, "unrecoverable_store": keep})
                    else:
                        note += " (restart FAILED)"
                status, matched, hunks, nlines = compare(regress, test, res, os.path.join(builddir, "diffs"))
                if rc == 124:
                    status = "timeout" if status != "pass" else status
                sc, why = scope.get(test, ("in", ""))
                rows.append({"test": test, "group": gi, "status": status, "matched": matched,
                             "hunks": hunks, "diff_lines": nlines, "seconds": round(secs, 2),
                             "psql_rc": rc, "crashed": crashed, "note": note, "scope": sc,
                             "scope_reason": why, "adapted_copy": len(pairs)})
                print(f"{test:28s} {status:8s} hunks={hunks:4d} lines={nlines:5d} {secs:6.1f}s {note}",
                      flush=True)
                if crashed and not t.alive():
                    raise SystemExit("server could not be restarted; stopping")
                if crashed:
                    restarts.append({"after": test})
    finally:
        total = time.time() - started
        setup_log.append({"restarts": restarts})
        write_summary(args, builddir, rows, setup_log, total, t)
        t.stop()
        sh(["docker", "rm", "-f", CLIENT], check=False)


def write_summary(args, builddir, rows, setup_log, total, t):
    with open(os.path.join(builddir, "status.tsv"), "w") as f:
        f.write("test\tstatus\tscope\thunks\tdiff_lines\tseconds\tmatched\tnote\n")
        for r in rows:
            f.write(f"{r['test']}\t{r['status']}\t{r['scope']}\t{r['hunks']}\t{r['diff_lines']}\t"
                    f"{r['seconds']}\t{r['matched'] or ''}\t{r['note']}\n")
    def count(sel):
        sub = [r for r in rows if sel(r)]
        return {"tests": len(sub), "pass": sum(r["status"] == "pass" for r in sub),
                "fail": sum(r["status"] != "pass" for r in sub),
                "hunks": sum(r["hunks"] for r in sub)}
    summary = {
        "pg_version": PG_VERSION, "target": args.target, "label": args.label,
        "date": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "bicdb_bin": args.bicdb_bin if args.target == "bicdb" else None,
        "adaptations": ADAPTATIONS if args.target == "bicdb" else {},
        "setup": setup_log, "seconds": round(total, 1),
        "all": count(lambda r: True),
        "in_scope": count(lambda r: r["scope"] == "in"),
        "out_of_scope": count(lambda r: r["scope"] != "in"),
        "tests": rows,
    }
    with open(os.path.join(builddir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    with open(os.path.join(builddir, "summary.md"), "w") as f:
        f.write(f"# PostgreSQL {PG_VERSION} regression suite against `{args.target}` {args.label}\n\n")
        for k in ("all", "in_scope", "out_of_scope"):
            c = summary[k]
            f.write(f"- {k}: {c['pass']}/{c['tests']} pass, {c['hunks']} failing diff hunks\n")
        f.write("\n| test | scope | status | hunks | diff lines | seconds | note |\n|---|---|---|---|---|---|---|\n")
        for r in rows:
            f.write(f"| {r['test']} | {r['scope']} | {r['status']} | {r['hunks']} | {r['diff_lines']} | "
                    f"{r['seconds']} | {r['note']} |\n")
    print(json.dumps({k: summary[k] for k in ("all", "in_scope", "out_of_scope")}))


if __name__ == "__main__":
    main()
