#!/usr/bin/env python3
"""Cluster the failures of a pg_regress_compat.py run by root cause.

Usage: analyze.py <run-dir> [--out clusters.json] [--md clusters.md] [--scope in|out|all]

Every failing test's result file is split into statement blocks (echoed SQL + the output that
follows it, using the test's .sql file to recognise echoed lines) and aligned with the expected
file the harness judged closest. Each block that differs is classified:

  bicdb-error    BicDB raised an ERROR where PostgreSQL did not (key: normalized BicDB message)
  accepts        PostgreSQL raised an ERROR, BicDB did not (key: normalized PostgreSQL message)
  error-cursor   same ERROR text, BicDB lacks the `LINE n:` / `^` position lines
  error-detail   same ERROR text, DETAIL/HINT/CONTEXT lines differ
  error-wording  both raised an ERROR with different text (key: normalized PostgreSQL message)
  notice         only NOTICE/WARNING/INFO lines differ
  row-order      same rows in a different order
  header         only the column-name header differs
  values         result rows differ (key: leading statement keywords)

Clusters are ranked by the number of differing statement blocks; failing diff hunks are attributed
to the cluster of the first differing block they cover.
"""
import argparse
import collections
import json
import os
import re
import sys

CACHE = os.environ.get("PG18_REGRESS_CACHE", "/root/pg18-regress-cache")
REGRESS = os.path.join(CACHE, "src", "postgresql-18.4", "src", "test", "regress")

MSG_LINE = re.compile(r"^(ERROR|WARNING|NOTICE|INFO|DETAIL|HINT|CONTEXT|LINE \d+|QUERY|psql:|FATAL|server closed)")


def read_lines(p):
    with open(p, "rb") as f:
        return f.read().decode("utf-8", errors="replace").split("\n")


def segment(src, out):
    """Map output to statements: every non-echo output line belongs to the last echoed source line
    before it (its "anchor"). Returns (anchor -> output lines, output line number -> anchor,
    source lines without blanks)."""
    src = [l for l in src if l.strip() != ""]
    by_anchor = collections.OrderedDict()
    line_anchor = {}
    p = 0
    anchor = -1
    for ln, line in enumerate(out, 1):
        if p < len(src) and line == src[p]:
            anchor = p
            p += 1
            line_anchor[ln] = anchor
            continue
        hit = None
        if len(line.strip()) > 2 and not line.startswith((" ", "ERROR:", "NOTICE:", "WARNING:", "DETAIL:",
                                                           "HINT:", "CONTEXT:", "LINE ", "INFO:", "(")):
            # resynchronise when psql did not echo some source lines (skipped \if branches,
            # COPY data consumed differently, ...)
            for k in range(p + 1, min(p + 400, len(src))):
                if line == src[k]:
                    hit = k
                    break
        if hit is not None:
            anchor = hit
            p = hit + 1
            line_anchor[ln] = anchor
            continue
        by_anchor.setdefault(anchor, []).append(line)
        line_anchor[ln] = anchor
    return by_anchor, line_anchor, src


def statement_text(src, anchors_sorted, anchor):
    """Source lines from just after the previous output-bearing anchor up to this anchor."""
    i = anchors_sorted.index(anchor)
    lo = anchors_sorted[i - 1] + 1 if i > 0 else 0
    lines = src[lo:anchor + 1]
    # keep the tail: the statement(s) right before the output
    return lines[-12:]


def norm(msg):
    m = msg.strip()
    m = re.sub(r"^(ERROR|WARNING|NOTICE|INFO):\s+", "", m)
    m = re.sub(r"\s+at Line: \d+, Column: \d+", "", m)
    m = re.sub(r"unsupported SQL statement ((?:\w+\s+){1,3}).*", lambda x: "unsupported SQL statement " + x.group(1).strip().upper(), m)
    m = re.sub(r"(sql parser error: Expected: [^,]*, found: \S+).*", r"\1", m)
    m = re.sub(r'"[^"]*"', '"…"', m)
    m = re.sub(r"'[^']*'", "'…'", m)
    m = re.sub(r"`[^`]*`", "`…`", m)
    m = re.sub(r"\b\d+(\.\d+)?\b", "N", m)
    m = re.sub(r"\bSET \w+ is not supported", "SET <guc> is not supported", m)
    return m[:160]


def first(lines, prefix):
    for l in lines:
        if l.startswith(prefix):
            return l
    return None


def strip_msgs(lines, kinds=("NOTICE", "WARNING", "INFO", "DETAIL", "HINT", "CONTEXT")):
    return [l for l in lines if not l.startswith(tuple(k + ":" for k in kinds))]


def lead(sql_lines):
    s = " ".join(l.strip() for l in sql_lines if not l.strip().startswith("--")).strip()
    s = re.sub(r"\s+", " ", s)
    words = re.findall(r"[A-Za-z_\\]+", s)
    if not words:
        return "?"
    w0 = words[0].upper()
    if w0 in ("CREATE", "ALTER", "DROP") and len(words) > 1:
        k = [w.upper() for w in words[1:4]]
        k = [w for w in k if w not in ("OR", "REPLACE", "TEMP", "TEMPORARY", "UNIQUE", "IF", "NOT", "EXISTS")]
        return f"{w0} {k[0] if k else ''}".strip()
    if w0.startswith("\\"):
        return "psql " + words[0]
    return w0


CASCADE_PATTERNS = [
    (r"current transaction is aborted", "transaction already aborted by an earlier error in the same block"),
    (r"collection not found: |relation \"[^\"]*\" does not exist|table \"[^\"]*\" does not exist|"
     r"type \"[^\"]*\" does not exist|function [^ ]+ does not exist",
     "object missing (usually created by an earlier statement that failed on BicDB)"),
]


def classify(sql, exp, act, test=""):
    last = next((l.strip() for l in reversed(sql) if l.strip() and not l.strip().startswith("--")), "")
    if re.match(r"^\\d", last):
        # psql describe commands run psql's own catalog queries: one root cause per command family
        cmd = re.match(r"^\\(d\w*\+?)", last).group(1)
        return "psql-describe", f"psql \\{cmd} (catalog queries / output) differs"
    ee = first(exp, "ERROR:")
    ae = first(act, "ERROR:")
    if any(l.startswith(("server closed", "connection to server was lost", "psql: error")) for l in act):
        return "crash", "server connection lost"
    if ae and not ee:
        for pat, key in CASCADE_PATTERNS:
            if re.search(pat, ae):
                return "cascade", key
        return "bicdb-error", norm(ae)
    if ee and not ae:
        return "accepts", norm(ee)
    if ee and ae:
        if ee != ae:
            return "error-wording", norm(ee)
        e2 = [l for l in exp if not re.match(r"^LINE \d+:|^\s*\^\s*$", l) and l.strip()]
        a2 = [l for l in act if not re.match(r"^LINE \d+:|^\s*\^\s*$", l) and l.strip()]
        if e2 == a2:
            return "error-cursor", "ERROR position (LINE n: / ^) not reported"
        if strip_msgs(exp) == strip_msgs(act) or strip_msgs(e2) == strip_msgs(a2):
            de = [l.split(":")[0] for l in exp if l.startswith(("DETAIL:", "HINT:", "CONTEXT:"))]
            da = [l.split(":")[0] for l in act if l.startswith(("DETAIL:", "HINT:", "CONTEXT:"))]
            kinds = sorted(set(de) ^ set(da)) or sorted(set(de)) or ["DETAIL"]
            return "error-detail", "/".join(kinds) + " lines differ"
        return "error-wording", norm(ee) + " (+ following output)"
    if strip_msgs(exp, ("NOTICE", "WARNING", "INFO")) == strip_msgs(act, ("NOTICE", "WARNING", "INFO")):
        ne = [l for l in exp if l.startswith(("NOTICE:", "WARNING:", "INFO:"))]
        na = [l for l in act if l.startswith(("NOTICE:", "WARNING:", "INFO:"))]
        d = [l for l in ne if l not in na] or [l for l in na if l not in ne]
        side = "missing" if [l for l in ne if l not in na] else "extra"
        return "notice", f"{side} " + (norm(d[0]) if d else "NOTICE")
    e_rows = [l for l in exp if l.strip()]
    a_rows = [l for l in act if l.strip()]
    if sorted(e_rows) == sorted(a_rows):
        return "row-order", "rows returned in a different order"
    if len(exp) == len(act) and len(exp) > 2:
        diffs = [i for i in range(len(exp)) if exp[i] != act[i]]
        if diffs and all(i <= 1 for i in diffs):
            return "header", "result column name/width differs"
    ne = len([l for l in exp if re.match(r"^\(\d+ rows?\)$", l)])
    rows_e = next((l for l in exp if re.match(r"^\(\d+ rows?\)$", l)), None)
    rows_a = next((l for l in act if re.match(r"^\(\d+ rows?\)$", l)), None)
    kind = "row count differs" if rows_e != rows_a else "values differ"
    return "values", f"{test}: {lead(sql)} result {kind}"


def analyze(run, scope_sel):
    with open(os.path.join(run, "summary.json")) as f:
        summary = json.load(f)
    clusters = {}
    per_test = {}
    for t in summary["tests"]:
        if t["status"] == "pass":
            continue
        if scope_sel != "all" and t["scope"] != scope_sel:
            continue
        test = t["test"]
        sqlp = os.path.join(REGRESS, "sql", f"{test}.sql")
        resp = os.path.join(run, "results", f"{test}.out")
        expp = os.path.join(REGRESS, "expected", t["matched"] or f"{test}.out")
        if not (os.path.isfile(resp) and os.path.isfile(expp)):
            continue
        src0 = read_lines(sqlp)
        eo, _, src = segment(src0, read_lines(expp))
        ao, aline, _ = segment(src0, read_lines(resp))
        anchors = sorted(set(eo) | set(ao))
        max_echo = max(aline.values()) if aline else -1
        differing = {}
        for key in anchors:
            e = [l for l in eo.get(key, []) if l != ""] if key in eo else []
            a_ = [l for l in ao.get(key, []) if l != ""] if key in ao else []
            if eo.get(key, []) != ao.get(key, []):
                sql = statement_text(src, anchors, key) if key >= 0 else ["<before first statement>"]
                if not e and not a_:
                    continue
                if key > max_echo:
                    cat, k = "not-run", "statement never ran: the test stopped early (server crash, hang or psql exit)"
                else:
                    cat, k = classify(sql, eo.get(key, []), ao.get(key, []), test)
                differing[key] = (sql, eo.get(key, []), ao.get(key, []), cat, k)
        # attribute hunks
        hunks = []
        dp = os.path.join(run, "diffs", f"{test}.diff")
        if os.path.isfile(dp):
            for l in read_lines(dp):
                m = re.match(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", l)
                if m:
                    c = int(m.group(1))
                    d = int(m.group(2) or 1)
                    hunks.append((c, c + max(d, 1) - 1))
        hunk_owner = collections.Counter()
        for (c0, c1) in hunks:
            owner = None
            for ln in range(c0, c1 + 1):
                k = aline.get(ln)
                if k in differing:
                    owner = differing[k][3:5]
                    break
            if owner is None:
                # the hunk may only remove expected lines: find the anchor just before it
                k = aline.get(max(c0 - 1, 1))
                for kk in sorted(differing):
                    if k is not None and kk >= k:
                        owner = differing[kk][3:5]
                        break
            hunk_owner[owner or ("unattributed", "diff hunk not matched to a statement")] += 1
        per_test[test] = {"blocks": len(differing), "hunks": len(hunks)}
        for key, (sql, e, a_, cat, k) in differing.items():
            ck = (cat, k)
            c = clusters.setdefault(ck, {"category": cat, "key": k, "blocks": 0, "hunks": 0,
                                         "tests": collections.Counter(), "examples": []})
            c["blocks"] += 1
            c["tests"][test] += 1
            if len(c["examples"]) < 3 and test not in [x["test"] for x in c["examples"]]:
                c["examples"].append({
                    "test": test,
                    "sql": "\n".join(sql)[-400:],
                    "pg": "\n".join(e[:8])[:400],
                    "bicdb": "\n".join(a_[:8])[:400],
                })
        for ck, n in hunk_owner.items():
            c = clusters.setdefault(ck, {"category": ck[0], "key": ck[1], "blocks": 0, "hunks": 0,
                                         "tests": collections.Counter(), "examples": []})
            c["hunks"] += n
    out = sorted(clusters.values(), key=lambda c: (-c["blocks"], -c["hunks"]))
    for c in out:
        c["tests"] = dict(c["tests"].most_common())
    return summary, out, per_test


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--out")
    ap.add_argument("--md")
    ap.add_argument("--scope", default="in", choices=["in", "out", "all"])
    ap.add_argument("--top", type=int, default=80)
    args = ap.parse_args()
    summary, clusters, per_test = analyze(args.run, args.scope)
    if args.out:
        with open(args.out, "w") as f:
            json.dump({"run": summary.get("label"), "scope": args.scope, "clusters": clusters,
                       "per_test": per_test}, f, indent=1)
    lines = []
    tb = sum(c["blocks"] for c in clusters)
    th = sum(c["hunks"] for c in clusters)
    lines.append(f"{len(clusters)} clusters, {tb} differing statements, {th} diff hunks (scope={args.scope})")
    for i, c in enumerate(clusters[: args.top], 1):
        tests = ", ".join(f"{t}({n})" for t, n in list(c["tests"].items())[:6])
        lines.append(f"{i:3d}. [{c['category']}] {c['key']}  stmts={c['blocks']} hunks={c['hunks']}  {tests}")
    text = "\n".join(lines)
    if args.md:
        with open(args.md, "w") as f:
            f.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
