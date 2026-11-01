#!/usr/bin/env python3
"""Recompute recorded oracle digests from intent history, using only Python."""
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    report = json.loads((root / "release-report.json").read_text())
    evidence = json.loads((root / "evidence.json").read_text())
    for name, digest in evidence["file_sha256"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
    with gzip.open(root / "transaction-intents.jsonl.gz", "rt") as transcript:
        attempts = [json.loads(line) for line in transcript]
    assert len({(r["writer"], r["seq"]) for r in attempts}) == len(attempts)
    assert dict(Counter(r["outcome"] for r in attempts)) == evidence["transcript_outcomes"]
    assert all(r["outcome"] in {"acknowledged", "aborted", "resolved_committed", "resolved_aborted"}
               for r in attempts)
    for row in attempts:
        assert row["token"] == hashlib.sha256(f"qualified:{row['writer']}:{row['seq']}".encode()).hexdigest()
        assert row["delta"] == ((row["writer"] + 3) * (row["seq"] + 7)) % 199 - 99
        assert bool(row["acknowledged"]) == (row["outcome"] == "acknowledged")
    params = report["parameters"]
    payload = [(i, hashlib.shake_256(f"payload:{i}".encode()).hexdigest(params["seed_bytes"] // 2))
               for i in range(params["seed_rows"])]
    payload_digest = hashlib.sha256()
    for row in payload:
        payload_digest.update(json.dumps(row, separators=(",", ":")).encode())
    assert payload_digest.hexdigest() == evidence["payload_fixture_sha256"]
    committed = sorted((r for r in attempts if r["outcome"] in {"acknowledged", "resolved_committed"}),
                       key=lambda r: (r["writer"], r["seq"]))
    assert len(committed) == report["acknowledged_transactions"] + evidence["transcript_outcomes"]["resolved_committed"]
    for check in report["oracle_checks"]:
        heads = {int(w): seq for w, seq in check["heads"].items()}
        accounts = {w: [w, 0, 0] for w in range(params["writers"])}
        slots = {w * 16 + s: [w * 16 + s, 0, "initial"]
                 for w in range(params["writers"]) for s in range(16)}
        ledger = []
        total = 0
        for row in committed:
            writer, seq, delta = row["writer"], row["seq"], row["delta"]
            if seq > heads[writer]:
                continue
            assert not row["rolled_back"]
            for w, predecessor in json.loads(row["dependencies"]).items():
                assert heads[int(w)] >= predecessor
            ledger.append([writer * 10000000 + seq, writer, seq, delta, row["token"]])
            accounts[writer][1] = seq
            accounts[writer][2] += delta
            slot = writer * 16 + seq % 16
            slots[slot] = [slot, delta, row["token"]]
            total += delta
        assert {w: a[1] for w, a in accounts.items()} == heads
        rows_by_query = [ledger, list(accounts.values()), list(slots.values()),
                         [[1, len(ledger), total]], payload]
        rows_by_query.extend([[row[0]] for row in ledger if row[1] == w and 3 <= row[2] <= 10000000]
                             for w in range(params["writers"]))
        rows_by_query.append([[row[0], row[1]] for row in slots.values() if row[1] >= 0])
        digest = hashlib.sha256()
        for rows in rows_by_query:
            digest.update(json.dumps(rows, separators=(",", ":")).encode())
        assert len(ledger) == check["ledger_rows"], check["label"]
        assert len(rows_by_query) == check["queries_compared"], check["label"]
        assert digest.hexdigest() == check["oracle_sha256"], check["label"]
    print(json.dumps({"passed": True, "attempts": len(attempts),
                      "oracle_checks_recomputed": len(report["oracle_checks"]),
                      "query_results_recomputed": sum(c["queries_compared"] for c in report["oracle_checks"])},
                     indent=2))


if __name__ == "__main__":
    main()
