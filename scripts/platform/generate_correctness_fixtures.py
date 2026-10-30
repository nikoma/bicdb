#!/usr/bin/env python3
"""Generate deterministic, strict PostgreSQL oracle cases plus audit regressions."""
import argparse
import json
from pathlib import Path
import random
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--seeds", type=int, default=64)
    args = parser.parse_args()
    if args.seeds < 1:
        parser.error("--seeds must be positive")
    args.destination.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[2]
    for fixture in (root / "fixtures/correctness-audit").glob("*.json"):
        shutil.copyfile(fixture, args.destination / fixture.name)
    for seed in range(args.seeds):
        rng = random.Random(seed)
        table = f"audit_fuzz_{seed}"
        rows = []
        for index in range(12):
            number = rng.randint(-10, 10)
            value = "NULL" if rng.random() < 0.3 else f"'v{rng.randint(0, 5)}'"
            flag = rng.choice(["true", "false", "NULL"])
            rows.append(f"('r{index}', {number}, {value}, {flag})")
        fixture = {
            "id": f"fuzz_{seed:03}",
            "description": "Strict seeded query/mutation oracle; no expected differences",
            "cleanup_sql": [f"DROP TABLE IF EXISTS {table}"],
            "sql": [
                f"CREATE TABLE {table}(id TEXT PRIMARY KEY,k INT,v TEXT,flag BOOLEAN)",
                f"INSERT INTO {table} VALUES {','.join(rows)}",
                f"SELECT id,COALESCE(v,'nil') AS val,k FROM {table} WHERE flag IS DISTINCT FROM false ORDER BY k,id LIMIT 7 OFFSET 1",
                f"SELECT id FROM {table} WHERE k IN (-3,0,4,NULL) ORDER BY id",
                f"SELECT id FROM {table} WHERE k NOT IN (-3,0,4,NULL) ORDER BY id",
                f"UPDATE {table} SET k=k+1 WHERE v IS NULL OR k<0",
                f"DELETE FROM {table} WHERE k>8",
                f"SELECT flag,COUNT(*) AS n,SUM(k) AS total,MIN(k) AS lo,MAX(k) AS hi FROM {table} GROUP BY flag ORDER BY flag NULLS FIRST",
                f"SELECT id,k,v,flag FROM {table} ORDER BY id",
            ],
        }
        (args.destination / f"fuzz_{seed:03}.json").write_text(
            json.dumps(fixture, indent=2) + "\n"
        )
    print(f"Generated {len(list(args.destination.glob('*.json')))} strict fixtures")


if __name__ == "__main__":
    main()
