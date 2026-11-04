"""Fixed numeric pictures with independent Oracle/PostgreSQL expectations.

The recorded expectations use NLS_NUMERIC_CHARACTERS='.,' and PostgreSQL
lc_numeric=en_US.utf8. Other locales and format tokens are not qualified here.
No application schema or persistent objects are required.
"""
import json
from pathlib import Path
from row_lock_waits import main as comparison_main


def observe(factory, oracle):
    observations = json.loads(Path(__file__).with_suffix(".json").read_text())["observations"]
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        oracle_driver = type(conn).__module__.startswith("oracledb")
        report["server_version"] = conn.version if oracle_driver else conn.info.server_version
        for index, case in enumerate(observations):
            expected = case["oracle" if oracle else "postgres"]
            row = dict(id=str(index), model=case["model"], literal=case["literal"], expected=expected)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT TO_CHAR(" + case["literal"] + ", '" + case["model"] + "')" + (" FROM DUAL" if oracle else ""))
                    actual = cursor.fetchone()[0]
                    row.update(actual=actual, qualified=actual == expected)
            except Exception as exc:
                row.update(exception=type(exc).__name__, detail=str(exc)[:500], qualified=False)
            report["cases"].append(row)
    return report


def qualified(row):
    return bool(row.get("qualified", False))


def run(source_factory, target_factory):
    with source_factory() as probe:
        oracle = type(probe).__module__.startswith("oracledb")
    source, target = observe(source_factory, oracle), observe(target_factory, oracle)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"], matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(c["matched"] for c in cases),
                source_qualified=all(c["source_qualified"] for c in cases), cleanup_errors=[],
                success=len(cases) == 120 and all(c["matched"] for c in cases))


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
