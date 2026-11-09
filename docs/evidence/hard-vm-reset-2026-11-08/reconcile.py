#!/usr/bin/env python3
"""Check portable external ledgers; does not substitute for guest row checks."""
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def lines(path):
    if not path.exists():
        return []
    with gzip.open(path, "rt") as stream:
        return [json.loads(line) for line in stream]

def main():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    for name, expected in manifest.items():
        relative = Path(name)
        assert not relative.is_absolute() and ".." not in relative.parts
        assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == expected, name
    output = []
    for directory in sorted((ROOT / "runs").glob("campaign-*")):
        result = json.loads((directory / "result.json").read_text())
        ack = lines(directory / "ack.jsonl.gz")
        intent = lines(directory / "intent.jsonl.gz")
        attempts = lines(directory / "commit-attempts.jsonl.gz")
        identities = lines(directory / "tx-identities.jsonl.gz")
        recovery = lines(directory / "recoveries.jsonl.gz")
        resets = lines(directory / "resets.jsonl.gz")
        by_id = {item["id"]: item for item in intent}
        assert len(by_id) == len(intent)
        assert len({item["id"] for item in ack}) == len(ack)
        assert len(ack) == result["acknowledged"]
        assert len(intent) == result["attempted"]
        for item in ack:
            assert by_id[item["id"]]["row"] == item["row"]
        if result.get("native_transaction_ids_recorded"):
            committed = {item["id"]: item for item in attempts}
            allocated = {item["id"]: item for item in identities}
            assert len(committed) == len(attempts) == result["commit_attempts"]
            for item in ack:
                assert item["native_transaction_id"] == committed[item["id"]]["native_transaction_id"]
                assert item["native_transaction_id"] == allocated[item["id"]]["native_transaction_id"]
        native = [item["native_transaction_id"] for item in ack if "native_transaction_id" in item]
        reused = len(native) - len(set(native))
        if reused:
            assert not result["complete"]
            assert any(item["kind"] == "acknowledged-native-txid-reused" for item in result["failures"])
        if result.get("complete"):
            assert not result["failures"] and not any(item["problems"] for item in recovery)
            assert len(resets) == 12
            assert {"repeat-0", "repeat-1"}.issubset({str(item["round"]) for item in recovery})
            assert result["initial_backup_restore_verified"]
        mode = directory / "persisted-format.json"
        mode = json.loads(mode.read_text())["format"]["storage_mode"] if mode.exists() else "not opened"
        output.append(dict(run=directory.name, source=result["source"], storage_mode=mode,
                           acknowledged=len(ack), attempted=len(intent), hard_resets=len(resets),
                           recoveries=len(recovery), duplicate_acknowledged_native_ids=reused,
                           complete=result["complete"], failure_kinds=sorted({item["kind"] for item in result["failures"]})))
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
