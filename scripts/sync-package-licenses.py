#!/usr/bin/env python3
"""Synchronize mixed-scope Rust legal files. --check never writes."""
import argparse
from pathlib import Path
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
config = tomllib.loads((root / "licensing/transition.toml").read_text())
assert config["status"] == "active"
assert workspace["package"].get("license-file") == "LICENSE"
retained = set(config["retained_apache_packages"])
missing = []
for member in workspace["members"]:
    package = root / member
    name = tomllib.loads((package / "Cargo.toml").read_text())["package"]["name"]
    files = {"THIRD_PARTY_NOTICES.md": "THIRD_PARTY_NOTICES.md"}
    if name in retained:
        files.update({"LICENSE": "LICENSES/Apache-2.0.txt", "NOTICE": "licensing/NOTICE-Apache.txt"})
    else:
        files.update({"LICENSE": "LICENSE", "NOTICE": "NOTICE", "LICENSE-SCOPE.md": "LICENSE-SCOPE.md",
                      "LICENSES/Apache-2.0.txt": "LICENSES/Apache-2.0.txt",
                      "licensing/NOTICE-Apache.txt": "licensing/NOTICE-Apache.txt"})
    for destination_name, source_name in files.items():
        source = (root / source_name).read_bytes()
        destination = package / destination_name
        if not destination.exists() or destination.read_bytes() != source:
            if args.check:
                missing.append(str(destination.relative_to(root)))
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(source)
if missing:
    raise SystemExit("Package legal files missing/stale: " + ", ".join(missing))
print(f"Mixed-scope package legal files {'verified' if args.check else 'synchronized'}: {len(workspace['members'])} packages")
