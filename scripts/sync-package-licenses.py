#!/usr/bin/env python3
"""Synchronize every package's packaged legal files with the root copies.

The root LICENSE (verbatim Apache License 2.0), NOTICE and
THIRD_PARTY_NOTICES.md are the single source of truth. Every Cargo workspace
member ships identical LICENSE, NOTICE and THIRD_PARTY_NOTICES.md copies; every
non-Cargo distributable directory listed in licensing/transition.toml
(`package_paths`) ships identical LICENSE and NOTICE copies. Legal files of the
retired custom license (per-package LICENSE-SCOPE.md, LICENSES/Apache-2.0.txt,
licensing/NOTICE-Apache.txt) must not be shipped by packages.

--check never writes; without it, stale copies are rewritten and retired
files are removed.
"""
import argparse
from pathlib import Path
import shutil
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
config = tomllib.loads((root / "licensing/transition.toml").read_text())
assert config["license_identifier"] == "Apache-2.0"

CARGO_FILES = ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md")
DIRECTORY_FILES = ("LICENSE", "NOTICE")
RETIRED = ("LICENSE-SCOPE.md", "LICENSES", "licensing")

targets = [(member, CARGO_FILES) for member in workspace["members"]]
targets += [(path, DIRECTORY_FILES) for path in config["package_paths"]]

problems = []
for relative, names in targets:
    package = root / relative
    assert package.is_dir(), relative
    for name in names:
        source = (root / name).read_bytes()
        destination = package / name
        if destination.exists() and destination.read_bytes() == source:
            continue
        if args.check:
            problems.append(f"stale or missing {destination.relative_to(root)}")
        else:
            destination.write_bytes(source)
    if relative in workspace["members"]:
        for name in RETIRED:
            retired = package / name
            if not retired.exists():
                continue
            if args.check:
                problems.append(f"retired legal file present {retired.relative_to(root)}")
            elif retired.is_dir():
                shutil.rmtree(retired)
            else:
                retired.unlink()
if problems:
    raise SystemExit("Package legal files out of sync:\n  " + "\n  ".join(problems))
print(f"Package legal files {'verified' if args.check else 'synchronized'}: "
      f"{len(workspace['members'])} Cargo packages, {len(config['package_paths'])} directories")
