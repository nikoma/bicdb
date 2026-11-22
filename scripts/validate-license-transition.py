#!/usr/bin/env python3
"""Check the Apache-2.0 license invariants, package metadata and licensing links.

Invariants:
- the root LICENSE is the verbatim, standard Apache License 2.0 text (hash-pinned,
  including the appendix) and LICENSES/Apache-2.0.txt is identical to it;
- the root NOTICE has exactly the owner-supplied text;
- every Cargo package declares `license = "Apache-2.0"` (directly or through the
  workspace) and none uses `license-file`; the npm client and other manifests
  declare Apache-2.0;
- deny.toml carries no first-party custom-license clarification or exception;
- no tracked file outside historical records claims the retired custom license
  identifier, and every SPDX header names Apache-2.0;
- licensing documents have no broken relative links.
Packaged LICENSE/NOTICE copies are checked by scripts/sync-package-licenses.py.
"""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib

root = Path(__file__).resolve().parents[1]
config = tomllib.loads((root / "licensing/transition.toml").read_text())
APACHE_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
RETIRED = "LicenseRef-" + "BicDB-1.0"
EXPECTED_NOTICE = """BicDB
Copyright 2026 WalkNorth Inc. and Nikolai Manek
https://bicdb.com

BicDB is licensed under the Apache License,
Version 2.0.

Original software developed by WalkNorth Inc. and Nikolai Manek.
"""

# License record.
assert config["status"] == "active"
assert config["license_identifier"] == "Apache-2.0"
assert config["license_file"] == "LICENSE" and config["notice_file"] == "NOTICE"
assert config["license_sha256"] == APACHE_SHA256

# Root LICENSE: verbatim Apache 2.0, nothing prepended or appended.
license_bytes = (root / "LICENSE").read_bytes()
assert hashlib.sha256(license_bytes).hexdigest() == APACHE_SHA256, \
    "LICENSE must be the verbatim Apache License 2.0 text"
assert license_bytes == (root / "LICENSES/Apache-2.0.txt").read_bytes()
license_text = license_bytes.decode()
assert license_text.lstrip().startswith("Apache License\n                           Version 2.0, January 2004")
assert "END OF TERMS AND CONDITIONS" in license_text
assert "APPENDIX: How to apply the Apache License to your work." in license_text

# Root NOTICE: exactly the owner's text.
assert (root / "NOTICE").read_text() == EXPECTED_NOTICE, "NOTICE must match the recorded text"

# Cargo metadata.
workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
assert workspace["package"].get("license") == "Apache-2.0"
assert "license-file" not in workspace["package"]
for member in workspace["members"]:
    pkg = tomllib.loads((root / member / "Cargo.toml").read_text())["package"]
    assert pkg.get("license") in ({"workspace": True}, "Apache-2.0"), f"{member}: license"
    assert "license-file" not in pkg, f"{member}: license-file must not be used"
    # A published crate's `include` list must keep the legal files in the .crate.
    if pkg.get("publish", True) is not False and "include" in pkg:
        for name in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"):
            assert f"/{name}" in pkg["include"], f"{member}: include must ship {name}"

# Other manifests.
for path in config["package_paths"]:
    assert (root / path).is_dir(), path
client = json.loads((root / "web/bicdb-client/package.json").read_text())
lock = json.loads((root / "web/bicdb-client/package-lock.json").read_text())
assert client["license"] == lock["packages"][""]["license"] == "Apache-2.0"
assert {"LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md", "docs/licensing-faq.md"} <= set(client["files"])
assert ':license {:name "Apache-2.0"' in (root / "jepsen/bicdb/project.clj").read_text()
assert "<name>Apache License, Version 2.0</name>" in (root / "examples/oracle/jdbc/pom.xml").read_text()

# Dependency policy: Apache-2.0 allowed, no first-party custom-license carve-outs.
deny = tomllib.loads((root / "deny.toml").read_text())["licenses"]
assert "Apache-2.0" in deny["allow"]
assert all(RETIRED not in c["expression"] for c in deny.get("clarify", []))
assert all(RETIRED not in c["allow"] for c in deny.get("exceptions", []))

# No tracked file outside historical records claims the retired identifier,
# and every SPDX header names Apache-2.0.
SPDX_HEADER = re.compile("SPDX-License-" + r"Identifier:\s*([^\s*\"'<>]+)")
HISTORICAL_PREFIXES = ("docs/releases/", "docs/evidence/")
tracked = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True,
                         capture_output=True).stdout.decode().split("\0")
for relative in filter(None, tracked):
    if relative.startswith(HISTORICAL_PREFIXES):
        continue
    path = root / relative
    if not path.is_file():
        continue
    try:
        content = path.read_text()
    except (UnicodeDecodeError, OSError):
        continue
    assert RETIRED not in content, f"{relative} references the retired custom license"
    for spdx in SPDX_HEADER.findall(content):
        assert spdx == "Apache-2.0", f"{relative}: SPDX identifier {spdx}"

# Current licensing documents: relative links resolve.
documents = ["README.md", "CONTRIBUTING.md", "THIRD_PARTY_NOTICES.md", "docs/licensing-faq.md",
             "licensing/packaging-proposal.md",
             "web/bicdb-client/docs/licensing-faq.md"]
for relative in documents:
    path = root / relative
    content = path.read_text()
    if relative == "README.md":
        content = content.split("## License\n", 1)[1]
    for link in re.findall(r"\]\(([^)]+)\)", content):
        if "://" not in link and not link.startswith("#"):
            assert (path.parent / link.split("#", 1)[0]).exists(), f"Broken link: {relative}: {link}"

# Source versions advance at every merge. Download links must continue to
# identify an actual published release rather than inventing that source tag.
version = tomllib.loads((root / "crates/bicdb-core/Cargo.toml").read_text())["package"]["version"]
readme = (root / "README.md").read_text()
downloads = re.findall(
    r"\[Download ([0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?)\]"
    r"\(https://github\.com/nikoma/bicdb/releases/tag/v([^)]+)\)", readme)
assert len(downloads) == 1, "Exactly one published release download is required"
published, tag = downloads[0]
assert published == tag, "Published download label and release tag must agree"
def version_components(value):
    parsed = re.fullmatch(r"([0-9]+)\.([0-9]+)\.([0-9]+)(?:-[A-Za-z0-9.-]+)?", value)
    assert parsed, value
    return tuple(map(int, parsed.groups()))
assert version_components(published) <= version_components(version), "Published release cannot exceed current source"
assert (f"https://github.com/nikoma/bicdb/releases/download/v{published}/"
        f"bicdb-{published}-linux-x86_64.tar.gz") in readme
assert (f"https://github.com/nikoma/bicdb/releases/download/v{published}/SHA256SUMS") in readme
print(f"Apache-2.0 license: verbatim LICENSE, NOTICE, {len(workspace['members'])} Cargo packages, "
      f"manifests, deny policy, {len(tracked) - 1} tracked files and licensing links passed")
