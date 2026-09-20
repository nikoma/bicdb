#!/usr/bin/env python3
"""Check active license, mixed metadata, clause references and licensing links."""
import hashlib
import json
from pathlib import Path
import re
import tomllib

root = Path(__file__).resolve().parents[1]
config = tomllib.loads((root / "licensing/transition.toml").read_text())
identifier = "LicenseRef-BicDB-1.0"
assert config["status"] == "active"
assert config["license_identifier"] == config["current_license"] == identifier
assert config["license_file"] == "LICENSE"
assert config["authorized_licensor"] and config["authority_record"]
# Activation is revision-based, not a fabricated release number or retroactive date.
assert re.fullmatch(r"[0-9a-f]{40}", config["first_covered_revision"])
assert config["first_covered_revision"] in (root / "LICENSE-SCOPE.md").read_text()
text = (root / "LICENSE").read_text()
assert text.startswith("BicDB License 1.0\nApache 2.0 + three exceptions\n")
assert identifier in text and "DRAFT" not in text
assert hashlib.sha256((root / "LICENSE").read_bytes()).hexdigest() == config["license_sha256"]
marker = "----- BEGIN VERBATIM APACHE LICENSE 2.0 -----\n"
assert text.count(marker) == 1
prefix, apache = (root / "LICENSE").read_bytes().split(marker.encode(), 1)
assert apache == (root / "LICENSES/Apache-2.0.txt").read_bytes(), "Apache text must be verbatim"
assert re.findall(rb"^([0-9]+)\. ", prefix, re.M) == [b"1", b"2", b"3"]
assert b"own modifications under different terms" in prefix
assert b"not unmodified Apache-2.0" in prefix
assert "same community version" not in text
assert hashlib.sha256((root / "LICENSES/Apache-2.0.txt").read_bytes()).hexdigest() == \
    "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
assert "Copyright 2026 WalkNorth, Inc. and Nikolai Manek" in (root / "NOTICE").read_text()
assert "This product is licensed under the Apache" not in (root / "NOTICE").read_text()
workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
assert workspace["package"].get("license-file") == "LICENSE"
assert "license" not in workspace["package"]
retained = set(config["retained_apache_packages"])
packages = {}
for member in workspace["members"]:
    pkg = tomllib.loads((root / member / "Cargo.toml").read_text())["package"]
    name = pkg["name"]
    packages[name] = member
    if name in retained:
        assert pkg.get("license") == "Apache-2.0" and "license-file" not in pkg, name
    else:
        assert pkg.get("license-file") == {"workspace": True} and "license" not in pkg, name
assert retained <= packages.keys()
covered = packages.keys() - retained
assert len(covered) == 17
for path in config["retained_apache_paths"] + config["console_paths"]:
    assert (root / path).exists(), path
for path in ["web/bicdb-client", "abi", "examples", "jepsen"]:
    assert (root / path / "LICENSE").read_bytes() == (root / "LICENSES/Apache-2.0.txt").read_bytes()
client = json.loads((root / "web/bicdb-client/package.json").read_text())
lock = json.loads((root / "web/bicdb-client/package-lock.json").read_text())
assert client["license"] == lock["packages"][""]["license"] == "Apache-2.0"
assert {"LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md", "docs/licensing-faq.md"} <= set(client["files"])
deny = tomllib.loads((root / "deny.toml").read_text())["licenses"]
assert identifier not in deny["allow"], "Do not allow this license on all dependencies"
clarifications = {c["name"]: c for c in deny["clarify"] if c["expression"] == identifier}
assert clarifications.keys() == covered
exceptions = {c["name"] for c in deny["exceptions"] if identifier in c["allow"]}
assert exceptions == covered
for c in clarifications.values():
    assert c["license-files"] == [{"path": "LICENSE", "hash": 0xf29e79cd}]
# Check current licensing pages, leaving unrelated historical records untouched.
documents = ["README.md", "CONTRIBUTING.md", "LICENSE-SCOPE.md", "docs/licensing-faq.md",
             "docs/licensing-transition.md", "docs/commercial-licensing.md",
             "docs/licensing-implementation-report.md", "licensing/packaging-proposal.md",
             "licensing/contributor-agreement.draft.md", "web/bicdb-client/docs/licensing-faq.md"]
for relative in documents:
    path = root / relative
    content = path.read_text()
    if relative == "README.md":
        content = content.split("## License\n", 1)[1]
    for link in re.findall(r"\]\(([^)]+)\)", content):
        if "://" not in link and not link.startswith("#"):
            assert (path.parent / link.split("#", 1)[0]).exists(), f"Broken link: {relative}: {link}"
    if relative != "licensing/contributor-agreement.draft.md":
        for stale in ("draft-not-adopted", "current checkout retains", "not an operative replacement yet"):
            assert stale not in content, f"Stale active status: {relative}"
faq = (root / "docs/licensing-faq.md").read_text()
assert faq.count("| Permitted under community terms |") == 10
assert faq.count("| Separate commercial agreement required |") == 8
for number in re.findall(r"Exception (\d+)", faq):
    assert number in {"1", "2", "3"}, number
version = tomllib.loads((root / "crates/bicdb-core/Cargo.toml").read_text())["package"]["version"]
assert f"Current release: {version}" in (root / "README.md").read_text()
print("active license: 17 community packages, 11 Apache exceptions, notices, 18 scenarios and links passed")
