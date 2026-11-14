#!/usr/bin/env python3
"""Run all selected workspace tests while retaining only dependency artifacts.

Cargo normally links every integration binary before running the first test.
This workspace's runtime dependencies make that exceed a typical runner's disk.
Discover the real Cargo target graph, execute each target, and remove only the
successful target's reported test executable. Failed tests stop the run.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess


# Fixtures that start four to six real server processes per test and assert
# the product's real-time request budgets. Several of them at once on one
# runner measure runner contention, not the protocol (the same reason the
# in-process multi-node Core fixtures have their own serial job), so these
# targets run their tests one at a time. Nothing is skipped.
SERIAL_TARGETS = {("bicdb-cli", "native_metadata_operator_cli")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exclude", action="append", default=[])
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], text=True
    ))
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members and p["name"] not in args.exclude]
    deps = (Path(os.environ.get("CARGO_TARGET_DIR", metadata["target_directory"])) / "debug/deps").resolve()
    threads = os.environ.get("RUST_TEST_THREADS", "4")
    count = 0
    for package in packages:
        invocations = []
        for target in package["targets"]:
            if not target["test"] or "example" in target["kind"]:
                continue
            selection = None
            if "test" in target["kind"]:
                selection = ["--test", target["name"]]
            elif "bin" in target["kind"]:
                selection = ["--bin", target["name"]]
            elif any(kind in target["kind"] for kind in ("lib", "rlib", "proc-macro")):
                selection = ["--lib"]
            if selection is not None:
                # Cargo skips gated targets in the default workspace run.
                # Explicit target discovery must supply their declared features.
                if target.get("required-features"):
                    selection += ["--features", ",".join(target["required-features"])]
                serial = (package["name"], target["name"]) in SERIAL_TARGETS
                invocations.append((selection, "1" if serial else threads))
        if any(target.get("doctest") for target in package["targets"]):
            invocations.append((["--doc"], threads))
        for selection, target_threads in invocations:
            command = ["cargo", "test", "-p", package["name"], *selection, "--locked", "--message-format=json-render-diagnostics", "--", f"--test-threads={target_threads}"]
            print("Running:", " ".join(command), flush=True)
            executables = set()
            process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            for line in process.stdout:
                print(line, end="", flush=True)
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if (isinstance(item, dict) and item.get("reason") == "compiler-artifact"
                    and item.get("package_id") == package["id"]
                    and item.get("profile", {}).get("test") and item.get("executable")):
                    executable = Path(item["executable"]).resolve()
                    if executable.parent == deps:
                        executables.add(executable)
            status = process.wait()
            if status:
                raise SystemExit(status)
            for executable in executables:
                executable.unlink(missing_ok=True)
            count += 1
    print(f"Completed {count} Cargo test targets across {len(packages)} workspace packages", flush=True)


if __name__ == "__main__":
    main()
