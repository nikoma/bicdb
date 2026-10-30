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
            if "test" in target["kind"]:
                invocations.append(["--test", target["name"]])
            elif "bin" in target["kind"]:
                invocations.append(["--bin", target["name"]])
            elif any(kind in target["kind"] for kind in ("lib", "rlib", "proc-macro")):
                invocations.append(["--lib"])
        if any(target.get("doctest") for target in package["targets"]):
            invocations.append(["--doc"])
        for selection in invocations:
            command = ["cargo", "test", "-p", package["name"], *selection, "--locked", "--message-format=json-render-diagnostics", "--", f"--test-threads={threads}"]
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
