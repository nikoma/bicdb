#!/usr/bin/env python3
"""Strip only marked cfg(test) insertions, then compare rustfmt-normalized bodies.

This is a source parity check, not a compiler, runtime test or timing claim.
"""
import argparse
import hashlib
import re
import subprocess

BASE = "735b3de5a1380deacaf8dd88702945f78df741b9"
FILES = [
    "distribution_data/native_range_read.rs",
    "distribution_native_read.rs",
    "distribution_native_read/quorum.rs",
    "distribution_native_read/strong.rs",
    "distribution_range_recovery/runtime/native_read.rs",
    "distribution_range_recovery/runtime/strong_point_read.rs",
]
PREFIX = "crates/bicdb-core/src/"
MARKED = re.compile(r"^[ \t]*// STRONG_TRACE_BEGIN\n.*?^[ \t]*// STRONG_TRACE_END\n", re.M | re.S)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--candidate", default="HEAD")
    args = parser.parse_args()

    def source(revision, path):
        return subprocess.check_output(
            ["git", "-C", args.repo, "show", f"{revision}:{path}"], text=True
        )

    def normalize(text):
        formatted = subprocess.run(
            ["rustfmt", "--edition", "2021", "--config", "skip_children=true", "--emit", "stdout"],
            input=text, capture_output=True, text=True, check=True,
        ).stdout
        # Diagnostic insertions add blank lines; retain every nonblank source line.
        return "\n".join(line for line in formatted.splitlines() if line.strip())

    for leaf in FILES:
        path = PREFIX + leaf
        before = normalize(source(BASE, path))
        raw = source(args.candidate, path)
        assert raw.count("// STRONG_TRACE_BEGIN") == raw.count("// STRONG_TRACE_END"), path
        for insertion in MARKED.findall(raw):
            assert "#[cfg(test)]" in insertion, path
        after = normalize(MARKED.sub("", raw))
        assert before == after, f"non-diagnostic source differs: {path}"
        print("PASS", path, hashlib.sha256(before.encode()).hexdigest())
    print("PASS: six existing files retain normalized source after stripping marked diagnostics")


if __name__ == "__main__":
    main()
