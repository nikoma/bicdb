#!/usr/bin/env python3
"""Summarize the JUnit XML files written by `bench run-tests --junit-xml-output`.

usage: junit_summary.py JUNIT_DIR [KNOWN_FAILURES_FILE] [EXPECTED_MODULE ...]

Exits non-zero when a test fails or errors that is not listed in
KNOWN_FAILURES_FILE (one `module.Class.test_name` per line, `#` comments), or
when an expected module wrote no report (it crashed before running).
"""

import pathlib
import re
import sys
import xml.etree.ElementTree as ET

directory = pathlib.Path(sys.argv[1])
known = set()
if len(sys.argv) > 2 and sys.argv[2]:
	for line in pathlib.Path(sys.argv[2]).read_text().splitlines():
		line = line.split("#", 1)[0].strip()
		if line:
			known.add(line)
expected_modules = sys.argv[3:]

total = {"tests": 0, "passed": 0, "failed": 0, "skipped": 0}
unexpected, known_seen, reported = [], [], set()
print(f"{'module':<28} {'tests':>6} {'passed':>7} {'failed':>7} {'skipped':>8}")
for path in sorted(directory.glob("*.xml")):
	counts = dict.fromkeys(total, 0)
	# The unit and integration phases each append a <testsuites> document.
	text = re.sub(r"<\?xml[^>]*\?>", "", path.read_text())
	try:
		if not text.strip():
			raise ET.ParseError("empty report")
		cases = list(ET.fromstring(f"<reports>{text}</reports>").iter("testcase"))
	except ET.ParseError:
		# The runner died before writing its report (e.g. in test setup).
		print(f"{path.stem:<28} (no report)")
		continue
	for case in cases:
		name = f"{case.get('classname')}.{case.get('name')}"
		reported.add(case.get("classname", "").rsplit(".", 1)[0])
		counts["tests"] += 1
		if case.find("failure") is not None or case.find("error") is not None:
			counts["failed"] += 1
			(known_seen if name in known else unexpected).append(name)
		elif case.find("skipped") is not None:
			counts["skipped"] += 1
		else:
			counts["passed"] += 1
	for key in total:
		total[key] += counts[key]
	print(f"{path.stem:<28} {counts['tests']:>6} {counts['passed']:>7} {counts['failed']:>7} {counts['skipped']:>8}")
print(f"{'total':<28} {total['tests']:>6} {total['passed']:>7} {total['failed']:>7} {total['skipped']:>8}")

missing = [module for module in expected_modules if module not in reported]
for name in known_seen:
	print(f"known failure: {name}")
for name in unexpected:
	print(f"UNEXPECTED FAILURE: {name}")
for module in missing:
	print(f"MISSING REPORT: {module}")
sys.exit(1 if unexpected or missing else 0)
