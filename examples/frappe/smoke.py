#!/usr/bin/env python3
"""HTTP smoke test for a Frappe site served from a BicDB database.

Logs in as Administrator, round-trips ToDo documents, creates a custom
DocType through the REST API (CREATE TABLE), adds a field to it (ALTER TABLE),
and exercises create/read/update/delete, filtered list queries and counts.

Usage: smoke.py http://127.0.0.1:8000 [admin-password]
"""

import http.cookiejar
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
PASSWORD = sys.argv[2] if len(sys.argv) > 2 else "admin"
# A fresh name per run: deleting a DocType keeps its table in Frappe.
DOCTYPE = f"BicDB Smoke {int(time.time() * 1000) % 100000000}"
DELETED = (200, 202)  # 202 Accepted in Frappe 16

opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def call(method, path, body=None, expect=200):
	data = None
	headers = {"Accept": "application/json"}
	if body is not None:
		data = json.dumps(body).encode()
		headers["Content-Type"] = "application/json"
	request = urllib.request.Request(BASE + path, data=data, method=method, headers=headers)
	try:
		with opener.open(request, timeout=120) as response:
			status, payload = response.status, response.read()
	except urllib.error.HTTPError as error:
		status, payload = error.code, error.read()
	if status not in (expect if isinstance(expect, tuple) else (expect,)):
		raise SystemExit(f"{method} {path} -> HTTP {status} (expected {expect})\n{payload.decode(errors='replace')[:4000]}")
	return json.loads(payload) if payload else None


def resource(doctype, name=None, query=None):
	path = "/api/resource/" + urllib.parse.quote(doctype)
	if name is not None:
		path += "/" + urllib.parse.quote(str(name))
	if query:
		path += "?" + urllib.parse.urlencode({k: v if isinstance(v, str) else json.dumps(v) for k, v in query.items()})
	return path


def check(condition, message):
	if not condition:
		raise SystemExit("smoke check failed: " + message)
	print("ok -", message)


started = time.monotonic()
call("POST", "/api/method/login", {"usr": "Administrator", "pwd": PASSWORD})
me = call("GET", "/api/method/frappe.auth.get_logged_user")["message"]
check(me == "Administrator", "logged in as Administrator")

# ToDo CRUD
todo = call("POST", resource("ToDo"), {"description": "BicDB smoke todo", "priority": "High"})["data"]
check(todo["status"] == "Open", "ToDo created")
read = call("GET", resource("ToDo", todo["name"]))["data"]
check(read["description"] == "BicDB smoke todo", "ToDo read back")
updated = call("PUT", resource("ToDo", todo["name"]), {"status": "Closed"})["data"]
check(updated["status"] == "Closed", "ToDo updated")
listed = call("GET", resource("ToDo", query={"filters": [["status", "=", "Closed"]], "fields": ["name", "status"]}))["data"]
check(any(row["name"] == todo["name"] for row in listed), "ToDo listed with a filter")
call("DELETE", resource("ToDo", todo["name"]), expect=DELETED)
call("GET", resource("ToDo", todo["name"]), expect=404)
check(True, "ToDo deleted")

# Custom DocType through the API (DDL)
call(
	"POST",
	resource("DocType"),
	{
		"name": DOCTYPE,
		"module": "Custom",
		"custom": 1,
		"autoname": "hash",
		"fields": [
			{"fieldname": "title", "fieldtype": "Data", "label": "Title", "reqd": 1, "in_list_view": 1},
			{"fieldname": "qty", "fieldtype": "Int", "label": "Qty"},
			{"fieldname": "amount", "fieldtype": "Currency", "label": "Amount"},
			{"fieldname": "due", "fieldtype": "Date", "label": "Due"},
			{"fieldname": "done", "fieldtype": "Check", "label": "Done"},
		],
		"permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}],
	},
)
check(True, "custom DocType created (CREATE TABLE)")

doctype = call("GET", resource("DocType", DOCTYPE))["data"]
doctype["fields"].append({"fieldname": "notes", "fieldtype": "Small Text", "label": "Notes"})
call("PUT", resource("DocType", DOCTYPE), {"fields": doctype["fields"]})
check(True, "field added to the custom DocType (ALTER TABLE ADD COLUMN)")

names = []
for i in range(1, 6):
	doc = call(
		"POST",
		resource(DOCTYPE),
		{"title": f"item {i}", "qty": i, "amount": i * 1.5, "due": f"2026-01-0{i}", "done": i % 2, "notes": f"note {i}"},
	)["data"]
	names.append(doc["name"])
check(len(names) == 5, "five custom documents inserted")

doc = call("GET", resource(DOCTYPE, names[2]))["data"]
check(doc["qty"] == 3 and float(doc["amount"]) == 4.5 and doc["notes"] == "note 3", "custom document read back")
call("PUT", resource(DOCTYPE, names[2]), {"qty": 30, "title": "item three"})
doc = call("GET", resource(DOCTYPE, names[2]))["data"]
check(doc["qty"] == 30 and doc["title"] == "item three", "custom document updated")

rows = call(
	"GET",
	resource(
		DOCTYPE,
		query={"filters": [["qty", ">", 2], ["done", "=", 1]], "fields": ["name", "qty"], "order_by": "qty desc"},
	),
)["data"]
check([row["qty"] for row in rows] == [30, 5], f"filtered, ordered list query {rows}")
count = call("GET", "/api/method/frappe.client.get_count?" + urllib.parse.urlencode({"doctype": DOCTYPE}))["message"]
check(count == 5, "count query")
like = call("GET", resource(DOCTYPE, query={"filters": [["title", "like", "%three%"]], "fields": ["name"]}))["data"]
check([row["name"] for row in like] == [names[2]], "LIKE filter")

for name in names:
	call("DELETE", resource(DOCTYPE, name), expect=DELETED)
count = call("GET", "/api/method/frappe.client.get_count?" + urllib.parse.urlencode({"doctype": DOCTYPE}))["message"]
check(count == 0, "custom documents deleted")
call("DELETE", resource("DocType", DOCTYPE), expect=DELETED)
check(True, "custom DocType deleted")
print(f"smoke passed in {time.monotonic() - started:.1f}s")
