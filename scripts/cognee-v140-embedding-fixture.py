#!/usr/bin/env python3
"""Deterministic OpenAI-compatible embeddings for the Cognee v1.4.0 gate."""

from __future__ import annotations

import argparse
import json
import math
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def embedding(value: str) -> list[float]:
    text = value.lower()
    if "quantum" in text:
        return [1.0, 0.0, 0.0]
    if "machine" in text or "learning" in text:
        return [0.0, 1.0, 0.0]
    if "neural" in text:
        return [0.0, 0.0, 1.0]
    component = 1.0 / math.sqrt(3.0)
    return [component, component, component]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        inputs = payload.get("input", [])
        if isinstance(inputs, str):
            inputs = [inputs]
        response = {
            "object": "list",
            "model": payload.get("model", "local-deterministic"),
            "data": [
                {"object": "embedding", "index": index, "embedding": embedding(value)}
                for index, value in enumerate(inputs)
            ],
            "usage": {"prompt_tokens": 0, "total_tokens": 0},
        }
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
