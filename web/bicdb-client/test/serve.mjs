// Tiny static server for the smoke test: serves this package at / and the
// compiled wasm module at /wasm/bicdb_wasm.wasm. localhost is a secure
// context, so OPFS works without TLS.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { join, resolve, extname } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const wasmPath = resolve(
  pkgRoot,
  "../../target/wasm32-wasip1/release/bicdb_wasm.wasm",
);

const TYPES = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

export function serve(port = 0) {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, "http://localhost");
      let file;
      if (url.pathname === "/wasm/bicdb_wasm.wasm") {
        file = wasmPath;
      } else {
        const rel = url.pathname.replace(/^\/+/, "") || "test/smoke.html";
        file = join(pkgRoot, rel);
        if (!resolve(file).startsWith(pkgRoot)) throw new Error("traversal");
      }
      const body = await readFile(file);
      res.writeHead(200, {
        "content-type": TYPES[extname(file)] ?? "application/octet-stream",
      });
      res.end(body);
    } catch {
      res.writeHead(404);
      res.end("not found");
    }
  });
  return new Promise((ready) =>
    server.listen(port, "127.0.0.1", () =>
      ready({ server, port: server.address().port }),
    ),
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { port } = await serve(8788);
  console.log(`http://127.0.0.1:${port}/test/smoke.html`);
}
