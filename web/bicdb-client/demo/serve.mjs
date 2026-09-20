// Demo server for the Chaos Lab: serves this package + the wasm module.
//
// OPFS requires a secure context, so remote access must be HTTPS — a
// self-signed cert is generated on first run (accept the browser warning).
// Plain HTTP is also served for localhost / ssh -L use.
//
// The wasm module (the size-optimized build when present) is pre-gzipped
// at startup and served with Content-Encoding: gzip — ~2.5 MB on the wire
// instead of 18 MB — plus an x-decompressed-length header so the client
// can show real download progress. Every request is logged to stdout.
//
//   node demo/serve.mjs            # https :8443, http :8788
import { createServer as httpServer, request as httpRequest } from "node:http";
import { createServer as httpsServer } from "node:https";
import { execSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { join, resolve, extname } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const wasmDir = resolve(pkgRoot, "../../target/wasm32-wasip1/release");
const certDir = join(pkgRoot, "demo", ".cert");

const TYPES = {
  ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript",
  ".wasm": "application/wasm", ".json": "application/json", ".css": "text/css",
};

// Prefer the size-optimized module (panic=abort + opt-z + wasm-opt).
const wasmPath = existsSync(join(wasmDir, "bicdb_wasm.opt.wasm"))
  ? join(wasmDir, "bicdb_wasm.opt.wasm")
  : join(wasmDir, "bicdb_wasm.wasm");
const wasmRaw = readFileSync(wasmPath);
const wasmGz = gzipSync(wasmRaw, { level: 9 });
console.log(
  `wasm: ${wasmPath.split("/").pop()} ${(wasmRaw.length / 1048576).toFixed(1)}MB ` +
    `(${(wasmGz.length / 1048576).toFixed(1)}MB gzipped on the wire)`,
);

// The page is https (OPFS needs a secure context) but `bicdb sync-serve`
// speaks plain http on localhost; proxying /sync/* keeps everything
// same-origin and avoids mixed-content blocking. Start the sync server
// with: bicdb sync-serve <root> --host 127.0.0.1 --port $SYNC_PORT
const SYNC_PORT = Number(process.env.SYNC_PORT ?? 8787);
const BICUI_SYNC_PORT = Number(process.env.BICUI_SYNC_PORT ?? 8791);
const BICUI_API_PORT = Number(process.env.BICUI_API_PORT ?? 8790);
const BICUI_EHR_SYNC_PORT = Number(process.env.BICUI_EHR_SYNC_PORT ?? 8793);
const BICUI_EHR_API_PORT = Number(process.env.BICUI_EHR_API_PORT ?? 8792);
const BICUI_GEN_SYNC_PORT = Number(process.env.BICUI_GEN_SYNC_PORT ?? 8795);
const BICUI_GEN_API_PORT = Number(process.env.BICUI_GEN_API_PORT ?? 8794);
function proxySync(req, res, url, port = SYNC_PORT, prefix = "/sync") {
  const upstream = httpRequest(
    {
      host: "127.0.0.1",
      port,
      method: req.method,
      path: url.pathname.slice(prefix.length) + url.search,
      headers: {
        "content-type": req.headers["content-type"] ?? "application/json",
        ...(req.headers.authorization
          ? { authorization: req.headers.authorization }
          : {}),
        ...(req.headers["content-length"]
          ? { "content-length": req.headers["content-length"] }
          : {}),
      },
    },
    (up) => {
      res.writeHead(up.statusCode ?? 502, { "content-type": "application/json" });
      up.pipe(res);
    },
  );
  upstream.on("error", () => {
    res.writeHead(502, { "content-type": "application/json" });
    res.end('{"error":"sync server is not running on this host"}');
  });
  req.pipe(upstream);
}

async function handle(req, res) {
  const started = Date.now();
  let status = 200, sent = 0;
  try {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname.startsWith("/sync/")) {
      proxySync(req, res, url);
      console.log(`${new Date().toISOString()} ${req.socket.remoteAddress} ${req.method} ${req.url} -> sync`);
      return;
    }
    // BicUI demo backends (see /root/bicui/demo/control-server.mjs).
    if (url.pathname.startsWith("/bicui-sync/")) {
      proxySync(req, res, url, BICUI_SYNC_PORT, "/bicui-sync");
      return;
    }
    if (url.pathname.startsWith("/bicui-api/")) {
      proxySync(req, res, url, BICUI_API_PORT, "/bicui-api");
      return;
    }
    // Walknorth EHR instance (see /root/bicui/demo/control-server-ehr.mjs).
    if (url.pathname.startsWith("/bicui-ehr-sync/")) {
      proxySync(req, res, url, BICUI_EHR_SYNC_PORT, "/bicui-ehr-sync");
      return;
    }
    if (url.pathname.startsWith("/bicui-ehr-api/")) {
      proxySync(req, res, url, BICUI_EHR_API_PORT, "/bicui-ehr-api");
      return;
    }
    // Annotation-generated ClinicOps (bicui demo/control-server-generated.mjs).
    if (url.pathname.startsWith("/bicui-gen-sync/")) {
      proxySync(req, res, url, BICUI_GEN_SYNC_PORT, "/bicui-gen-sync");
      return;
    }
    if (url.pathname.startsWith("/bicui-gen-api/")) {
      proxySync(req, res, url, BICUI_GEN_API_PORT, "/bicui-gen-api");
      return;
    }
    // Sibling BicUI repo (Phase 0 preview + test vectors), when present.
    if (url.pathname.startsWith("/bicui/")) {
      const bicuiRoot = resolve(pkgRoot, "../../../bicui");
      const rel = url.pathname.slice("/bicui/".length);
      const file = join(bicuiRoot, rel);
      if (!resolve(file).startsWith(bicuiRoot)) throw new Error("traversal");
      const body = await readFile(file);
      sent = body.length;
      res.writeHead(200, {
        "content-type": TYPES[extname(file)] ?? "application/octet-stream",
        "cache-control": "no-cache",
      });
      res.end(body);
      return;
    }
    if (url.pathname === "/wasm/bicdb_wasm.wasm") {
      const acceptsGzip = /\bgzip\b/.test(req.headers["accept-encoding"] ?? "");
      const body = acceptsGzip ? wasmGz : wasmRaw;
      sent = body.length;
      res.writeHead(200, {
        "content-type": "application/wasm",
        "content-length": body.length,
        ...(acceptsGzip ? { "content-encoding": "gzip" } : {}),
        "x-decompressed-length": wasmRaw.length,
        "access-control-expose-headers": "x-decompressed-length",
        "cache-control": "no-cache",
      });
      res.end(body);
      return;
    }
    const rel = url.pathname.replace(/^\/+/, "") || "demo/crazy.html";
    const file = join(pkgRoot, rel);
    if (!resolve(file).startsWith(pkgRoot)) throw new Error("traversal");
    const body = await readFile(file);
    sent = body.length;
    res.writeHead(200, {
      "content-type": TYPES[extname(file)] ?? "application/octet-stream",
      "cache-control": "no-cache",
    });
    res.end(body);
  } catch {
    status = 404;
    res.writeHead(404);
    res.end("not found");
  } finally {
    console.log(
      `${new Date().toISOString()} ${req.socket.remoteAddress} ${req.method} ` +
        `${req.url} ${status} ${sent}B ${Date.now() - started}ms`,
    );
  }
}

function ensureCert() {
  const key = join(certDir, "key.pem");
  const cert = join(certDir, "cert.pem");
  if (!existsSync(key) || !existsSync(cert)) {
    mkdirSync(certDir, { recursive: true });
    execSync(
      `openssl req -x509 -newkey rsa:2048 -keyout ${key} -out ${cert} ` +
        `-days 30 -nodes -subj "/CN=bicdb-chaos-lab" ` +
        `-addext "subjectAltName=DNS:localhost,IP:127.0.0.1"`,
      { stdio: "ignore" },
    );
  }
  return { key: readFileSync(key), cert: readFileSync(cert) };
}

const HTTP_PORT = Number(process.env.HTTP_PORT ?? 8788);
const HTTPS_PORT = Number(process.env.HTTPS_PORT ?? 8443);

httpServer(handle).listen(HTTP_PORT, "0.0.0.0", () =>
  console.log(`http  : http://localhost:${HTTP_PORT}/  (secure context on localhost only)`),
);
httpsServer(ensureCert(), handle).listen(HTTPS_PORT, "0.0.0.0", () =>
  console.log(`https : https://<this-host>:${HTTPS_PORT}/  (self-signed — accept the warning)`),
);
