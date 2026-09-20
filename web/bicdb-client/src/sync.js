// HTTP sync for the browser cache: the async mirror of bicdb-sync's
// SyncCoordinator, with the server reached over fetch.
//
// One syncOnce() run:
//   1. load this node's checkpoint from the server
//   2. export local events since checkpoint.local_export; push if non-empty
//   3. pull server bundles since checkpoint.remote_imports[server]; import
//   4. re-export to advance local_export past what we just imported
//      (so imported events don't echo back on the next push)
//   5. save the checkpoint
//
// Conflicts resolve inside the engine: events dedup by id, records resolve
// last-writer-wins on (timestamp, sequence, node_id) — see
// docs/browser-sync.md for the full policy.

const CHECKPOINT_FORMAT_VERSION = 1;

export class HttpSyncEndpoint {
  #base;
  #scope;
  #token;

  constructor({ url, scope, token = null }) {
    if (!url || !scope) throw new Error("url and scope are required");
    this.#base = String(url).replace(/\/+$/, "");
    this.#scope = scope;
    this.#token = token;
  }

  async #call(method, path, body) {
    const headers = { "content-type": "application/json" };
    if (this.#token) headers.authorization = `Bearer ${this.#token}`;
    // Pre-serialized strings (hash-verified bundles) are sent verbatim;
    // re-encoding them would change float formatting and break the hashes.
    const encoded =
      body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body);
    const response = await fetch(`${this.#base}/v1/${this.#scope}${path}`, {
      method,
      headers,
      body: encoded,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error ?? `sync server returned ${response.status}`);
    }
    return payload;
  }

  loadCheckpoint(nodeId) {
    return this.#call("GET", `/checkpoint/${nodeId}`);
  }

  saveCheckpoint(nodeId, checkpoint) {
    return this.#call("PUT", `/checkpoint/${nodeId}`, checkpoint);
  }

  // bundleJson: the engine's exact serialized bundle (opaque string).
  pushBundle(bundleJson) {
    return this.#call("POST", "/push", bundleJson);
  }

  // -> {server_node_id, bundles: [string]} — each bundle an opaque string.
  pullBundles(nodeId, checkpoint) {
    return this.#call("POST", "/pull", { node_id: nodeId, checkpoint });
  }
}

function recordLocalExport(checkpoint, next) {
  if (next.event_offset > checkpoint.local_export.event_offset) {
    checkpoint.local_export = next;
  }
}

function recordRemoteImport(checkpoint, nodeId, next) {
  const current = checkpoint.remote_imports[nodeId] ?? { event_offset: 0 };
  if (next.event_offset > current.event_offset) {
    checkpoint.remote_imports[nodeId] = next;
  }
}

export class SyncManager {
  #client;
  #endpoint;
  #timer = null;
  #intervalMs;
  #backoffMs;
  #running = false;
  #inFlight = null;
  #onReport;
  #onError;
  #onlineListener = null;

  constructor(client, endpoint, { intervalMs = 30_000, onReport, onError } = {}) {
    this.#client = client;
    this.#endpoint = endpoint;
    this.#intervalMs = intervalMs;
    this.#backoffMs = intervalMs;
    this.#onReport = onReport ?? (() => {});
    this.#onError = onError ?? (() => {});
  }

  // One full round trip. Concurrent callers share the in-flight run —
  // the checkpoint protocol is not safe to interleave per node.
  syncOnce() {
    if (this.#inFlight === null) {
      this.#inFlight = this.#syncOnceInner().finally(() => {
        this.#inFlight = null;
      });
    }
    return this.#inFlight;
  }

  async #syncOnceInner() {
    // Serialize against compact(): compaction rewrites event offsets and
    // resets the engine's export watermark, which must not happen between
    // an export and its mark-exported.
    return this.#client.runExclusive(() => this.#syncOnceLocked());
  }

  async #syncOnceLocked() {
    const nodeId = await this.#client.nodeId();
    const checkpoint = await this.#endpoint.loadCheckpoint(nodeId);
    if (checkpoint.format_version !== CHECKPOINT_FORMAT_VERSION) {
      throw new Error(
        `unsupported sync checkpoint version ${checkpoint.format_version}`,
      );
    }
    const report = {
      nodeId,
      pushedEvents: 0,
      pulledBundles: 0,
      importedEvents: 0,
      duplicateEvents: 0,
      recordsMerged: 0,
      conflictsResolved: 0,
    };

    // Push side is driven by the ENGINE's export watermark
    // (sync_status().last_export_checkpoint), not the server-stored
    // checkpoint: the engine resets it during compaction together with the
    // event-offset rewrite, so this path self-heals where an external
    // offset would silently strand post-compact writes. Re-exports after a
    // reset are absorbed by event-id dedup on the server.
    const local = await this.#client.syncExportPending();
    if (local.eventCount > 0) {
      const push = await this.#endpoint.pushBundle(local.bundleJson);
      await this.#client.syncMarkExported(push.next_checkpoint, push.event_count);
      recordLocalExport(checkpoint, push.next_checkpoint); // informational
      report.pushedEvents = push.event_count;
    }

    const importedIds = new Set();
    const pulled = await this.#endpoint.pullBundles(nodeId, checkpoint);
    for (const bundleJson of pulled.bundles) {
      // Parsing for bookkeeping is fine; only the exact original string is
      // ever handed to the engine (payload hashes must survive the hop).
      const meta = JSON.parse(bundleJson);
      if (meta.event_count > 0) {
        const imported = await this.#client.syncImport(bundleJson);
        for (const entry of meta.events) {
          importedIds.add(entry.envelope.event_id);
        }
        report.importedEvents += imported.imported_events;
        report.duplicateEvents += imported.duplicate_events;
        report.recordsMerged += imported.records_merged;
        report.conflictsResolved += imported.conflicts_resolved;
      }
      report.pulledBundles += 1;
      recordRemoteImport(checkpoint, meta.source_node_id, meta.next_checkpoint);
    }

    // Advance the watermark past what we just imported so it doesn't echo
    // back on the next push — but only if the tail contains nothing else.
    // If a local write raced in during the pulls, skip the advance: the
    // next push re-sends the imports too (server dedups), which is
    // harmless, whereas marking past an unpushed local write would lose it.
    if (importedIds.size > 0) {
      const tail = await this.#client.syncExportPending();
      const onlyImports = tail.eventIds.every((id) => importedIds.has(id));
      if (onlyImports && tail.eventCount > 0) {
        await this.#client.syncMarkExported(tail.nextCheckpoint, tail.eventCount);
        recordLocalExport(checkpoint, tail.nextCheckpoint);
      }
    }

    await this.#endpoint.saveCheckpoint(nodeId, checkpoint);
    return report;
  }

  // Background loop: sync now, then every intervalMs; exponential backoff
  // (capped at 8x) while the server is unreachable; an immediate run when
  // the browser reports connectivity returning.
  start() {
    if (this.#running) return;
    this.#running = true;
    if (typeof addEventListener === "function") {
      this.#onlineListener = () => this.#tick();
      addEventListener("online", this.#onlineListener);
    }
    this.#tick();
  }

  async #tick() {
    if (!this.#running) return;
    clearTimeout(this.#timer);
    try {
      const report = await this.syncOnce();
      this.#backoffMs = this.#intervalMs;
      this.#onReport(report);
    } catch (error) {
      this.#backoffMs = Math.min(this.#backoffMs * 2, this.#intervalMs * 8);
      this.#onError(error);
    }
    if (this.#running) {
      this.#timer = setTimeout(() => this.#tick(), this.#backoffMs);
    }
  }

  stop() {
    this.#running = false;
    clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#onlineListener) {
      removeEventListener("online", this.#onlineListener);
      this.#onlineListener = null;
    }
  }
}
