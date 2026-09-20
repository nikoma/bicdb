// Client telemetry: buffered events posted to the sync server's
// per-scope JSONL log (POST /v1/<scope>/telemetry). Feed it sync reports,
// cache-pressure snapshots, and cold-open timings — the numbers the
// operator needs to see cache health per user without a dashboard stack.

export class TelemetryReporter {
  #url;
  #scope;
  #token;
  #buffer = [];
  #maxBuffer;
  #timer = null;
  #flushIntervalMs;

  constructor({ url, scope, token = null, flushIntervalMs = 60_000, maxBuffer = 200 }) {
    if (!url || !scope) throw new Error("url and scope are required");
    this.#url = String(url).replace(/\/+$/, "");
    this.#scope = scope;
    this.#token = token;
    this.#flushIntervalMs = flushIntervalMs;
    this.#maxBuffer = maxBuffer;
  }

  record(type, fields = {}) {
    this.#buffer.push({ type, at: Date.now(), ...fields });
    if (this.#buffer.length > this.#maxBuffer) {
      // Oldest-first drop keeps the reporter O(1) memory when offline.
      this.#buffer.splice(0, this.#buffer.length - this.#maxBuffer);
    }
  }

  async flush() {
    if (this.#buffer.length === 0) return { recorded: 0 };
    const events = this.#buffer.splice(0);
    const headers = { "content-type": "application/json" };
    if (this.#token) headers.authorization = `Bearer ${this.#token}`;
    try {
      const response = await fetch(`${this.#url}/v1/${this.#scope}/telemetry`, {
        method: "POST",
        headers,
        body: JSON.stringify({ events }),
      });
      if (!response.ok) throw new Error(`telemetry returned ${response.status}`);
      return await response.json();
    } catch (error) {
      // Put them back (bounded) and retry on the next flush.
      this.#buffer.unshift(...events.slice(-this.#maxBuffer));
      throw error;
    }
  }

  start() {
    if (this.#timer !== null) return;
    this.#timer = setInterval(() => {
      this.flush().catch(() => {});
    }, this.#flushIntervalMs);
  }

  stop() {
    clearInterval(this.#timer);
    this.#timer = null;
  }
}
