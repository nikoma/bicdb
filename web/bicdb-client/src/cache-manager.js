// Cache-pressure guardian: watches the database's OPFS footprint against a
// byte budget and escalates — idle-time compaction first, then a
// "cache-full" signal the app resolves (typically by shrinking the working
// set server-side, or resetting the cache: close → BicDbClient.destroy →
// reopen → sync re-bootstraps everything from the server).
//
// Why there is no per-collection LRU eviction here: the cache is an
// event-sourced sync replica. Locally dropping one collection's records
// would (a) leave its full history in the event log (little space won), and
// (b) get resurrected by the next import's audit reconciliation. Bounding
// therefore happens where the data is composed — the server's working-set
// scope plus its retention rules — and whole-cache reset is the local
// escape hatch. See docs/browser-cache-hygiene.md.

const IDLE = (fn) =>
  typeof requestIdleCallback === "function"
    ? requestIdleCallback(fn, { timeout: 10_000 })
    : setTimeout(fn, 250);

export class CacheManager {
  #client;
  #onState;
  #telemetry;
  #timer = null;
  #maxBytes;
  #softRatio;
  #checkIntervalMs;
  #checking = null;
  #lastState = "ok";

  constructor(
    client,
    {
      maxBytes = 200 * 1024 * 1024, // the bounded-working-set target
      softRatio = 0.7,              // compact above soft, report full above max
      checkIntervalMs = 60_000,
      onState = () => {},
      telemetry = null,
    } = {},
  ) {
    this.#client = client;
    this.#maxBytes = maxBytes;
    this.#softRatio = softRatio;
    this.#checkIntervalMs = checkIntervalMs;
    this.#onState = onState;
    this.#telemetry = telemetry;
  }

  start() {
    if (this.#timer !== null) return;
    const loop = () => {
      this.#timer = setTimeout(() => IDLE(async () => {
        try {
          await this.check();
        } catch {
          /* worker closed mid-check */
        }
        if (this.#timer !== null) loop();
      }), this.#checkIntervalMs);
    };
    this.#timer = setTimeout(() => IDLE(async () => {
      try {
        await this.check();
      } catch {
        /* worker closed mid-check */
      }
      if (this.#timer !== null) loop();
    }), 0);
  }

  stop() {
    clearTimeout(this.#timer);
    this.#timer = null;
  }

  // One pressure evaluation; safe to call directly (the smoke test does).
  // Returns {state, usageBytes, quota, compacted}.
  async check() {
    if (this.#checking === null) {
      this.#checking = this.#checkInner().finally(() => {
        this.#checking = null;
      });
    }
    return this.#checking;
  }

  async #checkInner() {
    let { pool, quota } = await this.#client.stats();
    let usage = pool.poolBytes;
    let compacted = false;

    if (usage > this.#maxBytes * this.#softRatio) {
      // Reclaimable space lives in two places: superseded event history
      // (horizon trim) and dead rows in the record log (compact).
      await this.#client.trimEvents();
      await this.#client.compact();
      compacted = true;
      ({ pool, quota } = await this.#client.stats());
      usage = pool.poolBytes;
    }

    const state = usage > this.#maxBytes ? "cache-full" : "ok";
    const snapshot = { state, usageBytes: usage, quota, compacted };
    this.#telemetry?.record("cache-pressure", {
      usageBytes: usage,
      compacted,
      state,
      quotaUsage: quota?.usageBytes ?? null,
      quotaTotal: quota?.quotaBytes ?? null,
    });
    if (state !== this.#lastState || state === "cache-full") {
      this.#lastState = state;
      this.#onState(snapshot);
    }
    return snapshot;
  }
}
