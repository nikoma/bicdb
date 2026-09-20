// Encrypted chunked attachment store: media (videos, transcripts, images)
// lives OUTSIDE the database as AES-GCM-encrypted chunked OPFS files, with
// a manifest row inside the database so metadata syncs like everything
// else. Blobs themselves are local-only cache: on a manifest row whose blob
// is missing (new device, evicted cache), the app re-fetches from the URL
// in the manifest metadata and calls put() again.
//
// File format (one OPFS file per attachment, dir `bicdb-<db>-attachments`):
//   "BATT1" magic, then per chunk: u32-le ciphertext length, 12-byte IV,
//   AES-GCM ciphertext (plaintext chunk <= 1 MiB). Per-chunk IVs keep
//   memory flat for large media and let a future range-read seek by chunk.

const MAGIC = new TextEncoder().encode("BATT1");
const CHUNK_BYTES = 1024 * 1024;
const MANIFEST_TABLE = "_attachments";

function assertId(id) {
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(id)) {
    throw new Error(`invalid attachment id: ${id}`);
  }
}

export class AttachmentStore {
  #client;
  #database;
  #key;
  #manifestReady = null;

  constructor(client, { database, key }) {
    this.#client = client;
    this.#database = database;
    this.#key = key; // CryptoKey (AES-GCM)
  }

  // rawKeyHex: 64 hex chars (32 bytes) — typically the same
  // WebCrypto-unwrapped key material the database itself uses.
  static async open(client, { database, rawKeyHex }) {
    const bytes = Uint8Array.from(
      rawKeyHex.match(/.{2}/g).map((pair) => parseInt(pair, 16)),
    );
    const key = await crypto.subtle.importKey(
      "raw",
      bytes,
      { name: "AES-GCM" },
      false,
      ["encrypt", "decrypt"],
    );
    return new AttachmentStore(client, { database, key });
  }

  async #dir(create = true) {
    const root = await navigator.storage.getDirectory();
    return root.getDirectoryHandle(`bicdb-${this.#database}-attachments`, {
      create,
    });
  }

  #ensureManifest() {
    if (this.#manifestReady === null) {
      this.#manifestReady = this.#client
        .query(
          `CREATE TABLE IF NOT EXISTS ${MANIFEST_TABLE} ` +
            `(id TEXT PRIMARY KEY, size BIGINT, chunks INT, created_at BIGINT, meta TEXT)`,
        )
        .catch(() => {}); // exists already (older engines without IF NOT EXISTS)
    }
    return this.#manifestReady;
  }

  // data: Uint8Array | Blob. meta: JSON-serializable (e.g. {url, mime}).
  async put(id, data, meta = {}) {
    assertId(id);
    const bytes =
      data instanceof Uint8Array ? data : new Uint8Array(await data.arrayBuffer());

    const dir = await this.#dir();
    const file = await dir.getFileHandle(id, { create: true });
    const writable = await file.createWritable();
    await writable.write(MAGIC);
    let chunks = 0;
    for (let at = 0; at < bytes.length || chunks === 0; at += CHUNK_BYTES) {
      const chunk = bytes.subarray(at, Math.min(at + CHUNK_BYTES, bytes.length));
      const iv = crypto.getRandomValues(new Uint8Array(12));
      const sealed = new Uint8Array(
        await crypto.subtle.encrypt({ name: "AES-GCM", iv }, this.#key, chunk),
      );
      const header = new Uint8Array(4);
      new DataView(header.buffer).setUint32(0, sealed.length, true);
      await writable.write(header);
      await writable.write(iv);
      await writable.write(sealed);
      chunks += 1;
    }
    await writable.close();

    await this.#ensureManifest();
    await this.#client.query(`DELETE FROM ${MANIFEST_TABLE} WHERE id = '${id}'`);
    await this.#client.query(
      `INSERT INTO ${MANIFEST_TABLE} (id, size, chunks, created_at, meta) VALUES ` +
        `('${id}', ${bytes.length}, ${chunks}, ${Date.now()}, ` +
        `'${JSON.stringify(meta).replaceAll("'", "''")}')`,
    );
    return { id, size: bytes.length, chunks };
  }

  async get(id) {
    assertId(id);
    const dir = await this.#dir(false);
    const handle = await dir.getFileHandle(id);
    const raw = new Uint8Array(await (await handle.getFile()).arrayBuffer());
    if (raw.length < MAGIC.length || !MAGIC.every((b, i) => raw[i] === b)) {
      throw new Error(`attachment ${id} is not a BATT1 file`);
    }
    const parts = [];
    let at = MAGIC.length;
    while (at < raw.length) {
      const len = new DataView(raw.buffer, raw.byteOffset + at).getUint32(0, true);
      const iv = raw.subarray(at + 4, at + 16);
      const sealed = raw.subarray(at + 16, at + 16 + len);
      if (sealed.length !== len) throw new Error(`attachment ${id} is truncated`);
      parts.push(
        new Uint8Array(
          await crypto.subtle.decrypt({ name: "AES-GCM", iv }, this.#key, sealed),
        ),
      );
      at += 16 + len;
    }
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    const out = new Uint8Array(total);
    let cursor = 0;
    for (const part of parts) {
      out.set(part, cursor);
      cursor += part.length;
    }
    return out;
  }

  // Does the encrypted blob exist locally? (The manifest row may sync in
  // before the bytes do — that's the re-fetch-on-miss signal.)
  async has(id) {
    assertId(id);
    try {
      const dir = await this.#dir(false);
      await dir.getFileHandle(id);
      return true;
    } catch {
      return false;
    }
  }

  async delete(id) {
    assertId(id);
    try {
      const dir = await this.#dir(false);
      await dir.removeEntry(id);
    } catch {
      /* blob already gone */
    }
    await this.#ensureManifest();
    await this.#client.query(`DELETE FROM ${MANIFEST_TABLE} WHERE id = '${id}'`);
  }

  // Local bytes used by attachment blobs (feeds cache-pressure decisions).
  async usageBytes() {
    let total = 0;
    try {
      const dir = await this.#dir(false);
      for await (const [, handle] of dir) {
        if (handle.kind === "file") total += (await handle.getFile()).size;
      }
    } catch {
      /* no attachments dir yet */
    }
    return total;
  }
}
