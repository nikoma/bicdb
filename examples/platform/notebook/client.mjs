// Storage is injected so queue behavior can be checked without a browser.
export class Notebook {
  constructor(store, transport) { this.store = store; this.transport = transport; }
  async add(note) { await this.store.enqueue(note); }
  async flush() {
    for (const note of await this.store.pending()) {
      await this.transport.append(note);
      // Delete the pending record only after a successful acknowledgement.
      // A lost response is safe: the next attempt uses the same ID and value.
      await this.store.acknowledge(note.id);
    }
  }
  async page(cursor) {
    const page = await this.transport.page(cursor);
    await this.store.merge(page.notes);
    return page.next;
  }
}
