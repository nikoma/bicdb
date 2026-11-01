import { Controller } from "@hotwired/stimulus"

// A fast job can broadcast before a redirected page subscribes to Cable.
// Reconcile queued reports with persisted state until an update arrives.
export default class extends Controller {
  static values = { url: String }

  connect() {
    this.abortController = new AbortController()
    this.timer = setTimeout(() => this.refresh(), 1000)
  }

  disconnect() {
    clearTimeout(this.timer)
    this.abortController.abort()
  }

  async refresh() {
    try {
      const response = await fetch(this.urlValue, {
        headers: { Accept: "text/html" },
        cache: "no-store",
        signal: this.abortController.signal
      })
      if (!response.ok) throw new Error("Report status unavailable")
      const html = await response.text()
      if (this.element.isConnected) this.element.outerHTML = html
    } catch (error) {
      if (error.name !== "AbortError" && this.element.isConnected) {
        this.timer = setTimeout(() => this.refresh(), 3000)
      }
    }
  }
}
