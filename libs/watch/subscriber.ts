export type WatchFilter =
  | { type: "exact"; key: string }
  | { type: "prefix"; prefix: string }
  | { type: "suffix"; suffix: string }
  | { type: "contains"; substring: string }
  | { type: "all" };

export type WatchEvent = {
  key: string;
  value: string | null;
  rev?: number;
  createdAt?: number;
  leaseId?: number;
  leaseExpiresAt?: number;
};

export class Subscriber {
  id: number;
  filter: WatchFilter;
  once: boolean;
  ch: ((ev: WatchEvent) => void) | null;
  closed = false;
  // last seen revision (optional) used for resume-from-revision semantics
  lastSeenRev?: number;
  constructor(id: number, filter: WatchFilter, once = false) {
    this.id = id;
    this.filter = filter;
    this.once = once;
    this.ch = null;
  }

  onEvent(fn: (ev: WatchEvent) => void) {
    this.ch = fn;
  }

  // Set or update the resume revision for this subscriber. Useful for durable resume.
  setLastSeenRev(rev?: number) {
    if (typeof rev === "number") this.lastSeenRev = rev;
    else this.lastSeenRev = undefined;
  }

  getLastSeenRev(): number | undefined {
    return this.lastSeenRev;
  }

  // Return a small JSON-serializable snapshot suitable for durable storage.
  toSnapshot() {
    const out: any = { id: this.id, filter: this.filter, once: this.once };
    if (typeof this.lastSeenRev === "number")
      out.lastSeenRev = this.lastSeenRev;
    return out;
  }

  // Apply a snapshot onto an existing Subscriber instance. Only lastSeenRev is applied.
  static restoreSnapshot(sub: Subscriber, snap: any) {
    try {
      if (!sub || !snap) return;
      if (typeof snap.lastSeenRev === "number")
        sub.setLastSeenRev(snap.lastSeenRev);
    } catch {}
  }

  close() {
    this.closed = true;
    this.ch = null;
  }

  match(key: string) {
    const f = this.filter;
    if (f.type === "all") return true;
    if (f.type === "exact") return key === f.key;
    if (f.type === "prefix") return key.startsWith(f.prefix);
    if (f.type === "suffix") return key.endsWith(f.suffix);
    if (f.type === "contains") return key.indexOf(f.substring) !== -1;
    return false;
  }
}
