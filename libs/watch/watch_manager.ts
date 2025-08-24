import { Subscriber } from "./subscriber";
import type { WatchEvent, WatchFilter } from "./subscriber";

export interface WatchBackend {
  add(filter: WatchFilter, once?: boolean): Subscriber;
  remove(sub: Subscriber): void;
  publish(ev: WatchEvent): void;
  replayFromScan(
    scanIterable: AsyncIterable<any>,
    minRev?: number
  ): Promise<void>;
  replayFromWal?(
    wal: any,
    opts?: { minRev?: number; minWalOffset?: number }
  ): Promise<void>;
  // export persisted subscriber snapshots (JSON-serializable)
  exportSnapshots?(): any[];
  // restore persisted subscriber snapshots into this backend (recreate Subscriber instances)
  restoreSnapshots?(snapshots: any[]): void;
  // lookup a subscriber by id so callers can reattach callbacks after restore
  getSubscriberById?(id: number): Subscriber | null;
}

export class WatchManager implements WatchBackend {
  private subs = new Map<number, Subscriber>();
  private nextSubId = 1;

  // maintain a lightweight list of subscribers; for durable replay we will scan WAL
  // and push events to new subscribers from a given minRev or offset
  add(filter: WatchFilter, once = false) {
    const s = new Subscriber(this.nextSubId++, filter, once);
    this.subs.set(s.id, s);
    return s;
  }

  remove(sub: Subscriber) {
    this.subs.delete(sub.id);
    sub.close();
  }

  // publish an event to matching subscribers; subscribers marked `once` are removed after event
  publish(ev: WatchEvent) {
    const notify: number[] = [];
    for (const s of this.subs.values()) {
      if (s.closed) continue;
      if (s.match(ev.key)) {
        try {
          if (s.ch) s.ch(ev);
        } catch {}
        if (s.once) notify.push(s.id);
      }
    }
    for (const id of notify) {
      const s = this.subs.get(id);
      if (s) this.remove(s);
    }
  }

  // durable replay: scan WAL records starting from `scan` iterable and deliver matching events
  // scanIterable yields objects like { key, value, rev } or { tx: true, ops: [...] }
  // Optional: minRev - when provided, only publish events with rev > minRev
  async replayFromScan(scanIterable: AsyncIterable<any>, minRev?: number) {
    for await (const rec of scanIterable) {
      if (!rec || !rec.value) continue;
      const v = rec.value;
      if (v.tx && Array.isArray(v.ops)) {
        // if the transaction carries tx-level metadata (leaseId/leaseExpiresAt/createdAt),
        // propagate into each op for publishing so watchers see the richer payload.
        const txLeaseId = typeof v.leaseId === "number" ? v.leaseId : undefined;
        const txLeaseExpiresAt =
          typeof v.leaseExpiresAt === "number" ? v.leaseExpiresAt : undefined;
        const txCreatedAt =
          typeof v.createdAt === "number" ? v.createdAt : undefined;
        for (const op of v.ops) {
          if (!op) continue;
          const augmented = Object.assign({}, op);
          if (
            typeof augmented.leaseId !== "number" &&
            typeof txLeaseId === "number"
          )
            augmented.leaseId = txLeaseId;
          if (
            typeof augmented.leaseExpiresAt !== "number" &&
            typeof txLeaseExpiresAt === "number"
          )
            augmented.leaseExpiresAt = txLeaseExpiresAt;
          if (
            typeof augmented.createdAt !== "number" &&
            typeof txCreatedAt === "number"
          )
            augmented.createdAt = txCreatedAt;
          this._maybePublishOp(augmented, minRev);
        }
      } else {
        // per-op entry may be in rec.value
        if (v.key !== undefined) this._maybePublishOp(v, minRev);
      }
    }
  }

  // Replay by scanning WAL with offsets API. Accepts either minRev or minWalOffset.
  // If minRev provided, only events with rev > minRev are published. If minWalOffset
  // provided, the scan iterable should start at that offset.
  async replayFromWal(
    wal: any,
    opts?: { minRev?: number; minWalOffset?: number }
  ) {
    const minRev = opts?.minRev;
    const startOffset =
      typeof opts?.minWalOffset === "number" ? opts!.minWalOffset : 0;
    // use wal.scanWithOffsets(startOffset) to obtain entries with .value
    if (typeof wal?.scanWithOffsets !== "function")
      throw new Error("wal does not support scanWithOffsets");
    for await (const rec of wal.scanWithOffsets(startOffset)) {
      if (!rec || !rec.value) continue;
      const v = rec.value;
      if (v.tx && Array.isArray(v.ops)) {
        const txLeaseId = typeof v.leaseId === "number" ? v.leaseId : undefined;
        const txLeaseExpiresAt =
          typeof v.leaseExpiresAt === "number" ? v.leaseExpiresAt : undefined;
        const txCreatedAt =
          typeof v.createdAt === "number" ? v.createdAt : undefined;
        // Each op may have rev; if minRev provided, skip ops with rev <= minRev
        for (const op of v.ops) {
          if (!op || !op.key) continue;
          const augmented = Object.assign({}, op);
          if (
            typeof augmented.leaseId !== "number" &&
            typeof txLeaseId === "number"
          )
            augmented.leaseId = txLeaseId;
          if (
            typeof augmented.leaseExpiresAt !== "number" &&
            typeof txLeaseExpiresAt === "number"
          )
            augmented.leaseExpiresAt = txLeaseExpiresAt;
          if (
            typeof augmented.createdAt !== "number" &&
            typeof txCreatedAt === "number"
          )
            augmented.createdAt = txCreatedAt;
          this._maybePublishOp(augmented, minRev);
        }
      } else if (v.key !== undefined) {
        this._maybePublishOp(v, minRev);
      }
    }
  }

  // Internal helper that performs per-subscriber filtering and delivery.
  // Ensures that during replay we don't re-deliver events a subscriber has
  // already seen (based on subscriber.lastSeenRev). `minRev` is a global
  // optimization hint; subscribers with no lastSeenRev will still receive all events.
  private _maybePublishOp(op: any, minRev?: number) {
    try {
      const key = String(op.key);
      const val = op.value == null ? null : String(op.value);
      const rev = typeof op.rev === "number" ? op.rev : undefined;
      // If minRev provided, skip events with rev <= minRev globally
      if (typeof minRev === "number") {
        if (typeof rev !== "number" || rev <= minRev) return;
      }
      const createdAt =
        typeof op.createdAt === "number" ? op.createdAt : undefined;
      const leaseId = typeof op.leaseId === "number" ? op.leaseId : undefined;
      const leaseExpiresAt =
        typeof op.leaseExpiresAt === "number" ? op.leaseExpiresAt : undefined;
      const ev: any = {
        key,
        value: val,
        rev,
        createdAt,
        leaseId,
        leaseExpiresAt,
      };

      const notify: number[] = [];
      for (const s of this.subs.values()) {
        try {
          if (s.closed) continue;
          if (!s.match(key)) continue;
          // Per-subscriber resume: skip if subscriber already saw this rev
          try {
            const last =
              typeof s.getLastSeenRev === "function"
                ? s.getLastSeenRev()
                : undefined;
            if (
              typeof rev === "number" &&
              typeof last === "number" &&
              rev <= last
            )
              continue;
          } catch {}
          if (s.ch) {
            try {
              s.ch(ev);
            } catch {}
          }
          if (s.once) notify.push(s.id);
          // update lastSeenRev for this subscriber when event has a rev
          try {
            if (typeof rev === "number") s.setLastSeenRev(rev);
          } catch {}
        } catch {}
      }
      for (const id of notify) {
        const s = this.subs.get(id);
        if (s) this.remove(s);
      }
    } catch {}
  }

  // Export current subscribers as JSON-serializable snapshots
  exportSnapshots() {
    const out: any[] = [];
    for (const s of this.subs.values()) out.push(s.toSnapshot());
    return out;
  }

  // Restore subscribers from snapshots (recreate Subscriber instances and preserve ids/lastSeenRev)
  restoreSnapshots(snaps: any[]) {
    if (!Array.isArray(snaps)) return;
    for (const snap of snaps) {
      try {
        const id = typeof snap.id === "number" ? snap.id : this.nextSubId++;
        const s = new Subscriber(id, snap.filter, !!snap.once);
        Subscriber.restoreSnapshot(s, snap);
        this.subs.set(s.id, s);
        if (s.id >= this.nextSubId) this.nextSubId = s.id + 1;
      } catch {}
    }
  }

  getSubscriberById(id: number) {
    return this.subs.get(id) || null;
  }
}
