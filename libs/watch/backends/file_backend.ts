import fs from "node:fs";
import { join } from "node:path";
import { Subscriber } from "../subscriber";
import type { WatchBackend } from "../watch_manager";

export class FileBackend implements WatchBackend {
  private subs = new Map<number, Subscriber>();
  private nextId = 1;
  private path: string;

  constructor(private dir: string, private opts?: any) {
    this.path = join(dir || ".", "watch_subs.json");
    try {
      if (fs.existsSync(this.path)) {
        const raw = fs.readFileSync(this.path, "utf8");
        const arr = JSON.parse(raw || "[]");
        for (const s of arr) {
          try {
            const sub = new Subscriber(
              s.id || this.nextId++,
              s.filter,
              !!s.once
            );
            if (typeof s.lastSeenRev === "number")
              sub.setLastSeenRev(s.lastSeenRev);
            this.subs.set(sub.id, sub);
            if (sub.id >= this.nextId) this.nextId = sub.id + 1;
          } catch {}
        }
      }
    } catch {}
  }

  add(filter: any, once = false) {
    const id = this.nextId++;
    const s = new Subscriber(id, filter, once);
    this.subs.set(s.id, s);
    this.persist();
    return s;
  }

  remove(sub: Subscriber) {
    this.subs.delete(sub.id);
    try {
      sub.close();
    } catch {}
    this.persist();
  }

  publish(ev: any) {
    const notify: number[] = [];
    for (const s of this.subs.values()) {
      if (s.closed) continue;
      try {
        if (s.match(ev.key)) {
          if (s.ch) {
            try {
              s.ch(ev);
            } catch {}
          }
          if (s.once) notify.push(s.id);
          // update lastSeenRev if present
          try {
            if (typeof ev.rev === "number") s.setLastSeenRev(ev.rev);
          } catch {}
        }
      } catch {}
    }
    for (const id of notify) {
      const s = this.subs.get(id);
      if (s) this.remove(s);
    }
    this.persist();
  }

  exportSnapshots() {
    const out: any[] = [];
    for (const s of this.subs.values()) out.push(s.toSnapshot());
    return out;
  }

  restoreSnapshots(snaps: any[]) {
    if (!Array.isArray(snaps)) return;
    for (const s of snaps) {
      try {
        const id = typeof s.id === "number" ? s.id : this.nextId++;
        const sub = new Subscriber(id, s.filter, !!s.once);
        Subscriber.restoreSnapshot(sub, s);
        this.subs.set(sub.id, sub);
        if (sub.id >= this.nextId) this.nextId = sub.id + 1;
      } catch {}
    }
    this.persist();
  }

  getSubscriberById(id: number) {
    return this.subs.get(id) || null;
  }

  async replayFromScan(scanIterable: AsyncIterable<any>, minRev?: number) {
    for await (const rec of scanIterable) {
      if (!rec || !rec.value) continue;
      const v = rec.value;
      if (v.tx && Array.isArray(v.ops)) {
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
        if (v.key !== undefined) this._maybePublishOp(v, minRev);
      }
    }
  }

  // Optional: scan WAL offsets
  async replayFromWal(
    wal: any,
    opts?: { minRev?: number; minWalOffset?: number }
  ) {
    const startOffset =
      typeof opts?.minWalOffset === "number" ? opts!.minWalOffset : 0;
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
          this._maybePublishOp(augmented, opts?.minRev);
        }
      } else if (v.key !== undefined) {
        this._maybePublishOp(v, opts?.minRev);
      }
    }
  }

  private _maybePublishOp(op: any, minRev?: number) {
    try {
      const rev = typeof op.rev === "number" ? op.rev : undefined;
      if (typeof minRev === "number") {
        if (typeof rev !== "number" || rev <= minRev) return;
      }
      const key = String(op.key);
      const val = op.value == null ? null : String(op.value);
      const ev: any = {
        key,
        value: val,
        rev,
        createdAt: op.createdAt,
        leaseId: op.leaseId,
        leaseExpiresAt: op.leaseExpiresAt,
      };
      this.publish(ev);
    } catch {}
  }

  private persist() {
    try {
      const arr: any[] = [];
      for (const s of this.subs.values()) arr.push(s.toSnapshot());
      try {
        fs.writeFileSync(this.path, JSON.stringify(arr, null, 2), "utf8");
      } catch {}
    } catch {}
  }
}
