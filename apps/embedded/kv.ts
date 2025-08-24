import { Engine } from "../../libs/storage";
import type { EngineOptions } from "../../libs/storage";
import type { WatchEvent, WatchFilter } from "../../libs/watch";

export class KVStore {
  private engine: Engine;
  private dir: string;
  constructor(dir = "./data/embedded", private opts: EngineOptions = {}) {
    this.dir = dir;
    // allow caller to pass Engine options (watchBackend, timeProvider, etc)
    this.engine = new Engine(this.dir, "log.wal", this.opts);
    // index file path
    this._indexPath = `${this.dir}/.kv_index.json`;
    this._index = null; // lazy loaded Map<string, Set<string>> keyed by n-gram
  }

  async open() {
    await this.engine.open();
    // ensure index loaded (best-effort)
    await this._ensureIndexLoaded();
  }

  async close() {
    await this.engine.close();
  }

  async put(
    key: string,
    value: string,
    opts?: { leaseId?: number; leaseTtlMs?: number }
  ) {
    await this.engine.put(Buffer.from(key), Buffer.from(value), opts);
    try {
      await this._addKeyToIndex(key);
    } catch (e) {}
  }

  async get(key: string) {
    const v = this.engine.get(Buffer.from(key));
    return v ? v.toString() : null;
  }

  async del(key: string) {
    await this.engine.del(Buffer.from(key));
    try {
      await this._removeKeyFromIndex(key);
    } catch (e) {}
  }

  async cas(key: string, expectedRev: number | null, newValue: string | null) {
    return await this.engine.cas(
      Buffer.from(key),
      expectedRev,
      newValue === null ? null : Buffer.from(newValue)
    );
  }

  // Lease APIs
  grantLease(ttlMs: number) {
    return this.engine.grantLease(ttlMs);
  }

  attachLease(leaseId: number, key: string) {
    return this.engine.attachLease(leaseId, Buffer.from(key));
  }

  renewLease(leaseId: number, ttlMs: number) {
    return this.engine.renewLease(leaseId, ttlMs);
  }

  revokeLease(leaseId: number, deleteKeys = false) {
    return this.engine.revokeLease(leaseId, deleteKeys);
  }

  // Transaction helper: returns an object with put/del/commit/abort
  beginTransaction() {
    const inner = this.engine.beginTransaction();
    const self = this;
    // track pending keys for completion
    const pendingKeys = new Set<string>();
    return {
      put(key: Buffer, value: Buffer) {
        try {
          pendingKeys.add(key.toString());
        } catch (e) {}
        return inner.put(key, value);
      },
      del(key: Buffer) {
        try {
          pendingKeys.add(key.toString());
        } catch (e) {}
        return inner.del(key);
      },
      async commit() {
        const res = await inner.commit();
        // rebuild index after commit to ensure consistency
        try {
          await self.rebuildIndex();
        } catch (e) {}
        return res;
      },
      abort() {
        return inner.abort();
      },
      // helper used by CLI completer
      _pendingKeys: pendingKeys,
      pendingKeys() {
        return Array.from(pendingKeys);
      },
    };
  }

  // list some keys from the store for completion/hints (not exhaustive)
  async listKeys(limit = 100) {
    const out: string[] = [];
    for await (const e of this.engine.scan()) {
      if (!e || !e.key) continue;
      out.push(e.key.toString());
      if (out.length >= limit) break;
    }
    return out;
  }

  // return list of active lease ids
  listLeases() {
    // engine.leases is private; expose by mapping keys from getLeaseInfo helper
    const out: number[] = [];
    try {
      // iterate leaseCounter and probe getLeaseInfo
      for (let i = 1; i <= this.engine.leaseCounter; i++) {
        const info = this.engine.getLeaseInfo(i);
        if (info) out.push(i);
      }
    } catch (e) {}
    return out;
  }

  // create a subscription; returns subscriber id
  watch(filter: WatchFilter, once = false, handler?: (ev: WatchEvent) => void) {
    const sub = this.engine.watchManager.add(filter, once);
    if (typeof handler === "function") sub.onEvent(handler);
    return sub.id;
  }

  unwatch(id: number) {
    const sub = this.engine.watchManager.getSubscriberById
      ? this.engine.watchManager.getSubscriberById(id)
      : null;
    if (sub) this.engine.watchManager.remove(sub);
  }

  exportSubscribers() {
    if (typeof this.engine.watchManager.exportSnapshots === "function")
      return this.engine.watchManager.exportSnapshots();
    return [];
  }

  // helper: attach handler registry (id -> handler) to existing subscribers
  attachHandlerRegistry(registry: Record<number, (ev: WatchEvent) => void>) {
    for (const idStr of Object.keys(registry)) {
      const id = Number(idStr);
      const handler = registry[id];
      if (typeof handler !== "function") continue;
      const sub = this.engine.watchManager.getSubscriberById
        ? this.engine.watchManager.getSubscriberById(id)
        : null;
      if (sub) sub.onEvent(handler);
    }
  }

  // snapshot at rev or latest (returns array)
  async snapshot(rev?: number) {
    const out: any[] = [];
    for await (const e of this.engine.snapshotAtRevision(rev)) {
      out.push({
        key: e.key.toString(),
        value: e.value ? e.value.toString() : null,
      });
    }
    return out;
  }

  // scan with options: startWith (prefix), endWith (suffix), contains (substring), exact (key)
  // supports limit and offset (applied after filtering)
  async scan(opts?: {
    startWith?: string;
    endWith?: string;
    contains?: string;
    exact?: string;
    limit?: number;
    offset?: number;
  }) {
    const o = opts || {};
    // exact lookup shortcut
    if (typeof o.exact === "string") {
      const v = await this.get(o.exact);
      return [{ key: o.exact, value: v }];
    }

    const results: Array<{ key: string; value: string | null }> = [];
    const limit = typeof o.limit === "number" ? o.limit : Infinity;
    const offset = typeof o.offset === "number" ? o.offset : 0;
    let skipped = 0;

    // if we only have a prefix filter (startWith) and no other filters, we can use range
    const useRange =
      typeof o.startWith === "string" && !o.endWith && !o.contains;

    if (useRange) {
      const it = this.engine.range(Buffer.from(o.startWith!), { offset: 0 });
      for await (const e of it) {
        if (!e || !e.key) continue;
        if (skipped < offset) {
          skipped++;
          continue;
        }
        results.push({
          key: e.key.toString(),
          value: e.value ? e.value.toString() : null,
        });
        if (results.length >= limit) break;
      }
      return results;
    }

    // fallback: full scan and filter
    for await (const e of this.engine.scan()) {
      if (!e || !e.key) continue;
      const k = e.key.toString();
      // prefix
      if (typeof o.startWith === "string" && !k.startsWith(o.startWith))
        continue;
      // suffix
      if (typeof o.endWith === "string" && !k.endsWith(o.endWith)) continue;
      // contains
      if (typeof o.contains === "string" && !k.includes(o.contains)) continue;
      if (skipped < offset) {
        skipped++;
        continue;
      }
      results.push({ key: k, value: e.value ? e.value.toString() : null });
      if (results.length >= limit) break;
    }
    return results;
  }

  // streaming scan: yields results incrementally according to filters
  async *scanStream(opts?: {
    startWith?: string;
    endWith?: string;
    contains?: string;
    exact?: string;
    regex?: string;
    fuzzy?: string;
    limit?: number;
    offset?: number;
  }) {
    const o = opts || {};
    const limit = typeof o.limit === "number" ? o.limit : Infinity;
    const offset = typeof o.offset === "number" ? o.offset : 0;
    let emitted = 0;
    let skipped = 0;

    const matchKey = (k: string) => {
      if (typeof o.exact === "string" && k !== o.exact) return false;
      if (typeof o.startWith === "string" && !k.startsWith(o.startWith))
        return false;
      if (typeof o.endWith === "string" && !k.endsWith(o.endWith)) return false;
      if (typeof o.contains === "string" && !k.includes(o.contains))
        return false;
      if (typeof o.regex === "string") {
        try {
          const re = new RegExp(o.regex);
          if (!re.test(k)) return false;
        } catch (e) {
          return false;
        }
      }
      if (typeof o.fuzzy === "string") {
        // simple subsequence fuzzy: all chars of fuzzy appear in order
        const pat = o.fuzzy;
        let pi = 0;
        for (let i = 0; i < k.length && pi < pat.length; i++)
          if (k[i] === pat[pi]) pi++;
        if (pi !== pat.length) return false;
      }
      return true;
    };

    // If contains filter and index available use index to produce candidates
    if (
      typeof o.contains === "string" &&
      o.contains.length >= 3 &&
      this._index
    ) {
      const grams = this._ngrams(o.contains, 3);
      if (grams.length > 0) {
        // intersect sets
        let cand: Set<string> | null = null;
        for (const g of grams) {
          const s = this._index.get(g) || new Set<string>();
          if (cand === null) cand = new Set(s);
          else {
            for (const k of Array.from(cand)) if (!s.has(k)) cand.delete(k);
          }
        }
        if (cand) {
          for (const k of cand) {
            if (!matchKey(k)) continue;
            if (skipped < offset) {
              skipped++;
              continue;
            }
            const v = await this.get(k);
            yield { key: k, value: v };
            emitted++;
            if (emitted >= limit) return;
          }
          return;
        }
      }
    }

    // fallback: stream from engine.scan
    for await (const e of this.engine.scan()) {
      if (!e || !e.key) continue;
      const k = e.key.toString();
      if (!matchKey(k)) continue;
      if (skipped < offset) {
        skipped++;
        continue;
      }
      const v = e.value
        ? typeof e.value === "string"
          ? e.value
          : e.value.toString()
        : null;
      yield { key: k, value: v };
      emitted++;
      if (emitted >= limit) return;
    }
  }

  // INTERNAL: index management (3-gram index)
  private _indexPath: string;
  private _index: Map<string, Set<string>> | null;

  private async _ensureIndexLoaded() {
    if (this._index) return;
    try {
      const data = await require("fs").promises.readFile(
        this._indexPath,
        "utf-8"
      );
      const obj = JSON.parse(data);
      const m = new Map<string, Set<string>>();
      for (const k of Object.keys(obj)) m.set(k, new Set(obj[k]));
      this._index = m;
    } catch (e) {
      this._index = new Map();
    }
  }

  private async _saveIndex() {
    if (!this._index) return;
    const obj: any = {};
    for (const [k, s] of this._index.entries()) obj[k] = Array.from(s);
    try {
      await require("fs").promises.mkdir(this.dir, { recursive: true });
      await require("fs").promises.writeFile(
        this._indexPath,
        JSON.stringify(obj),
        "utf-8"
      );
    } catch (e) {}
  }

  private _ngrams(s: string, n: number) {
    const out: string[] = [];
    if (s.length <= n) {
      out.push(s);
      return out;
    }
    for (let i = 0; i <= s.length - n; i++) out.push(s.slice(i, i + n));
    return out;
  }

  private async _addKeyToIndex(key: string) {
    await this._ensureIndexLoaded();
    const idx = this._index!;
    const grams = this._ngrams(key, 3);
    for (const g of grams) {
      let s = idx.get(g);
      if (!s) {
        s = new Set<string>();
        idx.set(g, s);
      }
      s.add(key);
    }
    await this._saveIndex();
  }

  private async _removeKeyFromIndex(key: string) {
    await this._ensureIndexLoaded();
    const idx = this._index!;
    const grams = this._ngrams(key, 3);
    for (const g of grams) {
      const s = idx.get(g);
      if (!s) continue;
      s.delete(key);
      if (s.size === 0) idx.delete(g);
    }
    await this._saveIndex();
  }

  async rebuildIndex() {
    // rebuild full index from scratch
    this._index = new Map();
    for await (const e of this.engine.scan()) {
      if (!e || !e.key) continue;
      const k = e.key.toString();
      const grams = this._ngrams(k, 3);
      for (const g of grams) {
        let s = this._index.get(g);
        if (!s) {
          s = new Set<string>();
          this._index.set(g, s);
        }
        s.add(k);
      }
    }
    await this._saveIndex();
  }

  // return last N events from WAL (best-effort) to preload monitor/console
  async recentEvents(limit = 100) {
    const out: any[] = [];
    try {
      const wal = this.engine.wal;
      if (!wal || typeof wal.scan !== "function") return out;
      for await (const entry of wal.scan(0)) {
        if (!entry) continue;
        if (!entry.key) continue;
        const ev: WatchEvent = {
          key: entry.key,
          value: entry.value == null ? null : entry.value,
          rev: entry.rev,
        };
        if (typeof entry.createdAt === "number") ev.createdAt = entry.createdAt;
        out.push(ev);
        if (out.length > limit * 3) out.shift(); // keep window slightly larger
      }
      // return last `limit` events
      return out.slice(-limit);
    } catch (e) {
      return out;
    }
  }
}
