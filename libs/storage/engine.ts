import fs, { existsSync, unlinkSync, renameSync } from "node:fs";
import { join } from "node:path";
import { prefixEndExclusive } from "../utils";
import { HandoffWal, Wal, type WalLike } from "../wal";
import MemTable from "./memtable";
import Manifest from "./manifest";
import { kWayMerge } from "./merge_iterator";
import { cleanupDeleteMarkers } from "./file_refcount";
import Compactor from "./compactor";
import type { CompactorOptions } from "./compactor";
import { SSTReader } from "./sstreader";
import { SSTWriter } from "./sstwriter";

type SSTHandle = { reader: SSTReader; meta: any };

export class Engine {
  // simple runtime metrics
  public metrics = {
    gc: {
      deleteMarkersScanned: 0,
      deleteMarkersRemoved: 0,
      filesDeleted: 0,
      runs: 0,
    },
    compaction: {
      runs: 0,
      bytesWritten: 0,
      filesCreated: 0,
      lastDurationMs: 0,
    },
  };
  private mem = new MemTable();
  // global monotonically-increasing revision counter for MVCC
  private revCounter = 0;
  private manifest: Manifest;
  private wal: WalLike;
  private walFile: string;
  private sstReaders: SSTHandle[] = [];
  private flushing = false;
  private walGCTimer: NodeJS.Timeout | null = null;
  // simple per-key async queue for CAS/atomic ops
  private keyQueues: Map<string, (() => void)[]> = new Map();
  // track active snapshot revisions to compute min-active-rev for compaction GC
  private activeSnapshotCounts: Map<number, number> = new Map();

  constructor(
    private dir = "./data",
    walFile = "log.wal",
    private opts?: {
      gcIntervalMs?: number;
      deleteMarkerStaleMs?: number;
      enableMarkerGC?: boolean;
      compactorOptions?: CompactorOptions;
      // optional time provider for deterministic tests
      timeProvider?: () => number;
      // choose WAL implementation: 'wal' (default) or 'handoff'
      walImpl?: "wal" | "handoff";
      // when true, SSTWriter and Manifest will perform durable fsyncs on tmp files and parent dir
      // during writes/renames (best-effort). Explicit engine-level flag overrides compactorOptions.
      strictAtomicity?: boolean;
    }
  ) {
    const manifestStrict =
      typeof this.opts?.strictAtomicity === "boolean"
        ? this.opts.strictAtomicity
        : this.opts?.compactorOptions
        ? (this.opts.compactorOptions as any).strictAtomicity
        : undefined;
    this.manifest = Manifest.load(dir, manifestStrict);
    this.walFile = walFile;
    const impl = this.opts?.walImpl || "wal";
    if (impl === "handoff") {
      this.wal = new HandoffWal(walFile, { batching: false });
    } else {
      this.wal = new Wal(walFile, { batching: false });
    }
    (this.wal as any).rootDir = dir;
  }

  private compactionTimer: NodeJS.Timeout | null = null;

  // Start background compaction runner if compactorOptions provided. IntervalMs defaults to 5s.
  startBackgroundCompaction(intervalMs?: number) {
    if (this.compactionTimer) return;
    const iv = typeof intervalMs === "number" ? intervalMs : 5000;
    this.compactionTimer = setInterval(async () => {
      try {
        // backpressure: if flushing in progress, skip this run to avoid IO contention
        if (this.flushing) return;
        const start = Date.now();
        const res = await this.compactNow();
        const dur = Date.now() - start;
        this.metrics.compaction.runs += 1;
        if (res && typeof res.bytesWritten === "number")
          this.metrics.compaction.bytesWritten += res.bytesWritten;
        if (res && typeof res.filesCreated === "number")
          this.metrics.compaction.filesCreated += res.filesCreated;
        this.metrics.compaction.lastDurationMs = dur;
      } catch (e) {
        // ignore compaction errors in background
      }
    }, iv) as unknown as NodeJS.Timeout;
  }

  stopBackgroundCompaction() {
    if (this.compactionTimer) {
      clearInterval(this.compactionTimer as any);
      this.compactionTimer = null;
    }
  }

  // Acquire a per-key lock; returns an unlock function
  private async acquireKeyLock(key: Buffer): Promise<() => void> {
    const k = key.toString("hex");
    let q = this.keyQueues.get(k);
    if (!q) {
      q = [];
      this.keyQueues.set(k, q);
    }
    return new Promise((resolve) => {
      const ticket = () => {
        // unlock function
        const unlock = () => {
          const qq = this.keyQueues.get(k);
          if (!qq) return;
          // remove the head
          qq.shift();
          if (qq.length === 0) this.keyQueues.delete(k);
          else {
            // call next ticket to let next waiter proceed
            const next = qq[0];
            try {
              if (typeof next === "function") next();
            } catch {}
          }
        };
        resolve(unlock);
      };
      q!.push(ticket);
      // if we're first in queue, immediately run ticket
      if (q!.length === 1) ticket();
    });
  }

  /**
   * Public API: enable or disable background compaction at runtime.
   * When enabled, starts the background compaction runner with provided intervalMs (defaults to 5s).
   */
  setBackgroundCompaction(enabled: boolean, intervalMs?: number) {
    if (enabled) {
      this.startBackgroundCompaction(intervalMs);
    } else {
      this.stopBackgroundCompaction();
    }
  }

  // Run compaction with Engine-configured options
  async compactNow() {
    const baseOpts: any = this.opts?.compactorOptions || {};
    // clone to avoid mutating user-provided object
    const cOpts: any = Object.assign({}, baseOpts);
    const minActive = this.getMinActiveRev();
    if (typeof minActive === "number") cOpts.minActiveRev = minActive;
    // also compute max active rev and pass it so compactor can protect tombstones needed by any active snapshot
    const maxActive = this.getMaxActiveRev();
    if (typeof maxActive === "number") cOpts.maxActiveRev = maxActive;
    // sensible default: retain tombstones for 24 hours (time-based TTL)
    if (typeof cOpts.tombstoneRetentionMs !== "number") {
      cOpts.tombstoneRetentionMs = 24 * 60 * 60 * 1000; // 24h
    }
    // propagate engine time provider if present
    if (typeof this.opts?.timeProvider === "function")
      cOpts.timeProvider = this.opts.timeProvider;
    const c = new Compactor(this.dir, this.manifest, cOpts);
    const res = await c.compact();
    // After compaction, refresh in-memory SST readers to reflect manifest changes
    try {
      this.reloadSstReadersFromManifest();
    } catch (e) {
      // ignore reload errors
    }
    return res;
  }

  // Refresh this.sstReaders from manifest files on disk. Best-effort; ignore failures.
  private reloadSstReadersFromManifest() {
    this.sstReaders = [];
    const files = this.manifest.listFiles();
    for (const f of files) {
      try {
        if (!existsSync(f.file)) continue;
        const r = SSTReader.open(f.file);
        this.sstReaders.push({ reader: r, meta: f });
      } catch (e) {
        // skip failing readers
      }
    }
    // ensure ordering by walOffset
    this.sstReaders.sort((a, b) => {
      const A = typeof a.meta.walOffset === "number" ? a.meta.walOffset : 0;
      const B = typeof b.meta.walOffset === "number" ? b.meta.walOffset : 0;
      return A - B;
    });
  }

  /**
   * Run a compaction and invoke the provided callback with per-run stats.
   * The callback is invoked synchronously after compaction finishes.
   */
  async runCompactionWithCallback(
    callback: (stats: { bytesWritten: number; filesCreated: number }) => void
  ) {
    const stats = await this.compactNow();
    try {
      callback(stats || { bytesWritten: 0, filesCreated: 0 });
    } catch (e) {
      // swallow callback errors to avoid impacting engine state
    }
    return stats;
  }

  async open() {
    await this.wal.open();
    const debugRebuild = !!process.env.KV_DEBUG_REBUILD;
    // If debug flag set, dump WAL entries and offsets to help trace rebuild issues
    if (debugRebuild) {
      try {
        console.log("[debug] dumping WAL entries (start,end,key,len)");
        for await (const rec of (this.wal as any).scanWithOffsets(0)) {
          try {
            const valueStr =
              rec.value &&
              rec.value.value &&
              typeof rec.value.value === "string"
                ? rec.value.value.slice(0, 200)
                : JSON.stringify(rec.value && rec.value.value);
            console.log(
              "[debug] wal entry start=%d end=%d key=%s value=%s",
              rec.start,
              rec.end,
              rec.value && rec.value.key,
              valueStr
            );
          } catch (e) {}
        }
      } catch (e: any) {
        console.log(
          "[debug] failed to dump WAL",
          e && e.message ? e.message : String(e)
        );
      }
    }
    // load SST readers from manifest and validate; attempt remediation on corrupt SSTs
    // removed debug logging
    const files = this.manifest.listFiles();
    const toRemove: string[] = [];
    for (const f of files) {
      try {
        if (!existsSync(f.file)) continue;
        // SST open failed; attempt remediation
        const r = SSTReader.open(f.file);
        this.sstReaders.push({ reader: r, meta: f });
      } catch (e) {
        // SST open failed; attempt remediation
        // corrupt or unreadable SST: attempt remediation by rebuilding from WAL
        try {
          // compute key range from meta if available
          const minKey = f.minKeyHex
            ? Buffer.from(f.minKeyHex, "hex")
            : undefined;
          const maxKey = f.maxKeyHex
            ? Buffer.from(f.maxKeyHex, "hex")
            : undefined;
          if (minKey && maxKey) {
            // build a new SST from WAL entries in [minKey, maxKey]
            const tmp = `${this.dir}/sst_rebuild_${Date.now()}.tmp`;
            const final = `${this.dir}/sst_rebuild_${Date.now()}.sst`;
            const wOptsRebuild: any = {};
            // prefer explicit engine-level strictAtomicity, else fall back to compactorOptions
            if (
              typeof this.opts?.strictAtomicity === "boolean"
                ? this.opts.strictAtomicity
                : this.opts?.compactorOptions &&
                  (this.opts.compactorOptions as any).strictAtomicity
            )
              wOptsRebuild.strictAtomicity = true;
            const w = new SSTWriter(tmp, final, wOptsRebuild);
            // track last included WAL end offset
            let lastWalEnd: number | undefined = undefined;
            // conservative minOffset for scanning: replay from start (0)
            // This avoids skipping entries when other manifest entries may have missing/incorrect walOffset
            const minForRebuild = 0;
            for await (const rec of (this.wal as any).scanWithOffsets(
              minForRebuild
            )) {
              const entry = rec.value;
              if (!entry || !entry.key) continue;
              const k = Buffer.from(entry.key);
              if (Buffer.compare(k, minKey) < 0) continue;
              if (Buffer.compare(k, maxKey) > 0) continue;
              if (debugRebuild) {
                try {
                  console.log(
                    "[rebuild] wal rec start=%d end=%d key=%s val=%s",
                    rec.start,
                    rec.end,
                    entry.key,
                    typeof entry.value === "string"
                      ? entry.value.slice(0, 200)
                      : JSON.stringify(entry.value)
                  );
                } catch (e) {}
              }
              if (entry.value == null) {
                const entryCreated =
                  typeof this.opts?.timeProvider === "function"
                    ? this.opts.timeProvider()
                    : Date.now();
                w.add(
                  k,
                  null,
                  undefined,
                  typeof rec.end === "number" ? rec.end : undefined,
                  entryCreated
                );
              } else {
                const entryCreated =
                  typeof this.opts?.timeProvider === "function"
                    ? this.opts.timeProvider()
                    : Date.now();
                w.add(
                  k,
                  Buffer.from(entry.value),
                  undefined,
                  typeof rec.end === "number" ? rec.end : undefined,
                  entryCreated
                );
              }
              // record end offset for walOffset calculation
              if (typeof rec.end === "number") lastWalEnd = rec.end;
            }
            try {
              const metaNew = w.finish();
              // open and register
              // if we captured WAL end offset, set walOffset on meta
              const r2 = SSTReader.open(metaNew.file);
              this.sstReaders.push({ reader: r2, meta: metaNew });
              // persist replacement in manifest
              try {
                this.manifest.replaceFiles(
                  [f.file],
                  [
                    {
                      file: metaNew.file,
                      minKeyHex: metaNew.minKey.toString("hex"),
                      maxKeyHex: metaNew.maxKey.toString("hex"),
                      size: metaNew.size,
                      level: f.level || 0,
                      walOffset:
                        typeof lastWalEnd === "number"
                          ? lastWalEnd
                          : f.walOffset,
                      createdAt:
                        typeof this.opts?.timeProvider === "function"
                          ? this.opts.timeProvider()
                          : Date.now(),
                    },
                  ]
                );
                // try to remove the corrupted file
                try {
                  unlinkSync(f.file);
                } catch {}
                continue; // move to next manifest entry
              } catch (e2) {
                // fallback: if manifest update fails, fall through to renaming
              }
            } catch (eNew) {
              // building new SST failed; fall through to isolate corrupted file
            }
          }
        } catch (remErr) {
          // ignore remediation errors and fall back to isolating corrupted file
        }
        // isolate the file and remove from manifest
        try {
          const badName = `${f.file}.corrupt.${Date.now()}`;
          try {
            renameSync(f.file, badName);
          } catch (renameErr) {
            // if rename fails, ignore; we'll still remove manifest entry
          }
        } catch {}
        toRemove.push(f.file);
      }
    }
    if (toRemove.length) {
      // persist manifest changes to skip bad SSTs on future opens
      this.manifest.removeFiles(toRemove);
    }

    // Ensure SST readers are ordered by increasing walOffset so newer SSTs
    // (higher walOffset) are preferred when iterating newest-first.
    this.sstReaders.sort((a, b) => {
      const A = typeof a.meta.walOffset === "number" ? a.meta.walOffset : 0;
      const B = typeof b.meta.walOffset === "number" ? b.meta.walOffset : 0;
      return A - B;
    });

    // compute earliest WAL offset required from remaining manifest entries
    const filesForReplay = this.manifest.listFiles();
    const refOffsets = filesForReplay
      .map((f) => (typeof f.walOffset === "number" ? f.walOffset : undefined))
      .filter((x) => typeof x === "number") as number[];
    const minRequiredWalOffset = refOffsets.length
      ? Math.min(...refOffsets)
      : 0;
    const debugReplay = !!process.env.KV_DEBUG_REPLAY;
    // replay WAL into memtable (skip segments wholly covered by SSTs)
    for await (const entry of (this.wal as any).scan(minRequiredWalOffset)) {
      if (debugReplay) {
        try {
          const k =
            entry && entry.key ? String(entry.key).slice(0, 200) : "<nil>";
          const v =
            entry && entry.value
              ? typeof entry.value === "string"
                ? entry.value.slice(0, 200)
                : JSON.stringify(entry.value).slice(0, 200)
              : "<nil>";
          console.log("[debug replay] entry key=%s value=%s", k, v);
        } catch (e) {}
      }
      if (entry && entry.key) {
        // replaying entry into memtable. If WAL contains a revision, honor it; otherwise fall back.
        const kbuf = Buffer.from(entry.key);
        if (entry.value == null)
          this.mem.delete(
            kbuf,
            typeof entry.rev === "number" ? entry.rev : undefined
          );
        else
          this.mem.put(
            kbuf,
            Buffer.from(entry.value),
            typeof entry.rev === "number" ? entry.rev : undefined
          );
        // keep revCounter at least as large as any seen rev so future writes are monotonically increasing
        if (typeof entry.rev === "number" && Number.isFinite(entry.rev)) {
          this.revCounter = Math.max(this.revCounter, entry.rev);
        }
      }
    }

    // start WAL GC worker
    this.startWalGC();
    // start delete-marker GC if enabled
    if (this.opts?.enableMarkerGC) this.startDeleteMarkerGC();
    // start background compaction if compactor options present
    if (this.opts?.compactorOptions) {
      // use short interval in tests to make it responsive
      this.startBackgroundCompaction(1000);
    }
  }

  async put(key: Buffer, value: Buffer) {
    // ensure atomicity with CAS by acquiring per-key lock
    const unlock = await this.acquireKeyLock(key);
    try {
      // allocate next revision and persist it in WAL
      this.revCounter += 1;
      const rev = this.revCounter;
      await this.wal.append({
        key: key.toString(),
        value: value.toString(),
        rev,
      });
      this.mem.put(key, value, rev);
    } finally {
      unlock();
    }
    // trigger flush if memtable exceeds approximate limit
    try {
      const sz = (this.mem as any).sizeBytes?.();
      const limit = (this.mem as any).approxLimit?.();
      if (typeof sz === "number" && typeof limit === "number" && sz >= limit) {
        if (!this.flushing) {
          this.flushing = true;
          try {
            // blocking flush: await to provide backpressure to callers
            await this.flush();
          } finally {
            this.flushing = false;
          }
        }
      }
    } catch (e) {
      // best-effort, ignore
    }
  }

  async del(key: Buffer) {
    const unlock = await this.acquireKeyLock(key);
    try {
      this.revCounter += 1;
      const rev = this.revCounter;
      await this.wal.append({ key: key.toString(), value: null, rev });
      this.mem.delete(key, rev);
    } finally {
      unlock();
    }
  }

  /**
   * Compare-and-swap (CAS) semantic for a single key.
   * - expectedRev: if null means expect key to not exist; if number, expect latest rev to equal it
   * - newValue: Buffer|null the value to set when comparison succeeds (null = tombstone)
   * Returns { ok: boolean, rev?: number } where rev is the new assigned revision on success.
   */
  async cas(key: Buffer, expectedRev: number | null, newValue: Buffer | null) {
    // Acquire per-key lock to make read-compare-write atomic
    const unlock = await this.acquireKeyLock(key);
    try {
      // Read current latest rev for the key from memtable then SSTs
      // Check memtable first
      const m = (this.mem as any).get(key);
      let currentRev: number | null = null;
      if (m && typeof m.rev === "number") currentRev = m.rev;
      else {
        // fallback to scanning SSTs newest-first for the key
        for (let i = this.sstReaders.length - 1; i >= 0; i--) {
          const entry = this.sstReaders[i];
          if (!entry) continue;
          try {
            const r = entry.reader as any;
            const gr = r.getWithRev ? r.getWithRev(key) : null;
            if (gr && typeof (gr as any).rev === "number") {
              currentRev = (gr as any).rev;
              break;
            }
          } catch (e) {}
        }
      }

      // Interpret expectedRev: null means key must not exist (currentRev === null)
      const match =
        expectedRev === null ? currentRev === null : currentRev === expectedRev;
      if (!match) return { ok: false };

      // allocate new revision and persist via WAL and memtable
      this.revCounter += 1;
      const newRev = this.revCounter;
      await this.wal.append({
        key: key.toString(),
        value: newValue === null ? null : newValue.toString(),
        rev: newRev,
      });
      if (newValue === null) this.mem.delete(key, newRev);
      else this.mem.put(key, newValue, newRev);
      return { ok: true, rev: newRev };
    } finally {
      unlock();
    }
  }

  get(key: Buffer) {
    const v = this.mem.get(key);
    if (v && v.value) return Buffer.from(v.value);
    // check SST readers newest-first
    for (let i = this.sstReaders.length - 1; i >= 0; i--) {
      const entry = this.sstReaders[i];
      if (!entry) continue;
      const r = entry.reader;
      const v2 = r.get(key);
      if (v2 !== null) return v2;
    }
    return null;
  }

  /**
   * Read the value of `key` as of a specific revision. Returns Buffer or null if tombstoned or not present.
   * If rev is undefined, behaves like `get()` (latest).
   */
  async getAtRevision(key: Buffer, rev?: number) {
    if (typeof rev !== "number") return this.get(key);
    // For point-in-time reads, gather candidate revisions from memtable and each SST by scanning only
    // the target key range. This allows discovering older revisions recorded in SSTs (not only the
    // latest per-SST) which is required for strict MVCC reads.
    let bestRev: number | undefined = undefined;
    let bestVal: Buffer | null = null;

    try {
      const m = (this.mem as any).get(key);
      if (m && typeof m.rev === "number") {
        if (m.rev <= rev) {
          bestRev = m.rev;
          bestVal = m.value ? Buffer.from(m.value) : null;
        }
      }
    } catch (e) {}

    // Scan each SST reader's iteratorRange for the key, looking for revisions <= target and keeping the highest
    for (let i = this.sstReaders.length - 1; i >= 0; i--) {
      const entry = this.sstReaders[i];
      if (!entry) continue;
      try {
        const r = entry.reader as any;
        // use iteratorRange(start, undefined) to limit reads to keys >= start
        const iter = r.iteratorRange
          ? r.iteratorRange(key, undefined)
          : r.iterator();
        for await (const e of iter) {
          if (!e || !e.key) continue;
          if (!e.key.equals(key)) continue;
          const er = typeof e.rev === "number" ? e.rev : 0;
          if (er <= rev && (typeof bestRev !== "number" || er > bestRev)) {
            bestRev = er;
            bestVal = e.value === null ? null : Buffer.from(e.value);
          }
        }
      } catch (e) {
        // ignore per-reader errors
      }
    }

    return bestVal;
  }

  /**
   * Snapshot at a given revision. Yields entries visible at that revision in key order.
   * If rev is undefined, yields current latest state (same as range()).
   */
  async *snapshotAtRevision(
    rev?: number,
    prefix?: Buffer,
    opts?: { offset?: number; limit?: number }
  ) {
    // If rev undefined, just delegate to range
    if (typeof rev !== "number") {
      for await (const e of this.range(prefix, opts)) yield e;
      return;
    }

    // register this active snapshot revision so compactor can compute minActiveRev
    const prev = this.activeSnapshotCounts.get(rev) || 0;
    this.activeSnapshotCounts.set(rev, prev + 1);
    if (process.env.KV_SNAPSHOT_DEBUG) {
      try {
        console.error(
          "[snapshot-debug] registered snapshot rev=%d count=%d",
          rev,
          prev + 1
        );
      } catch (e) {}
    }
    const unregister = () => {
      const cur = this.activeSnapshotCounts.get(rev) || 0;
      if (cur <= 1) this.activeSnapshotCounts.delete(rev);
      else this.activeSnapshotCounts.set(rev, cur - 1);
      if (process.env.KV_SNAPSHOT_DEBUG) {
        try {
          console.error(
            "[snapshot-debug] unregistered snapshot rev=%d remaining=%d",
            rev,
            Math.max(0, cur - 1)
          );
        } catch (e) {}
      }
    };

    // Build iterables: memtable and SSTs
    const start = prefix;
    let end: Buffer | undefined;
    if (prefix) {
      const pe = prefixEndExclusive(prefix);
      end = pe ? Buffer.from(pe) : undefined;
    }
    const iterables: AsyncIterable<any>[] = [];
    if (typeof (this.mem as any).iteratorRange === "function") {
      iterables.push((this.mem as any).iteratorRange(start, end));
    } else {
      iterables.push(this.mem.iterator());
    }
    for (const s of this.sstReaders) {
      try {
        const minBuf = s.meta.minKeyHex
          ? Buffer.from(s.meta.minKeyHex, "hex")
          : undefined;
        const maxBuf = s.meta.maxKeyHex
          ? Buffer.from(s.meta.maxKeyHex, "hex")
          : undefined;
        if (start && end && minBuf && maxBuf) {
          if (Buffer.compare(maxBuf, start) < 0) continue;
          if (Buffer.compare(minBuf, end) >= 0) continue;
        }
        iterables.push(s.reader.iteratorRange(start, end));
      } catch (e) {
        iterables.push(s.reader.iteratorRange(start, end));
      }
    }

    // lightweight in-stream dedupe by key: for each emitted key choose highest rev <= target
    const seen = new Set<string>();
    try {
      for await (const e of kWayMerge(iterables as any) as any) {
        if (!e || !e.key) continue;
        const khex = e.key.toString("hex");
        if (seen.has(khex)) continue; // already decided
        // kWayMerge yields a chosen payload for this key. If multiple inputs existed for
        // the same key, the merge iterator attaches a `candidates` array containing all
        // payloads seen for this key. To compute visibility at a target revision, inspect
        // candidates and pick the highest revision <= target (if any).
        const cands: any[] | undefined = (e as any).candidates;
        let bestRevForKey: number | undefined = undefined;
        let bestValForKey: Buffer | null = null;
        if (Array.isArray(cands) && cands.length > 0) {
          for (const cand of cands) {
            const cr = typeof cand.rev === "number" ? cand.rev : 0;
            if (
              cr <= rev &&
              (typeof bestRevForKey !== "number" || cr > bestRevForKey)
            ) {
              bestRevForKey = cr;
              bestValForKey =
                cand.value === null ? null : Buffer.from(cand.value);
            }
          }
        } else {
          const entryRev = typeof e.rev === "number" ? e.rev : 0;
          if (entryRev <= rev) {
            bestRevForKey = entryRev;
            bestValForKey = e.value === null ? null : Buffer.from(e.value);
          }
        }
        if (typeof bestRevForKey === "number") {
          seen.add(khex);
          yield { key: e.key, value: bestValForKey };
        } else {
          // no visible entry at this rev; mark seen to skip later duplicates
          seen.add(khex);
        }
      }
    } finally {
      // ensure we unregister even if consumer stops early or an error occurs
      try {
        unregister();
      } catch (e) {}
    }
  }

  /**
   * Range query with optional prefix, offset (skip) and limit. Yields entries in key order.
   * - prefix: if provided, only keys starting with this prefix are returned
   * - offset: number of entries to skip
   * - limit: maximum number of entries to yield
   */
  async *range(prefix?: Buffer, opts?: { offset?: number; limit?: number }) {
    const start = prefix;
    // compute exact exclusive end for prefix
    let end: Buffer | undefined;
    if (prefix) {
      const pe = prefixEndExclusive(prefix);
      end = pe ? Buffer.from(pe) : undefined;
    }

    const iterables: AsyncIterable<any>[] = [];
    // prefer memtable.iteratorRange when available for efficient start/stop
    if (typeof (this.mem as any).iteratorRange === "function") {
      iterables.push((this.mem as any).iteratorRange(start, end));
    } else {
      iterables.push(this.mem.iterator());
    }

    // pre-filter SST readers by manifest min/max before including their iteratorRange
    for (const s of this.sstReaders) {
      try {
        const minBuf = s.meta.minKeyHex
          ? Buffer.from(s.meta.minKeyHex, "hex")
          : undefined;
        const maxBuf = s.meta.maxKeyHex
          ? Buffer.from(s.meta.maxKeyHex, "hex")
          : undefined;
        // If prefix start/end are both defined and SST's range doesn't overlap [start,end), skip
        if (start && end && minBuf && maxBuf) {
          if (Buffer.compare(maxBuf, start) < 0) continue; // sst max < start
          if (Buffer.compare(minBuf, end) >= 0) continue; // sst min >= end
        }
        iterables.push(s.reader.iteratorRange(start, end));
      } catch (e) {
        // best-effort: if meta missing or parse fails, include reader
        iterables.push(s.reader.iteratorRange(start, end));
      }
    }

    let skipped = 0;
    let yielded = 0;
    const off = opts?.offset || 0;
    const lim = typeof opts?.limit === "number" ? opts!.limit : Infinity;

    // kWayMerge will merge entries sorted by key. We still need to filter by prefix and apply offset/limit.
    for await (const e of kWayMerge(iterables as any)) {
      if (prefix && !e.key.slice(0, prefix.length).equals(prefix)) continue;
      if (skipped < off) {
        skipped++;
        continue;
      }
      if (yielded >= lim) break;
      yielded++;
      yield e;
    }
  }

  // merged scan: yield entries from memtable and SSTs in key order (simple merge)
  async *scan() {
    // collect iterators
    const iters: AsyncGenerator<any>[] = [];
    iters.push(this.mem.iterator());
    for (const s of this.sstReaders) iters.push(s.reader.iterator());

    // naive merge by materializing all entries (ok for prototype)
    const map = new Map<string, { key: Buffer; value: Buffer | null }>();
    for (const it of iters) {
      for await (const e of it) {
        map.set(e.key.toString("hex"), { key: e.key, value: e.value });
      }
    }
    const keys = Array.from(map.keys()).sort();
    for (const k of keys) {
      const e = map.get(k)!;
      yield e;
    }
  }

  // snapshot memtable to SST and persist manifest
  async flush() {
    // capture current WAL end offset before freezing memtable so SST records exactly which
    // WAL position it includes
    const off = (this.wal as any).currentEndOffset?.();
    const walOffset = typeof off === "number" ? off : undefined;

    const snap = this.mem.snapshot();
    const tmp = `${this.dir}/sst_${Date.now()}.tmp`;
    const final = `${this.dir}/sst_${Date.now()}.sst`;
    const wOpts: any = {};
    if (
      typeof this.opts?.strictAtomicity === "boolean"
        ? this.opts.strictAtomicity
        : this.opts?.compactorOptions &&
          (this.opts.compactorOptions as any).strictAtomicity
    )
      wOpts.strictAtomicity = true;
    const w = new SSTWriter(tmp, final, wOpts);
    for await (const e of snap.iterator()) {
      const entryCreated =
        typeof this.opts?.timeProvider === "function"
          ? this.opts.timeProvider()
          : Date.now();
      w.add(e.key, e.value, e.rev, walOffset, entryCreated);
    }
    const meta = w.finish();
    // open reader for the newly-created SST so it becomes visible to read paths
    try {
      const r = SSTReader.open(meta.file);
      this.sstReaders.push({ reader: r, meta });
    } catch (e) {
      // if opening fails, we still persisted manifest; keep going
    }
    this.manifest.addFile({
      file: meta.file,
      minKeyHex: meta.minKey.toString("hex"),
      maxKeyHex: meta.maxKey.toString("hex"),
      size: meta.size,
      level: 0,
      walOffset,
      createdAt:
        typeof this.opts?.timeProvider === "function"
          ? this.opts.timeProvider()
          : Date.now(),
    });
    // After successful SST write and manifest persist, record WAL offset and truncate WAL up to that offset
    try {
      if (typeof walOffset === "number") {
        // Ensure WAL entries up to walOffset are durably persisted before
        // updating the manifest to point at that offset. For implementations
        // like HandoffWal append() may return before durability; flush()
        // forces background writer to drain and fdatasync.
        if (typeof (this.wal as any).flush === "function") {
          try {
            await (this.wal as any).flush();
          } catch (e) {
            // best-effort: if flush fails, continue cautiously (we'll still set manifest)
          }
        }
        this.manifest.setWalOffset(walOffset);
        // truncate WAL up to this offset - WAL entries up to this pos are now also in SST
        if ((this.wal as any).truncateUpTo) {
          try {
            await (this.wal as any).truncateUpTo(walOffset);
          } catch (e) {}
        }
      }
    } catch (e) {
      // best-effort: if truncation or manifest update fails, keep WAL as-is to avoid data loss
    }
  }

  async close() {
    // stop GC worker
    this.stopWalGC();
    this.stopBackgroundCompaction();
    await this.wal.close();
  }

  private startWalGC() {
    if (this.walGCTimer) return;
    const run = async () => {
      try {
        const files = this.manifest.listFiles();
        const walOffsets = files
          .map((f) =>
            typeof f.walOffset === "number" ? f.walOffset : undefined
          )
          .filter((x) => typeof x === "number") as number[];
        if (walOffsets.length === 0) return;
        const minRef = Math.min(...walOffsets);
        // remove segment files older than minRef. WAL segments use pattern: log.wal.seg.<ts>.<offset>
        const all = fs.readdirSync(this.dir || "./data");
        const prefix = this.walFile + ".seg.";
        for (const fn of all) {
          if (!fn.startsWith(prefix)) continue;
          const parts = fn.split(".");
          const offPart = parts[parts.length - 1];
          const off = Number(offPart);
          if (!isNaN(off) && off <= minRef) {
            try {
              fs.unlinkSync(join((this.wal as any).rootDir || this.dir, fn));
            } catch {}
          }
        }
      } catch (e) {
        // ignore GC errors
      }
    };
    this.walGCTimer = setInterval(run, 5000) as unknown as NodeJS.Timeout;
  }

  private deleteMarkerTimer: NodeJS.Timeout | null = null;
  private startDeleteMarkerGC() {
    if (this.deleteMarkerTimer) return;
    const interval =
      typeof this.opts?.gcIntervalMs === "number"
        ? this.opts!.gcIntervalMs!
        : 60 * 1000; // default 1m
    this.deleteMarkerTimer = setInterval(() => {
      try {
        const res = cleanupDeleteMarkers(this.dir, {
          staleMs: this.opts?.deleteMarkerStaleMs,
          log: true,
        });
        this.metrics.gc.deleteMarkersScanned += res.markers;
        this.metrics.gc.deleteMarkersRemoved += res.removed;
        this.metrics.gc.filesDeleted += res.filesDeleted;
        this.metrics.gc.runs++;
      } catch (e) {
        // ignore
      }
    }, interval) as unknown as NodeJS.Timeout;
  }

  private stopDeleteMarkerGC() {
    if (this.deleteMarkerTimer) {
      clearInterval(this.deleteMarkerTimer as any);
      this.deleteMarkerTimer = null;
    }
  }

  private stopWalGC() {
    if (this.walGCTimer) {
      clearInterval(this.walGCTimer as any);
      this.walGCTimer = null;
    }
  }

  // returns minimum active snapshot revision or undefined
  getMinActiveRev(): number | undefined {
    let min: number | undefined = undefined;
    for (const r of this.activeSnapshotCounts.keys()) {
      if (typeof min !== "number" || r < min) min = r;
    }
    return min;
  }

  // returns maximum active snapshot revision or undefined
  getMaxActiveRev(): number | undefined {
    let max: number | undefined = undefined;
    for (const r of this.activeSnapshotCounts.keys()) {
      if (typeof max !== "number" || r > max) max = r;
    }
    return max;
  }
}

export default Engine;
