import { existsSync } from "node:fs";
import Manifest from "./manifest";
import { varintLen } from "./helper";
import { requestDelete } from "./file_refcount";
import { kWayMerge } from "./merge_iterator";
import { SSTReader } from "./sstreader";
import { SSTWriter } from "./sstwriter";

/**
 * Simple compactor: given a Manifest, pick all files, merge their entries and write a single new SST,
 * then replace manifest entries atomically and delete old files. This is a naive single-level compaction
 * suitable for prototypes and tests.
 */
export type CompactorOptions = {
  maxFilesPerCompaction?: number; // cap number of input files per compaction group
  maxSstSize?: number; // bytes per output SST
  perLevelMax?: number[]; // optional per-level size caps (not used yet)
  // optional throttling: bytes per second to limit compaction IO (0 = unlimited)
  bytesPerSecond?: number;
  // cooperative yielding: how many entries to process before checking throttle
  entriesPerYield?: number;
  // optional tombstone retention expressed as WAL-byte-age: if the current manifest walOffset minus
  // the source SST's walOffset or creation timestamp may be used to drop tombstones older than
  // a configured retention. Use either WAL-byte-age or wall-time TTL.
  tombstoneRetentionWalBytes?: number;
  tombstoneRetentionMs?: number;
  // Optional time provider for deterministic tests (returns epoch ms)
  timeProvider?: () => number;
};

export class Compactor {
  private opts: CompactorOptions;
  constructor(
    private dir: string,
    private manifest: Manifest,
    opts?: CompactorOptions
  ) {
    this.opts = opts || {};
  }

  async compact(): Promise<{ bytesWritten: number; filesCreated: number }> {
    // Multi-level compaction: for each level, pick candidate files and overlapping files in next level
    // and compact them into the next level. If a file has no overlap in next level, promote it.
    const maxLevels = 6; // scan levels 0..5
    const bufFromHex = (h: string) =>
      h ? Buffer.from(h, "hex") : Buffer.alloc(0);
    const overlaps = (
      a: { minKeyHex: string; maxKeyHex: string },
      b: { minKeyHex: string; maxKeyHex: string }
    ) => {
      const aMin = bufFromHex(a.minKeyHex);
      const aMax = bufFromHex(a.maxKeyHex);
      const bMin = bufFromHex(b.minKeyHex);
      const bMax = bufFromHex(b.maxKeyHex);
      if (
        aMin.length === 0 ||
        aMax.length === 0 ||
        bMin.length === 0 ||
        bMax.length === 0
      )
        return true;
      if (Buffer.compare(aMax, bMin) < 0) return false;
      if (Buffer.compare(aMin, bMax) > 0) return false;
      return true;
    };

    const created = new Set<string>();
    const maxFilesPerCompaction = this.opts.maxFilesPerCompaction || 20;
    const globalMaxSstSize = this.opts.maxSstSize || 16 * 1024;
    // compaction stats
    let totalBytesWritten = 0;
    let totalFilesCreated = 0;
    const throttleBps =
      this.opts.bytesPerSecond && this.opts.bytesPerSecond > 0
        ? this.opts.bytesPerSecond
        : 0;
    const entriesPerYield =
      this.opts.entriesPerYield && this.opts.entriesPerYield > 0
        ? this.opts.entriesPerYield
        : 64;

    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

    // Token-bucket throttler for smooth bytes/sec limiting. Capacity defaults to 1s worth
    // of tokens. consume(n) will wait until enough tokens are available.
    class TokenBucket {
      private tokens: number;
      private lastRefill: number;
      private readonly ratePerMs: number;
      private readonly capacity: number;
      constructor(rateBps: number, capacityMs = 1000) {
        this.ratePerMs = rateBps / 1000;
        this.capacity = Math.max(1, Math.floor((rateBps * capacityMs) / 1000));
        this.tokens = this.capacity;
        this.lastRefill = Date.now();
      }
      private refill() {
        const now = Date.now();
        const dt = now - this.lastRefill;
        if (dt <= 0) return;
        this.tokens = Math.min(
          this.capacity,
          this.tokens + dt * this.ratePerMs
        );
        this.lastRefill = now;
      }
      async consume(n: number) {
        if (!Number.isFinite(n) || n <= 0) return;
        // If ratePerMs is zero, treat as unlimited (shouldn't happen because throttleBps>0 to construct)
        while (true) {
          this.refill();
          if (this.tokens >= n) {
            this.tokens -= n;
            return;
          }
          const need = n - this.tokens;
          // compute ms to wait for required tokens
          const waitMs = Math.max(1, Math.ceil(need / this.ratePerMs));
          await sleep(waitMs);
        }
      }
    }

    const tokenBucket = throttleBps > 0 ? new TokenBucket(throttleBps) : null;
  const minActiveRev: number | undefined = (this.opts as any)?.minActiveRev;
  const maxActiveRev: number | undefined = (this.opts as any)?.maxActiveRev;
  const tombstoneRetentionMs: number | undefined = (this.opts as any)?.tombstoneRetentionMs;
  const nowMs = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : Date.now();
    for (let level = 0; level < maxLevels; level++) {
      // determine per-output SST max size for this compaction target level (nextLevel)
      const maxSstSize =
        this.opts.perLevelMax && Array.isArray(this.opts.perLevelMax)
          ? this.opts.perLevelMax[level + 1] ?? globalMaxSstSize
          : globalMaxSstSize;
      // skip files created earlier in this compaction run
      const filesAtLevel = this.manifest
        .listFilesByLevel(level)
        .filter((f) => !created.has(f.file));
      if (filesAtLevel.length === 0) continue;
      const nextLevel = level + 1;
      const filesNext = this.manifest
        .listFilesByLevel(nextLevel)
        .filter((f) => !created.has(f.file));

      // Special-case level 0: compact all files in level 0 together (plus any overlapping files in level1)
      if (level === 0) {
        // pick up to maxFilesPerCompaction from level0
        const group: typeof filesAtLevel = [];
        for (
          let i = 0;
          i < filesAtLevel.length && group.length < maxFilesPerCompaction;
          i++
        ) {
          const f = filesAtLevel[i];
          if (f) group.push(f);
        }
        // include any level1 files that overlap any level0 file
        for (const n of filesNext) {
          for (const f of filesAtLevel) {
            if (overlaps(f, n)) {
              group.push(n);
              break;
            }
          }
        }

        const readers: SSTReader[] = [];
        for (const g of group) {
          try {
            if (!existsSync(g.file)) continue;
            readers.push(SSTReader.open(g.file));
          } catch (e) {}
        }

        if (readers.length === 0) continue;

        // wrap each reader.iterator() to attach the source SST walOffset so compaction
        // can make age/offset-based decisions (e.g., tombstone TTL removal).
        const iterables = readers.map((r, idx) => {
          const srcWal = (group[idx] && typeof (group[idx] as any).walOffset === 'number') ? (group[idx] as any).walOffset : undefined;
          const srcCreated = (group[idx] && typeof (group[idx] as any).createdAt === 'number') ? (group[idx] as any).createdAt : undefined;
          if (process.env.KV_COMPACTOR_DEBUG) {
            try { console.error('[compactor-debug] source meta[%d]=%o', idx, group[idx]); } catch (e) {}
          }
          return (async function* () {
            for await (const it of r.iterator()) {
              // include source metadata so compaction can make time-based tombstone decisions
              yield Object.assign({}, it, { walOffsetSrc: srcWal, createdAtSrc: srcCreated });
            }
          })();
        });

        // Split merged output into multiple SSTs each <= maxSstSize (computed per-target-level)
        const metas: any[] = [];
        let curWriter: SSTWriter | null = null;
        let curTmp: string | null = null;
        let curOut: string | null = null;
        let curMin: Buffer | null = null;
        let curMax: Buffer | null = null;

        async function rotateWriter() {
          if (!curWriter) return;
          // estimate size before performing the synchronous finish/write so we can throttle
          const estimated =
            typeof (curWriter as any).getEstimatedSize === "function"
              ? (curWriter as any).getEstimatedSize()
              : 0;
          if (tokenBucket && estimated > 0) {
            await tokenBucket.consume(estimated);
          }
          const m = curWriter.finish();
          metas.push({
            file: m.file,
            minKeyHex: m.minKey.toString("hex"),
            maxKeyHex: m.maxKey.toString("hex"),
            size: m.size,
            level: nextLevel,
            walOffset: 0,
            createdAt: Date.now(),
          });
          totalBytesWritten += m.size;
          totalFilesCreated += 1;
          curWriter = null;
          curTmp = null;
          curOut = null;
          curMin = null;
          curMax = null;
        }

        let approxSize = 0;
        let processed = 0;
        for await (const e of kWayMerge(iterables)) {
          const key = e.key;
          const val = e.value;
          // prefer simulated exact estimate from SSTWriter when available (zero-tolerance)
          const entrySize =
            (key?.length || 0) +
            (val ? val.length : 0) +
            (typeof e.rev === "number" ? varintLen(e.rev) : 10); // fallback rough
          let willExceed = false;
          if (
            curWriter &&
            typeof (curWriter as any).simulatedSizeAfterAdd === "function"
          ) {
            const sim = (curWriter as any).simulatedSizeAfterAdd(
              key,
              val,
              e.rev
            );
            willExceed = sim > maxSstSize;
          } else if (curWriter) {
            willExceed =
              curWriter.estimateSizeAfterAdd(key, val, e.rev) > maxSstSize;
          } else {
            willExceed = entrySize > maxSstSize;
          }
          if (!curWriter || willExceed) {
            // rotate
            await rotateWriter();
            const outFile = `${this.dir}/compacted_${Date.now()}_${Math.random()
              .toString(16)
              .slice(2)}.sst`;
            const tmp = `${outFile}.tmp`;
            curTmp = tmp;
            curOut = outFile;
            // propagate writer options from compactor if any
            curWriter = new SSTWriter(tmp, outFile);
            approxSize = 0;
          }
          if (curMin === null || Buffer.compare(key, curMin) < 0) curMin = key;
          if (curMax === null || Buffer.compare(key, curMax) > 0) curMax = key;
          // Tombstone GC: respect minActiveRev, then optional time-based TTL from source SST
          let skipTombstone = false;
          const srcCreated = (e as any).createdAtSrc;
          if (
            val === null &&
            typeof e.rev === "number" &&
            typeof maxActiveRev === "number" &&
            e.rev <= maxActiveRev
          ) {
            // At least one active snapshot is at or after this tombstone's revision,
            // so we must preserve the tombstone for snapshot correctness.
            skipTombstone = false;
            // write tombstone as-is; preserve source walOffset/createdAt if available else use now
            const entryCreated = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : nowMs;
            curWriter!.add(key, val, e.rev, (e as any).walOffsetSrc, entryCreated);
            approxSize += entrySize;
            processed += 1;
            if (processed >= entriesPerYield) {
              processed = 0;
              await sleep(0);
            }
            continue;
          }
          if (
            !skipTombstone &&
            val === null &&
            typeof srcCreated === "number" &&
            typeof tombstoneRetentionMs === "number"
          ) {
            const ageMs = nowMs - srcCreated;
            if (ageMs > tombstoneRetentionMs) {
              // Tombstone expired by TTL. Try to promote next-highest candidate from merged list
              if (process.env.KV_COMPACTOR_DEBUG) {
                try { console.error('[compactor-debug] tombstone expired key=%s rev=%s srcCreated=%s nowMs=%s retention=%s', key && key.toString ? key.toString() : '<nil>', String(e.rev), String(srcCreated), String(nowMs), String(tombstoneRetentionMs)); } catch (e) {}
              }
              let promoted = false;
              const cands: any[] | undefined = (e as any).candidates;
              if (Array.isArray(cands) && cands.length > 0) {
                // choose highest-rev non-tombstone candidate
                let bestCand: any | null = null;
                for (const cand of cands) {
                  if (!cand || cand.value === null) continue;
                  const cr = typeof cand.rev === 'number' ? cand.rev : 0;
                  if (!bestCand || (typeof bestCand.rev !== 'number' || cr > bestCand.rev)) bestCand = cand;
                }
                if (bestCand) {
                  if (process.env.KV_COMPACTOR_DEBUG) {
                    try { console.error('[compactor-debug] promoting best candidate key=%s rev=%s val=%s', key && key.toString ? key.toString() : '<nil>', String(bestCand.rev), bestCand.value && bestCand.value.toString ? bestCand.value.toString() : '<nil>'); } catch (e) {}
                  }
                  const entryCreated = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : nowMs;
                  curWriter!.add(key, bestCand.value, bestCand.rev, (bestCand as any).walOffsetSrc, entryCreated);
                  promoted = true;
                }
              }
              if (!promoted) skipTombstone = true;
            }
          }
          if (!skipTombstone) {
            const entryCreated = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : nowMs;
            curWriter!.add(key, val, e.rev, (e as any).walOffsetSrc, entryCreated);
          }
          approxSize += entrySize;
          processed += 1;
          // cooperative yielding to keep event loop responsive; main throttling is enforced
          // by the token-bucket when rotating/finishing SST files.
          if (processed >= entriesPerYield) {
            processed = 0;
            await sleep(0);
          }
        }
        // flush final
        await rotateWriter();

        // set walOffset for metas
        const walOff = this.manifest.getWalOffset();
        for (const mm of metas) mm.walOffset = walOff;
        // tokenBucket consumed during rotateWriter; no additional window accounting required here

        // mark created files and persist new metas replacing oldPaths
        const oldPaths = group.map((g) => g.file);
        for (const mm of metas) created.add(mm.file);
        this.manifest.replaceFiles(oldPaths, metas);

        for (const p of oldPaths) {
          try {
            if (existsSync(p)) requestDelete(p);
          } catch {}
        }

        // done with level 0 compaction
        continue;
      }

      const processed = new Set<string>();
      const compactedNext = new Set<string>();

      for (const f of filesAtLevel) {
        if (processed.has(f.file)) continue;

        const overlapsNext = filesNext.filter(
          (n) => !compactedNext.has(n.file) && overlaps(f, n)
        );

        if (overlapsNext.length === 0) {
          // promote file to next level
          this.manifest.replaceFiles(
            [f.file],
            [
              {
                file: f.file,
                minKeyHex: f.minKeyHex,
                maxKeyHex: f.maxKeyHex,
                size: f.size,
                level: nextLevel,
                walOffset: f.walOffset,
              },
            ]
          );
          processed.add(f.file);
          continue;
        }

        // group files to compact
        const group = [f, ...overlapsNext];
        const readers: SSTReader[] = [];
        for (const g of group) {
          try {
            if (!existsSync(g.file)) continue;
            readers.push(SSTReader.open(g.file));
          } catch (e) {}
        }

        if (readers.length === 0) {
          for (const g of group) processed.add(g.file);
          continue;
        }

        const iterables = readers.map((r, idx) => {
          const srcWal = (group[idx] && typeof (group[idx] as any).walOffset === 'number') ? (group[idx] as any).walOffset : undefined;
          const srcCreated = (group[idx] && typeof (group[idx] as any).createdAt === 'number') ? (group[idx] as any).createdAt : undefined;
          return (async function* () {
            for await (const it of r.iterator()) {
              yield Object.assign({}, it, { walOffsetSrc: srcWal, createdAtSrc: srcCreated });
            }
          })();
        });

        // Split merged output into multiple SSTs each <= maxSstSize (computed per-target-level)
        const metas: any[] = [];
        let curWriter: SSTWriter | null = null;
        let curTmp: string | null = null;
        let curOut: string | null = null;
        let curMin: Buffer | null = null;
        let curMax: Buffer | null = null;

        async function rotateWriter() {
          if (!curWriter) return;
          const estimated =
            typeof (curWriter as any).getEstimatedSize === "function"
              ? (curWriter as any).getEstimatedSize()
              : 0;
          if (tokenBucket && estimated > 0) {
            await tokenBucket.consume(estimated);
          }
          const m = curWriter.finish();
            metas.push({
              file: m.file,
              minKeyHex: m.minKey.toString("hex"),
              maxKeyHex: m.maxKey.toString("hex"),
              size: m.size,
              level: nextLevel,
              walOffset: 0,
              createdAt: Date.now(),
            });
          curWriter = null;
          curTmp = null;
          curOut = null;
          curMin = null;
          curMax = null;
        }

        let approxSize = 0;
        for await (const e of kWayMerge(iterables)) {
          const key = e.key;
          const val = e.value;
          const entrySize = (key?.length || 0) + (val ? val.length : 0) + 8; // fallback rough
          let willExceed = false;
          if (
            curWriter &&
            typeof (curWriter as any).simulatedSizeAfterAdd === "function"
          ) {
            const sim = (curWriter as any).simulatedSizeAfterAdd(
              key,
              val,
              e.rev
            );
            willExceed = sim > maxSstSize;
          } else if (curWriter) {
            willExceed =
              curWriter.estimateSizeAfterAdd(key, val, e.rev) > maxSstSize;
          } else {
            willExceed = entrySize > maxSstSize;
          }
          if (!curWriter || willExceed) {
            // rotate
            await rotateWriter();
            const outFile = `${this.dir}/compacted_${Date.now()}_${Math.random()
              .toString(16)
              .slice(2)}.sst`;
            const tmp = `${outFile}.tmp`;
            curTmp = tmp;
            curOut = outFile;
            curWriter = new SSTWriter(tmp, outFile);
            approxSize = 0;
          }
          if (curMin === null || Buffer.compare(key, curMin) < 0) curMin = key;
          if (curMax === null || Buffer.compare(key, curMax) > 0) curMax = key;
          // Tombstone GC: respect minActiveRev, then optional time-based TTL from source SST
          let skipTombstone = false;
          const srcCreated = (e as any).createdAtSrc;
          if (
            val === null &&
            typeof e.rev === "number" &&
            typeof maxActiveRev === "number" &&
            e.rev <= maxActiveRev
          ) {
            // preserve for active snapshot
            skipTombstone = false;
            const entryCreated = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : nowMs;
            curWriter!.add(key, val, e.rev, (e as any).walOffsetSrc, entryCreated);
            approxSize += entrySize;
            continue;
          }
          if (
            !skipTombstone &&
            val === null &&
            typeof srcCreated === 'number' &&
            typeof tombstoneRetentionMs === 'number'
          ) {
            const ageMs = nowMs - srcCreated;
            if (ageMs > tombstoneRetentionMs) skipTombstone = true;
          }
          if (!skipTombstone) {
            const entryCreated = typeof this.opts.timeProvider === 'function' ? this.opts.timeProvider() : nowMs;
            curWriter!.add(key, val, e.rev, (e as any).walOffsetSrc, entryCreated);
          }
          approxSize += entrySize;
        }
        // flush final
        await rotateWriter();

        // set walOffset for metas
        const walOff = this.manifest.getWalOffset();
        for (const mm of metas) mm.walOffset = walOff;

        // mark created files and persist new metas replacing oldPaths
        const oldPaths = group.map((g) => g.file);
        for (const mm of metas) created.add(mm.file);
        this.manifest.replaceFiles(oldPaths, metas);

        for (const p of oldPaths) {
          try {
            if (existsSync(p)) requestDelete(p);
          } catch {}
        }

        for (const g of group) {
          processed.add(g.file);
          compactedNext.add(g.file);
        }
      }
    }
    return { bytesWritten: totalBytesWritten, filesCreated: totalFilesCreated };
  }
}

export default Compactor;
