import { existsSync } from "node:fs";
import Manifest from "./manifest";
import { varintLen } from "./helper";
import { requestDelete } from "./file_refcount";
import { kWayMerge } from "./merge_iterator";
import { SSTReader } from "./sstreader";
import { SSTWriter } from "./sstwriter";
import { maybeConsumePerEntry } from "./throttle";

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
  // Compression options passed to SSTWriter: algorithm id and minimum block size to attempt compression
  compressionAlgo?: number; // 0 = none, 1 = deflate
  compressionThreshold?: number; // bytes, minimum block size to try compression
  adaptiveCompression?: boolean;
  compressionSampleSize?: number;
  minCompressionRatio?: number;
  // When true, perform durable fsyncs on tmp files and parent dir after rename for strict atomicity.
  // This may be slower but reduces window for lost renames on crash.
  strictAtomicity?: boolean;
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

  async compact(): Promise<{
    bytesWritten: number;
    filesCreated: number;
    compressedBlocks?: number;
    compressionAttempts?: number;
  }> {
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
    // Build per-level size targets. If user provided perLevelMax use it; otherwise
    // compute sensible defaults (exponential growth per level) so compaction can
    // be size-target driven even when not explicitly configured.
    const userProvidedPerLevel = Array.isArray(this.opts.perLevelMax);
    const perLevelMaxArr: number[] = userProvidedPerLevel
      ? (this.opts.perLevelMax as number[])
      : (() => {
          const base = globalMaxSstSize; // default base target for level0
          const arr: number[] = [];
          for (let i = 0; i < maxLevels; i++)
            arr.push(Math.floor(base * Math.pow(10, i)));
          return arr;
        })();
    // compaction stats
    let totalBytesWritten = 0;
    let totalFilesCreated = 0;
    let totalCompressedBlocks = 0;
    let totalCompressionAttempts = 0;
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
    const tombstoneRetentionMs: number | undefined = (this.opts as any)
      ?.tombstoneRetentionMs;
    const nowMs =
      typeof this.opts.timeProvider === "function"
        ? this.opts.timeProvider()
        : Date.now();
    for (let level = 0; level < maxLevels; level++) {
      // determine per-output SST max size for this compaction target level (nextLevel)
      // If user explicitly provided perLevelMax, use it; otherwise default to globalMaxSstSize
      const maxSstSize: number = userProvidedPerLevel
        ? perLevelMaxArr[level + 1] ?? globalMaxSstSize
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

      // Special-case level 0: only compact level 0 when its total size exceeds the target.
      if (level === 0) {
        // If user explicitly provided per-level targets, only compact level 0 when it exceeds the target.
        if (userProvidedPerLevel) {
          const level0Size = filesAtLevel.reduce(
            (s, f) => s + (f.size || 0),
            0
          );
          const level0Target = perLevelMaxArr[0];
          if (typeof level0Target === "number" && level0Size <= level0Target) {
            // nothing to do for level 0 if it is within target
            continue;
          }
        }
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
          const srcWal =
            group[idx] && typeof (group[idx] as any).walOffset === "number"
              ? (group[idx] as any).walOffset
              : undefined;
          const srcCreated =
            group[idx] && typeof (group[idx] as any).createdAt === "number"
              ? (group[idx] as any).createdAt
              : undefined;
          if (process.env.KV_COMPACTOR_DEBUG) {
            try {
              console.error(
                "[compactor-debug] source meta[%d]=%o",
                idx,
                group[idx]
              );
            } catch (e) {}
          }
          return (async function* () {
            for await (const it of r.iterator()) {
              // include source metadata so compaction can make time-based tombstone decisions
              yield Object.assign({}, it, {
                walOffsetSrc: srcWal,
                createdAtSrc: srcCreated,
              });
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
          // collect compression stats if available
          if (m && typeof m.compressedBlocks === "number")
            totalCompressedBlocks += m.compressedBlocks;
          if (m && typeof m.totalBlocks === "number")
            totalCompressionAttempts += m.totalBlocks;
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
            const writerOpts: any = {};
            if (this.opts && (this.opts as any).strictAtomicity)
              writerOpts.strictAtomicity = true;
            if (
              this.opts &&
              typeof (this.opts as any).compressionAlgo === "number"
            )
              writerOpts.compressionAlgo = (this.opts as any).compressionAlgo;
            if (
              this.opts &&
              typeof (this.opts as any).compressionThreshold === "number"
            )
              writerOpts.compressionThreshold = (
                this.opts as any
              ).compressionThreshold;
            if (
              this.opts &&
              typeof (this.opts as any).adaptiveCompression === "boolean"
            )
              writerOpts.adaptiveCompression = (
                this.opts as any
              ).adaptiveCompression;
            if (
              this.opts &&
              typeof (this.opts as any).compressionSampleSize === "number"
            )
              writerOpts.compressionSampleSize = (
                this.opts as any
              ).compressionSampleSize;
            if (
              this.opts &&
              typeof (this.opts as any).minCompressionRatio === "number"
            )
              writerOpts.minCompressionRatio = (
                this.opts as any
              ).minCompressionRatio;
            curWriter = new SSTWriter(tmp, outFile, writerOpts);
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
            const entryCreated =
              typeof this.opts.timeProvider === "function"
                ? this.opts.timeProvider()
                : nowMs;
            // throttle per-entry based on estimated bytes that will be appended
            // per-entry throttling
            await maybeConsumePerEntry(tokenBucket, curWriter, key, val, e.rev);
            curWriter!.add(
              key,
              val,
              e.rev,
              (e as any).walOffsetSrc,
              entryCreated
            );
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
                try {
                  console.error(
                    "[compactor-debug] tombstone expired key=%s rev=%s srcCreated=%s nowMs=%s retention=%s",
                    key && key.toString ? key.toString() : "<nil>",
                    String(e.rev),
                    String(srcCreated),
                    String(nowMs),
                    String(tombstoneRetentionMs)
                  );
                } catch (e) {}
              }
              let promoted = false;
              const cands: any[] | undefined = (e as any).candidates;
              if (Array.isArray(cands) && cands.length > 0) {
                // choose highest-rev non-tombstone candidate
                let bestCand: any | null = null;
                for (const cand of cands) {
                  if (!cand || cand.value === null) continue;
                  const cr = typeof cand.rev === "number" ? cand.rev : 0;
                  if (
                    !bestCand ||
                    typeof bestCand.rev !== "number" ||
                    cr > bestCand.rev
                  )
                    bestCand = cand;
                }
                if (bestCand) {
                  if (process.env.KV_COMPACTOR_DEBUG) {
                    try {
                      console.error(
                        "[compactor-debug] promoting best candidate key=%s rev=%s val=%s",
                        key && key.toString ? key.toString() : "<nil>",
                        String(bestCand.rev),
                        bestCand.value && bestCand.value.toString
                          ? bestCand.value.toString()
                          : "<nil>"
                      );
                    } catch (e) {}
                  }
                  const entryCreated =
                    typeof this.opts.timeProvider === "function"
                      ? this.opts.timeProvider()
                      : nowMs;
                  // per-entry token-bucket throttling for promoted candidates
                  // per-entry throttling for promoted candidates
                  await maybeConsumePerEntry(
                    tokenBucket,
                    curWriter,
                    key,
                    bestCand.value,
                    bestCand.rev
                  );
                  curWriter!.add(
                    key,
                    bestCand.value,
                    bestCand.rev,
                    (bestCand as any).walOffsetSrc,
                    entryCreated
                  );
                  promoted = true;
                }
              }
              if (!promoted) skipTombstone = true;
            }
          }
          if (!skipTombstone) {
            const entryCreated =
              typeof this.opts.timeProvider === "function"
                ? this.opts.timeProvider()
                : nowMs;
            // per-entry throttling
            await maybeConsumePerEntry(tokenBucket, curWriter, key, val, e.rev);
            curWriter!.add(
              key,
              val,
              e.rev,
              (e as any).walOffsetSrc,
              entryCreated
            );
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

      // strategy: if perLevelMax is provided, repeatedly pick a small set of files
      // from this level to compact until the level meets its size target. Otherwise
      // fall back to the previous one-by-one grouping behavior.
      const processed = new Set<string>();
      const compactedNext = new Set<string>();

      const levelTargetDefined =
        this.opts.perLevelMax && Array.isArray(this.opts.perLevelMax);
      const levelTarget = levelTargetDefined
        ? (this.opts.perLevelMax as number[])[level]
        : undefined;

      if (typeof levelTarget === "number" && levelTarget >= 0) {
        // repeatedly compact until this level's total size <= levelTarget
        let levelFiles = this.manifest
          .listFilesByLevel(level)
          .filter((f) => !created.has(f.file));

        const computeLevelSize = (files: any[]) =>
          files.reduce((s, f) => s + (f.size || 0), 0);

        let levelSize = computeLevelSize(levelFiles);

        while (levelFiles.length > 0 && levelSize > levelTarget) {
          // 1) Primary: overlap-weighted selection. Compute how many files in next
          //    level each candidate overlaps; prefer files with high overlapCount and
          //    small size (score = overlapCount / size). Select until required reduction
          //    or maxFilesPerCompaction reached.
          const requiredReduction = Math.max(1, levelSize - levelTarget);

          const scored = levelFiles.map((f) => {
            const overlap = filesNext.reduce(
              (c, n) => (overlaps(f, n) ? c + 1 : c),
              0
            );
            const size = f && typeof f.size === "number" ? f.size : 0;
            const score = overlap > 0 ? overlap / Math.max(1, size) : 0;
            return { f, overlap, size, score };
          });

          scored.sort((a, b) => {
            if (b.score !== a.score) return b.score - a.score;
            if (b.overlap !== a.overlap) return b.overlap - a.overlap;
            return a.size - b.size;
          });

          let selected: any[] = [];
          let selSum = 0;
          for (const s of scored) {
            if (selected.length >= maxFilesPerCompaction) break;
            if (s.score <= 0) break; // remaining files have no overlap, stop
            selected.push(s.f);
            selSum += s.size || 0;
            if (selSum >= requiredReduction) break;
          }

          // 2) Final fallback (preferred): smallest-files-first if overlap
          //    selection didn't meet the reduction target. Try smallest files
          //    first to make selection deterministic for tests.
          if (selSum < requiredReduction) {
            const bySize = levelFiles
              .slice()
              .sort((a, b) => (a.size || 0) - (b.size || 0));
            let ssum = selSum;
            const already = new Set((selected || []).map((x) => x.file));
            for (const f of bySize) {
              if (selected.length >= maxFilesPerCompaction) break;
              if (already.has(f.file)) continue;
              selected.push(f);
              ssum += f.size || 0;
              if (ssum >= requiredReduction) break;
            }
            selSum = ssum;
          }

          // 3) Fallback to range-coalescing (smallest-window by key order) if still
          //    insufficient after trying smallest-first selection.
          if (selSum < requiredReduction) {
            const filesByKey = levelFiles.slice().sort((a, b) => {
              const aMin =
                a && a.minKeyHex
                  ? Buffer.from(a.minKeyHex, "hex")
                  : Buffer.alloc(0);
              const bMin =
                b && b.minKeyHex
                  ? Buffer.from(b.minKeyHex, "hex")
                  : Buffer.alloc(0);
              if (aMin.length === 0 && bMin.length === 0) return 0;
              if (aMin.length === 0) return -1;
              if (bMin.length === 0) return 1;
              return Buffer.compare(aMin, bMin);
            });

            let bestWindow: any[] | null = null;
            let bestSum = Infinity;
            for (let i = 0; i < filesByKey.length; i++) {
              let sum = 0;
              for (
                let j = i;
                j < filesByKey.length && j < i + maxFilesPerCompaction;
                j++
              ) {
                const f = filesByKey[j];
                if (!f) continue;
                sum += f.size || 0;
                if (sum >= requiredReduction) {
                  if (sum < bestSum) {
                    bestSum = sum;
                    bestWindow = filesByKey.slice(i, j + 1);
                  }
                  break;
                }
              }
            }

            if (bestWindow && bestWindow.length > 0) {
              selected = bestWindow;
              selSum = bestWindow.reduce((s, x) => s + (x.size || 0), 0);
            }
          }

          if (selected.length === 0) break; // nothing to compact

          // include overlapping files from next level
          const overlapsNext = filesNext.filter(
            (n) =>
              !compactedNext.has(n.file) && selected.some((s) => overlaps(s, n))
          );

          const group = Array.from(new Set([...selected, ...overlapsNext]));

          const readers: SSTReader[] = [];
          for (const g of group) {
            try {
              if (!existsSync(g.file)) continue;
              readers.push(SSTReader.open(g.file));
            } catch (e) {}
          }

          if (readers.length === 0) {
            // mark them processed so we won't loop forever
            for (const g of group) processed.add(g.file);
            break;
          }

          const iterables = readers.map((r, idx) => {
            const srcWal =
              group[idx] && typeof (group[idx] as any).walOffset === "number"
                ? (group[idx] as any).walOffset
                : undefined;
            const srcCreated =
              group[idx] && typeof (group[idx] as any).createdAt === "number"
                ? (group[idx] as any).createdAt
                : undefined;
            return (async function* () {
              for await (const it of r.iterator()) {
                yield Object.assign({}, it, {
                  walOffsetSrc: srcWal,
                  createdAtSrc: srcCreated,
                });
              }
            })();
          });

          // write merged output (re-using existing rotation logic)
          const metas: any[] = [];
          let curWriter: SSTWriter | null = null;
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
            if (m && typeof m.compressedBlocks === "number")
              totalCompressedBlocks += m.compressedBlocks;
            if (m && typeof m.totalBlocks === "number")
              totalCompressionAttempts += m.totalBlocks;
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
          }

          for await (const e of kWayMerge(iterables)) {
            const key = e.key;
            const val = e.value;
            const entrySize = (key?.length || 0) + (val ? val.length : 0) + 8;
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
              await rotateWriter();
              const outFile = `${
                this.dir
              }/compacted_${Date.now()}_${Math.random()
                .toString(16)
                .slice(2)}.sst`;
              const tmp = `${outFile}.tmp`;
              const writerOpts2: any = {};
              if (this.opts && (this.opts as any).strictAtomicity)
                writerOpts2.strictAtomicity = true;
              if (
                this.opts &&
                typeof (this.opts as any).compressionAlgo === "number"
              )
                writerOpts2.compressionAlgo = (
                  this.opts as any
                ).compressionAlgo;
              if (
                this.opts &&
                typeof (this.opts as any).compressionThreshold === "number"
              )
                writerOpts2.compressionThreshold = (
                  this.opts as any
                ).compressionThreshold;
              if (
                this.opts &&
                typeof (this.opts as any).adaptiveCompression === "boolean"
              )
                writerOpts2.adaptiveCompression = (
                  this.opts as any
                ).adaptiveCompression;
              if (
                this.opts &&
                typeof (this.opts as any).compressionSampleSize === "number"
              )
                writerOpts2.compressionSampleSize = (
                  this.opts as any
                ).compressionSampleSize;
              if (
                this.opts &&
                typeof (this.opts as any).minCompressionRatio === "number"
              )
                writerOpts2.minCompressionRatio = (
                  this.opts as any
                ).minCompressionRatio;
              curWriter = new SSTWriter(tmp, outFile, writerOpts2);
            }

            // tombstone logic and per-entry throttling (reuse existing code)
            let skipTombstone = false;
            const srcCreated = (e as any).createdAtSrc;
            if (
              val === null &&
              typeof e.rev === "number" &&
              typeof maxActiveRev === "number" &&
              e.rev <= maxActiveRev
            ) {
              skipTombstone = false;
              const entryCreated =
                typeof this.opts.timeProvider === "function"
                  ? this.opts.timeProvider()
                  : nowMs;
              await maybeConsumePerEntry(
                tokenBucket,
                curWriter,
                key,
                val,
                e.rev
              );
              curWriter!.add(
                key,
                val,
                e.rev,
                (e as any).walOffsetSrc,
                entryCreated
              );
              continue;
            }
            if (
              !skipTombstone &&
              val === null &&
              typeof srcCreated === "number" &&
              typeof tombstoneRetentionMs === "number"
            ) {
              const ageMs = nowMs - srcCreated;
              if (ageMs > tombstoneRetentionMs) skipTombstone = true;
            }
            if (!skipTombstone) {
              const entryCreated =
                typeof this.opts.timeProvider === "function"
                  ? this.opts.timeProvider()
                  : nowMs;
              await maybeConsumePerEntry(
                tokenBucket,
                curWriter,
                key,
                val,
                e.rev
              );
              curWriter!.add(
                key,
                val,
                e.rev,
                (e as any).walOffsetSrc,
                entryCreated
              );
            }
          }

          await rotateWriter();

          // set walOffset and persist
          const walOff = this.manifest.getWalOffset();
          for (const mm of metas) mm.walOffset = walOff;
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

          // recompute levelFiles/levelSize for the while loop
          levelFiles = this.manifest
            .listFilesByLevel(level)
            .filter((f) => !created.has(f.file));
          levelSize = computeLevelSize(levelFiles);
        }
      } else {
        // legacy behavior: iterate files and compact/promote individually
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
            const srcWal =
              group[idx] && typeof (group[idx] as any).walOffset === "number"
                ? (group[idx] as any).walOffset
                : undefined;
            const srcCreated =
              group[idx] && typeof (group[idx] as any).createdAt === "number"
                ? (group[idx] as any).createdAt
                : undefined;
            return (async function* () {
              for await (const it of r.iterator()) {
                yield Object.assign({}, it, {
                  walOffsetSrc: srcWal,
                  createdAtSrc: srcCreated,
                });
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
            if (m && typeof m.compressedBlocks === "number")
              totalCompressedBlocks += m.compressedBlocks;
            if (m && typeof m.totalBlocks === "number")
              totalCompressionAttempts += m.totalBlocks;
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
              const outFile = `${
                this.dir
              }/compacted_${Date.now()}_${Math.random()
                .toString(16)
                .slice(2)}.sst`;
              const tmp = `${outFile}.tmp`;
              curTmp = tmp;
              curOut = outFile;
              const writerOpts3: any = {};
              if (this.opts && (this.opts as any).strictAtomicity)
                writerOpts3.strictAtomicity = true;
              if (
                this.opts &&
                typeof (this.opts as any).compressionAlgo === "number"
              )
                writerOpts3.compressionAlgo = (
                  this.opts as any
                ).compressionAlgo;
              if (
                this.opts &&
                typeof (this.opts as any).compressionThreshold === "number"
              )
                writerOpts3.compressionThreshold = (
                  this.opts as any
                ).compressionThreshold;
              if (
                this.opts &&
                typeof (this.opts as any).adaptiveCompression === "boolean"
              )
                writerOpts3.adaptiveCompression = (
                  this.opts as any
                ).adaptiveCompression;
              if (
                this.opts &&
                typeof (this.opts as any).compressionSampleSize === "number"
              )
                writerOpts3.compressionSampleSize = (
                  this.opts as any
                ).compressionSampleSize;
              if (
                this.opts &&
                typeof (this.opts as any).minCompressionRatio === "number"
              )
                writerOpts3.minCompressionRatio = (
                  this.opts as any
                ).minCompressionRatio;
              curWriter = new SSTWriter(tmp, outFile, writerOpts3);
              approxSize = 0;
            }
            if (curMin === null || Buffer.compare(key, curMin) < 0)
              curMin = key;
            if (curMax === null || Buffer.compare(key, curMax) > 0)
              curMax = key;
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
              const entryCreated =
                typeof this.opts.timeProvider === "function"
                  ? this.opts.timeProvider()
                  : nowMs;
              // per-entry throttling
              await maybeConsumePerEntry(
                tokenBucket,
                curWriter,
                key,
                val,
                e.rev
              );
              curWriter!.add(
                key,
                val,
                e.rev,
                (e as any).walOffsetSrc,
                entryCreated
              );
              approxSize += entrySize;
              continue;
            }
            if (
              !skipTombstone &&
              val === null &&
              typeof srcCreated === "number" &&
              typeof tombstoneRetentionMs === "number"
            ) {
              const ageMs = nowMs - srcCreated;
              if (ageMs > tombstoneRetentionMs) skipTombstone = true;
            }
            if (!skipTombstone) {
              const entryCreated =
                typeof this.opts.timeProvider === "function"
                  ? this.opts.timeProvider()
                  : nowMs;
              if (tokenBucket) {
                try {
                  const delta =
                    typeof (curWriter as any).deltaSizeForEntry === "function"
                      ? (curWriter as any).deltaSizeForEntry(key, val, e.rev)
                      : (key?.length || 0) + (val ? val.length : 0) + 10;
                  if (delta > 0) await tokenBucket.consume(delta);
                } catch (e) {}
              }
              curWriter!.add(
                key,
                val,
                e.rev,
                (e as any).walOffsetSrc,
                entryCreated
              );
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
    }
    return {
      bytesWritten: totalBytesWritten,
      filesCreated: totalFilesCreated,
      compressedBlocks: totalCompressedBlocks,
      compressionAttempts: totalCompressionAttempts,
    };
  }
}

export default Compactor;
