import { open } from "node:fs/promises";
import type { WalLike } from "./types";
import fs from "node:fs";
import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";
import {
  adler32,
  dir,
  ensureDir,
  fasync,
  fileDescriptor,
  writevNative,
} from "../utils";
import { WallSched } from "./sched";

// New compact header layout to support versioning and entry types:
// 0: version (1 byte)
// 1: type    (1 byte)
// 2-3: reserved (2 bytes)
// 4-7: payload length (uint32)
// 8-11: checksum (uint32)
const HEADER_SIZE = 12;
const HEADER_LEN_OFFSET = 4;
const HEADER_CKS_OFFSET = 8;

export interface WalOptions {
  batching?: boolean;
  maxBatchSize?: number;
  maxBatchDelayMs?: number;
  backgroundFlush?: boolean;
  version?: number;
  rootDir?: string;
}

export class Wal extends WallSched implements WalLike {
  private fd: number | null = null;
  private rootDir: string = "./data";
  // Small pool for header buffers to avoid allocating 8 bytes every append.
  private headerPool: Buffer[] = [];
  // scratch buffer reused for reads to avoid per-entry allocations
  private _scratch: Buffer | null = null;
  private _scratchSize = 0;

  private getScratch(minSize: number) {
    if (!this._scratch || this._scratchSize < minSize) {
      // grow exponentially to avoid frequent reallocs
      const newSize = Math.max(minSize, this._scratchSize * 2 || 1024);
      this._scratch = Buffer.alloc(newSize);
      this._scratchSize = newSize;
    }
    return this._scratch;
  }
  // Batch queue for appends. We'll flush on demand or when batch size exceeded.
  private batchQueue: Array<Buffer> = [];
  private batchCount = 0;
  private maxBatchSize = 32; // number of entries to batch
  private batching = false;
  // simple metrics
  public metrics = {
    entriesAppended: 0,
    bytesAppended: 0,
    flushCount: 0,
    writevUsed: writevNative ? 1 : 0,
  };

  private version = 1;
  private backgroundFlush = true;
  // track current write offset (file length at end of last append/flush)
  private currentOffset: number = 0;
  // global base offset for the active WAL file (monotonic across rotations)
  private globalBase: number = 0;
  // global end offset after last append (globalBase + currentOffset)
  private globalNext: number = 0;
  // meta persistence tuning: write meta every N appends to reduce fs churn
  private metaFlushInterval = 64;
  private appendSinceMeta = 0;
  // persisted index of rotated segments: { filename -> { base, end } }
  private segmentsIndex: Record<string, { base: number; end: number }> = {};
  // Promise chain to serialize write operations and avoid races on fd/currentOffset
  private writeChain: Promise<void> = Promise.resolve();

  // Helper to serialize write operations. Ensures the provided async function
  // runs after previous writes and any thrown error does not break the chain.
  private async runWriteOp<T>(fn: () => Promise<T>): Promise<T> {
    let res: T;
    // Append to chain
    this.writeChain = this.writeChain.then(async () => {
      try {
        res = await fn();
      } catch (e) {
        // swallow here to preserve chain; rethrow outside of chain
        throw e;
      }
    });
    // Wait for the appended operation to complete and return its result
    await this.writeChain;
    // @ts-ignore - res is set by the chained function
    return res;
  }

  constructor(private file = "log.wal", opts: WalOptions = {}) {
    super();
    if (opts.rootDir) this.rootDir = opts.rootDir;
    if (opts.maxBatchSize != null) this.maxBatchSize = opts.maxBatchSize;
    if (opts.maxBatchDelayMs != null)
      this.maxBatchDelayMs = opts.maxBatchDelayMs;
    if (opts.batching != null) this.batching = opts.batching;
    if (opts.version != null) this.version = opts.version;
    if (opts.backgroundFlush != null)
      this.backgroundFlush = opts.backgroundFlush;
  }

  /**
   * Open the WAL file for reading and writing.
   * This will create the file if it does not exist.
   */
  async open() {
    await ensureDir(this.rootDir);
    // Try opening the file with a small retry loop to handle transient
    // Windows file locking (EPERM/EBUSY) when tests or other processes
    // briefly hold the file.
    const maxAttempts = 6;
    let attempt = 0;
    while (true) {
      try {
        this.fd = await fileDescriptor(dir(this.rootDir, this.file), "a+");
        break;
      } catch (err: any) {
        attempt++;
        const code = err && err.code;
        // If we've retried a few times and still hit a Windows lock, fall back
        // to a unique per-process WAL filename to avoid long blocking and test timeouts.
        if (attempt >= maxAttempts) {
          try {
            const uniqueSuffix = `pid${process.pid}.${Math.random()
              .toString(36)
              .slice(2, 8)}`;
            const original = this.file;
            this.file = `${original}.${uniqueSuffix}`;
            // ensure directory exists
            await ensureDir(this.rootDir);
            // create an empty file
            const path = dir(this.rootDir, this.file);
            try {
              fs.writeFileSync(path, "");
            } catch {}
            this.fd = await fileDescriptor(path, "a+");
            break;
          } catch (fallbackErr) {
            // if fallback also fails, rethrow the original error
            throw err;
          }
        }
        if (code !== "EPERM" && code !== "EBUSY" && code !== "EACCES") {
          throw err;
        }
        // small backoff with slight jitter
        const backoff =
          Math.min(100, 10 * attempt) + Math.floor(Math.random() * 10);
        await new Promise((res) => setTimeout(res, backoff));
      }
    }
    // Log whether native writev is available (helps benchmark analysis)
    if (writevNative) console.log("wal: native writev is available");
    else
      console.log("wal: native writev NOT available, falling back to concat");
    // Recover: trim any trailing partial/corrupt entry so future appends succeed
    await this.recoverTail();
    // refresh currentOffset
    try {
      const st = fs.statSync(dir(this.rootDir, this.file));
      this.currentOffset = st.size;
    } catch {}
    // load segments index and global base offset from meta file if present
    try {
      const metaPath = dir(this.rootDir, this.file + ".meta.json");
      const segPath = dir(this.rootDir, this.file + ".segments.json");
      if (fs.existsSync(segPath)) {
        try {
          const buf = fs.readFileSync(segPath, "utf8");
          const json = JSON.parse(buf);
          if (json && typeof json === "object") this.segmentsIndex = json;
        } catch {}
      }
      // If segmentsIndex is missing entries, try to rebuild it from existing rotated files
      try {
        const files = fs.readdirSync(this.rootDir || ".");
        const prefix = this.file + ".seg.";
        let changed = false;
        for (const f of files) {
          if (!f.startsWith(prefix)) continue;
          if (this.segmentsIndex[f]) continue;
          try {
            const st = fs.statSync(dir(this.rootDir, f));
            const parts = f.split(".");
            const offPart = parts[parts.length - 1];
            const off = Number(offPart);
            if (!isNaN(off)) {
              const end = off;
              const base = end - st.size;
              this.segmentsIndex[f] = { base, end };
              changed = true;
            }
          } catch {}
        }
        if (changed) {
          try {
            fs.writeFileSync(segPath, JSON.stringify(this.segmentsIndex));
          } catch {}
        }
      } catch {}
      if (fs.existsSync(metaPath)) {
        const buf = fs.readFileSync(metaPath, "utf8");
        const json = JSON.parse(buf);
        if (typeof json.globalBase === "number")
          this.globalBase = json.globalBase;
        if (typeof json.globalNext === "number")
          this.globalNext = json.globalNext;
      } else {
        // try to derive a reasonable globalBase from segmentsIndex (max end)
        let maxEnd = 0;
        for (const k of Object.keys(this.segmentsIndex)) {
          const e = this.segmentsIndex[k];
          if (e && typeof e.end === "number" && e.end > maxEnd) maxEnd = e.end;
        }
        if (maxEnd > 0) {
          this.globalBase = maxEnd;
        } else {
          // fallback: no explicit end offsets found; reconstruct by summing segment sizes in chronological order
          try {
            const files = fs.readdirSync(this.rootDir || ".");
            const prefix = this.file + ".seg.";
            const segs = files
              .filter((f: string) => f.startsWith(prefix))
              .sort((a: string, b: string) => {
                const tA = Number(a.split(".")[a.split(".").length - 2]) || 0;
                const tB = Number(b.split(".")[b.split(".").length - 2]) || 0;
                return tA - tB;
              });
            let acc = 0;
            for (const s of segs) {
              try {
                const st = fs.statSync(dir(this.rootDir, s));
                acc += st.size;
              } catch {}
            }
            this.globalBase = acc;
            this.globalNext = this.globalBase + this.currentOffset;
            // persist meta for next runs
            try {
              fs.writeFileSync(
                metaPath,
                JSON.stringify({
                  globalBase: this.globalBase,
                  globalNext: this.globalNext,
                })
              );
            } catch {}
          } catch {}
        }
      }
    } catch {}
    // start a background flush to ensure batches are periodically flushed
    if (this.backgroundFlush) this.startBackgroundFlush();
  }

  private writeMetaSync() {
    try {
      const metaPath = dir(this.rootDir, this.file + ".meta.json");
      fs.writeFileSync(
        metaPath,
        JSON.stringify({
          globalBase: this.globalBase,
          globalNext: this.globalNext,
        })
      );
    } catch {}
  }

  private async recoverTail() {
    // Use a bounded reverse scan from the file tail to detect a partial trailing entry.
    // Scanning the entire file on open is too slow for large WALs. We validate up to
    // `maxEntriesToCheck` entries or `maxBytesToCheck` bytes from the tail.
    const fh = await open(dir(this.rootDir, this.file), "r+");
    try {
      const st = await fh.stat();
      if (st.size === 0) return;

      let cursor = st.size;
      let lastGood = cursor;

      const maxEntriesToCheck = 4096; // bound how many entries to validate
      const maxBytesToCheck = 1024 * 1024; // 1MB
      let entriesChecked = 0;
      let bytesChecked = 0;

      // First, search a small tail window for a valid trailer position because a crash
      // may have appended a few garbage bytes after the last valid trailer. This finds
      // the true end-of-last-entry without scanning the whole file.
      const maxTrailerSearch = 4096; // search up to 4KB back for a trailer
      let found = false;
      const tailStart = Math.max(0, st.size - HEADER_SIZE - maxTrailerSearch);
      for (
        let candidate = st.size - HEADER_SIZE;
        candidate >= tailStart;
        candidate--
      ) {
        try {
          const trailer = Buffer.alloc(HEADER_SIZE);
          const r = await fh.read(trailer, 0, HEADER_SIZE, candidate);
          if (r.bytesRead !== HEADER_SIZE) continue;
          const len = trailer.readUInt32BE(HEADER_LEN_OFFSET);
          const cks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
          const payloadPos = candidate - len;
          const headerPos = payloadPos - HEADER_SIZE;
          if (headerPos < 0) continue;
          const header = Buffer.alloc(HEADER_SIZE);
          const r2 = await fh.read(header, 0, HEADER_SIZE, headerPos);
          if (r2.bytesRead !== HEADER_SIZE) continue;
          const hlen = header.readUInt32BE(HEADER_LEN_OFFSET);
          const hcks = header.readUInt32BE(HEADER_CKS_OFFSET);
          if (hlen !== len || hcks !== cks) continue;
          const data = Buffer.alloc(len);
          const r3 = await fh.read(data, 0, len, payloadPos);
          if (r3.bytesRead !== len) continue;
          if (adler32(data) !== cks) continue;
          // candidate trailer matched a valid entry
          lastGood = candidate + HEADER_SIZE;
          cursor = candidate - HEADER_SIZE; // move cursor before header of that entry
          found = true;
          break;
        } catch {
          continue;
        }
      }

      if (!found) {
        // fallback: no aligned trailer found within tail window; nothing to do
        // this avoids truncating too aggressively
        return;
      }

      // Now walk backwards validating previous entries until limits
      while (
        cursor >= HEADER_SIZE &&
        entriesChecked < maxEntriesToCheck &&
        bytesChecked < maxBytesToCheck
      ) {
        const trailerPos = cursor;
        const trailer = Buffer.alloc(HEADER_SIZE);
        const r1 = await fh.read(trailer, 0, HEADER_SIZE, trailerPos);
        if (r1.bytesRead !== HEADER_SIZE) break;
        const len = trailer.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = trailer.readUInt32BE(HEADER_CKS_OFFSET);

        const payloadPos = trailerPos - len;
        const headerPos = payloadPos - HEADER_SIZE;
        if (headerPos < 0) break;

        const header = Buffer.alloc(HEADER_SIZE);
        const r2 = await fh.read(header, 0, HEADER_SIZE, headerPos);
        if (r2.bytesRead !== HEADER_SIZE) break;
        const hlen = header.readUInt32BE(HEADER_LEN_OFFSET);
        const hcks = header.readUInt32BE(HEADER_CKS_OFFSET);
        if (hlen !== len || hcks !== cks) break;

        const data = Buffer.alloc(len);
        const r3 = await fh.read(data, 0, len, payloadPos);
        if (r3.bytesRead !== len) break;
        if (adler32(data) !== cks) break;

        lastGood = trailerPos + HEADER_SIZE;
        cursor = headerPos - HEADER_SIZE;
        entriesChecked++;
        bytesChecked += HEADER_SIZE * 2 + len;
      }

      if (lastGood !== st.size) {
        await fh.truncate(lastGood);
      }
    } finally {
      await fh.close();
    }
  }

  /**
   * Append a new entry to the WAL.
   * This will write the entry to the end of the WAL file.
   * @param obj The entry to append.
   */
  async append(obj: any) {
    try {
      // await this.writeLock.acquire();
      // Ensure the WAL is open
      if (this.fd == null) throw new Error("not open");

      // Write the entry to the WAL file
      // Prefer MessagePack for compact binary representation. Fall back to JSON.
      const encoded = (() => {
        try {
          return msgpackEncode(obj);
        } catch (e) {
          return Buffer.from(JSON.stringify(obj));
        }
      })();
      const payloadBuffer = Buffer.from(encoded);
      // Create the header, including the payload length and checksum
      // The header is 8 bytes: 4 bytes for the length and 4 bytes for the checksum
      const headerBuffer = this.headerPool.pop() ?? Buffer.alloc(HEADER_SIZE);

      // Write header fields: version(1), type(1), reserved(2), len(4), cks(4)
      headerBuffer.writeUInt8(this.version, 0);
      headerBuffer.writeUInt8(0, 1); // type 0 = data
      headerBuffer.writeUInt16BE(0, 2); // reserved
      headerBuffer.writeUInt32BE(payloadBuffer.length, HEADER_LEN_OFFSET);
      // Write the checksum to the header, which is the Adler-32 checksum of the payload
      headerBuffer.writeUInt32BE(adler32(payloadBuffer), HEADER_CKS_OFFSET);

      // Create trailer buffer (same as header). We'll write header, payload, trailer
      const trailerBuffer = Buffer.alloc(HEADER_SIZE);
      trailerBuffer.writeUInt8(this.version, 0);
      trailerBuffer.writeUInt8(0, 1);
      trailerBuffer.writeUInt16BE(0, 2);
      trailerBuffer.writeUInt32BE(payloadBuffer.length, HEADER_LEN_OFFSET);
      trailerBuffer.writeUInt32BE(adler32(payloadBuffer), HEADER_CKS_OFFSET);

      // If batching is enabled, queue the buffers and flush later.
      if (this.batching) {
        // Copy headerBuffer when queueing so pooled buffer can be reused safely
        this.batchQueue.push(
          Buffer.from(headerBuffer),
          payloadBuffer,
          Buffer.from(trailerBuffer)
        );
        this.batchCount++;
        // schedule a delayed flush if not set
        this.scheduleFlush();
        if (this.batchCount >= this.maxBatchSize) {
          await this.flushBatch();
        }
      } else {
        // Serialize writes via writeChain to avoid races between concurrent appends
        await this.runWriteOp(async () => {
          // Use writev to write header + payload + trailer in one syscall when possible.
          await (fasync as any).writev(this.fd!, [
            headerBuffer,
            payloadBuffer,
            trailerBuffer,
          ]);
          // fdatasync is typically faster than fsync because it only flushes
          // file data (not metadata).
          await fasync.fdatasync(this.fd!);
          this.metrics.flushCount++;
          // update current offset
          this.currentOffset += HEADER_SIZE * 2 + payloadBuffer.length;
          // update global next and persist meta periodically to reduce fs churn
          this.globalNext = this.globalBase + this.currentOffset;
          this.appendSinceMeta++;
          if (this.appendSinceMeta >= this.metaFlushInterval) {
            this.appendSinceMeta = 0;
            this.writeMetaSync();
          }
        });
      }

      // update metrics (count bytes/entries only after successful write)
      this.metrics.entriesAppended++;
      this.metrics.bytesAppended += HEADER_SIZE * 2 + payloadBuffer.length;

      // Return header buffer to pool (small pool, don't grow forever)
      if (this.headerPool.length < 64) this.headerPool.push(headerBuffer);
    } finally {
      // this.writeLock.release();
    }
  }

  async flushBatch() {
    if (this.batchQueue.length === 0) return;
    // writev supports array of buffers. Use the helper which falls back to concat.
    await this.runWriteOp(async () => {
      await (fasync as any).writev(this.fd!, this.batchQueue as any);
      await fasync.fdatasync(this.fd!);
      // update currentOffset conservatively: add batch size
      this.currentOffset += this.batchQueue.reduce(
        (s, b) => s + Buffer.from(b as any).length,
        0
      );
      this.batchQueue.length = 0;
      this.batchCount = 0;
      this.metrics.flushCount++;
    });
  }

  /**
   * Flush any pending batch synchronously (awaitable).
   */
  async flush() {
    // await this.writeLock.acquire();
    try {
      // cancel timer if set
      if (this.flushTimer != null) {
        clearTimeout(this.flushTimer);
        this.flushTimer = null;
      }
      await this.flushBatch();
    } finally {
      // this.writeLock.release();
    }
  }

  /**
   * Close the WAL: flush pending data and close file descriptor.
   */
  async close() {
    await this.flush();
    this.stopBackgroundFlush();
    if (this.fd != null) {
      await fasync.close(this.fd);
      this.fd = null;
    }
  }

  // Return the current end offset of the WAL (last durable byte position)
  currentEndOffset() {
    return this.globalNext || this.globalBase + this.currentOffset;
  }

  // Truncate WAL file up to offset (exclusive). After truncation, currentOffset is set accordingly.
  async truncateUpTo(offset: number) {
    // Implement segment rotation instead of in-place truncation.
    // We'll rename the current WAL file to a segment name that includes a timestamp and the
    // offset it covered, then create a fresh WAL file for new writes. This is safer across
    // crashes and simpler for distributed settings.
    // await this.writeLock.acquire();
    try {
      if (this.fd == null) throw new Error("not open");
      // flush any pending batch and sync
      await this.flushBatch();
      try {
        await fasync.fdatasync(this.fd);
      } catch {}
      // close current descriptor before renaming
      try {
        await fasync.close(this.fd);
      } catch {}
      this.fd = null;

      const orig = dir(this.rootDir, this.file);
      // segment name includes timestamp and offset for tracing
      const segName = `${this.file}.seg.${Date.now()}.${offset}`;
      const segPath = dir(this.rootDir, segName);
      try {
        fs.renameSync(orig, segPath);
      } catch (e) {
        // If rename fails, try to move with a fallback copy+unlink
        try {
          fs.copyFileSync(orig, segPath);
          fs.unlinkSync(orig);
        } catch (e2) {
          // give up; leave file as-is
        }
      }

      // compute rotated segment's base and end using previous globalBase and provided offset
      try {
        const st = fs.statSync(segPath);
        const segSize = st.size;
        const segBase = this.globalBase;
        const segEnd = offset;
        this.segmentsIndex[segName] = { base: segBase, end: segEnd };
        // persist segments index
        try {
          const segPathMeta = dir(this.rootDir, this.file + ".segments.json");
          fs.writeFileSync(segPathMeta, JSON.stringify(this.segmentsIndex));
        } catch {}
      } catch {}

      // open a fresh WAL file for subsequent appends
      this.fd = await fileDescriptor(dir(this.rootDir, this.file), "a+");
      // set new global base to the provided offset (monotonic global position) and reset offsets
      this.globalBase = offset;
      this.currentOffset = 0;
      this.globalNext = this.globalBase;
      // persist meta for globalBase/globalNext so future restarts know the mapping
      try {
        const metaPath = dir(this.rootDir, this.file + ".meta.json");
        fs.writeFileSync(
          metaPath,
          JSON.stringify({
            globalBase: this.globalBase,
            globalNext: this.globalNext,
          })
        );
      } catch {}
    } finally {
      // this.writeLock.release();
    }
  }

  /**
   * Scan the WAL for entries.
   * This will yield each entry in the order it was written.
   */
  async *scan(minOffset?: number) {
    // Read across current WAL file plus any rotated segments.
    const files = fs.readdirSync(this.rootDir || ".");
    const prefix = this.file + ".seg.";
    // pick segment files (prefix match) and the current file; sort by embedded timestamp so order is chronological
    const walFiles = files
      .filter((f: string) => f === this.file || f.startsWith(prefix))
      .sort((a: string, b: string) => {
        const tsA =
          a === this.file
            ? Number.MAX_SAFE_INTEGER
            : Number(a.split(".")[a.split(".").length - 2]) || 0;
        const tsB =
          b === this.file
            ? Number.MAX_SAFE_INTEGER
            : Number(b.split(".")[b.split(".").length - 2]) || 0;
        return tsA - tsB;
      });

    // Compute per-file global bases. Prefer authoritative mapping from segmentsIndex
    // for rotated segment files and use globalBase for the active WAL file. Fall back
    // to deterministic summing of sizes if no authoritative mapping exists for a file.
    const fileBases: Record<string, number> = {};
    let acc = 0;
    for (const fname of walFiles) {
      try {
        // if this is the active WAL file, base is globalBase
        if (fname === this.file) {
          fileBases[fname] = this.globalBase || acc;
          // current file size contributes to acc for any following files (none expected)
          try {
            const st = fs.statSync(dir(this.rootDir, fname));
            acc = (fileBases[fname] ?? 0) + st.size;
          } catch {}
          continue;
        }
        // rotated segments: prefer persisted base from segmentsIndex
        if (
          this.segmentsIndex &&
          this.segmentsIndex[fname] &&
          typeof this.segmentsIndex[fname].base === "number"
        ) {
          fileBases[fname] = this.segmentsIndex[fname].base;
          // keep acc in sync for files without explicit mapping
          acc = Math.max(
            acc,
            fileBases[fname] +
              (this.segmentsIndex[fname].end - this.segmentsIndex[fname].base ||
                0)
          );
          continue;
        }
        // fallback: use deterministic accumulation
        const st = fs.statSync(dir(this.rootDir, fname));
        fileBases[fname] = acc;
        acc += st.size;
      } catch {
        fileBases[fname] = acc;
      }
    }

    for (const fname of walFiles) {
      const path = dir(this.rootDir, fname);
      const fh = await open(path, "r");
      try {
        const st = await fh.stat();
        if (st.size === 0) continue;
        let cursor = 0;
        const fileBase = fileBases[fname] ?? 0;
        const header = Buffer.alloc(HEADER_SIZE);
        while (cursor + HEADER_SIZE * 2 <= st.size) {
          const r1 = await fh.read(header, 0, HEADER_SIZE, cursor);
          if (r1.bytesRead !== HEADER_SIZE) break;
          const len = header.readUInt32BE(HEADER_LEN_OFFSET);
          const cks = header.readUInt32BE(HEADER_CKS_OFFSET);
          // read payload + trailer in one syscall to reduce syscalls
          const total = len + HEADER_SIZE;
          const buf = this.getScratch(total);
          const r2 = await fh.read(buf, 0, total, cursor + HEADER_SIZE);
          if (r2.bytesRead !== total) break;
          const data = buf.subarray(0, len);
          const trailer = buf.subarray(len, len + HEADER_SIZE);
          const tlen = trailer.readUInt32BE(HEADER_LEN_OFFSET);
          const tcks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
          if (tlen !== len || tcks !== cks) break;
          if (adler32(data) !== cks) break;
          const start = cursor;
          const end = cursor + HEADER_SIZE + len + HEADER_SIZE;
          cursor = end;
          let value: any;
          try {
            value = msgpackDecode(data);
          } catch (e) {
            value = JSON.parse(data.toString("utf8"));
          }
          const globalStart = fileBase + start;
          const globalEnd = fileBase + end;
          if (typeof minOffset === "number" && globalEnd <= minOffset) continue;
          yield value;
        }
      } finally {
        await fh.close();
      }
    }
  }

  /**
   * Scan the WAL and yield each entry with its byte offsets { value, start, end }.
   * start is the header start offset, end is the offset just after the trailer.
   */
  async *scanWithOffsets(minOffset?: number) {
    const files = fs.readdirSync(this.rootDir || ".");
    const prefix = this.file + ".seg.";
    const walFiles = files
      .filter((f: string) => f === this.file || f.startsWith(prefix))
      .sort((a: string, b: string) => {
        const tsA =
          a === this.file
            ? Number.MAX_SAFE_INTEGER
            : Number(a.split(".")[a.split(".").length - 2]) || 0;
        const tsB =
          b === this.file
            ? Number.MAX_SAFE_INTEGER
            : Number(b.split(".")[b.split(".").length - 2]) || 0;
        return tsA - tsB;
      });

    const fileBases: Record<string, number> = {};
    let acc2 = 0;
    for (const fname of walFiles) {
      try {
        if (fname === this.file) {
          fileBases[fname] = this.globalBase || acc2;
          try {
            const st = fs.statSync(dir(this.rootDir, fname));
            acc2 = (fileBases[fname] ?? 0) + st.size;
          } catch {}
          continue;
        }
        if (
          this.segmentsIndex &&
          this.segmentsIndex[fname] &&
          typeof this.segmentsIndex[fname].base === "number"
        ) {
          fileBases[fname] = this.segmentsIndex[fname].base;
          acc2 = Math.max(
            acc2,
            fileBases[fname] +
              (this.segmentsIndex[fname].end - this.segmentsIndex[fname].base ||
                0)
          );
          continue;
        }
        const st = fs.statSync(dir(this.rootDir, fname));
        fileBases[fname] = acc2;
        acc2 += st.size;
      } catch {
        fileBases[fname] = acc2;
      }
    }

    for (const fname of walFiles) {
      const path = dir(this.rootDir, fname);
      const fh = await open(path, "r");
      try {
        const st = await fh.stat();
        if (st.size === 0) continue;
        let cursor = 0;
        const fileBase = fileBases[fname] ?? 0;
        const header = Buffer.alloc(HEADER_SIZE);
        while (cursor + HEADER_SIZE * 2 <= st.size) {
          const r1 = await fh.read(header, 0, HEADER_SIZE, cursor);
          if (r1.bytesRead !== HEADER_SIZE) break;
          const len = header.readUInt32BE(HEADER_LEN_OFFSET);
          const cks = header.readUInt32BE(HEADER_CKS_OFFSET);
          const total = len + HEADER_SIZE;
          const buf = this.getScratch(total);
          const r2 = await fh.read(buf, 0, total, cursor + HEADER_SIZE);
          if (r2.bytesRead !== total) break;
          const data = buf.subarray(0, len);
          const trailer = buf.subarray(len, len + HEADER_SIZE);
          const tlen = trailer.readUInt32BE(HEADER_LEN_OFFSET);
          const tcks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
          if (tlen !== len || tcks !== cks) break;
          if (adler32(data) !== cks) break;
          const start = cursor;
          const end = cursor + HEADER_SIZE + len + HEADER_SIZE;
          cursor = end;
          let value: any;
          try {
            value = msgpackDecode(data);
          } catch (e) {
            value = JSON.parse(data.toString("utf8"));
          }
          const globalStart = fileBase + start;
          const globalEnd = fileBase + end;
          if (typeof minOffset === "number" && globalEnd <= minOffset) continue;
          yield { value, start: globalStart, end: globalEnd };
        }
      } finally {
        await fh.close();
      }
    }
  }

  async *reverseScan() {
    // Reverse scan over rotated segment files and current WAL. Order: newest first.
    const files = fs.readdirSync(this.rootDir || ".");
    const prefix = this.file + ".seg.";
    const walFiles = files
      .filter((f: string) => f === this.file || f.startsWith(prefix))
      .sort((a: string, b: string) => {
        const tsKey = (fn: string) => {
          if (fn === this.file) return Number.MIN_SAFE_INTEGER;
          const parts = fn.split(".");
          if (parts.length >= 3) {
            const tsPart = parts[parts.length - 2];
            const ts = Number(tsPart);
            if (!isNaN(ts)) return ts;
          }
          const offPart = parts[parts.length - 1];
          const off = Number(offPart);
          return isNaN(off) ? 0 : off;
        };
        return tsKey(b) - tsKey(a);
      });

    for (const fname of walFiles) {
      const path = dir(this.rootDir, fname);
      const fh = await open(path, "r");
      try {
        const st = await fh.stat();
        let cursor = st.size;
        const trailer = Buffer.alloc(HEADER_SIZE);
        while (cursor >= HEADER_SIZE) {
          const tpos = cursor - HEADER_SIZE;
          const r1 = await fh.read(trailer, 0, HEADER_SIZE, tpos);
          if (r1.bytesRead !== HEADER_SIZE) break;
          const len = trailer.readUInt32BE(HEADER_LEN_OFFSET);
          const cks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
          const payloadPos = tpos - len;
          if (payloadPos < HEADER_SIZE) break;
          // read header + payload in one syscall
          const total = HEADER_SIZE + len;
          const buf = this.getScratch(total);
          const r2 = await fh.read(buf, 0, total, payloadPos - HEADER_SIZE);
          if (r2.bytesRead !== total) break;
          const header = buf.subarray(0, HEADER_SIZE);
          const data = buf.subarray(HEADER_SIZE, HEADER_SIZE + len);
          const hlen = header.readUInt32BE(HEADER_LEN_OFFSET);
          const hcks = header.readUInt32BE(HEADER_CKS_OFFSET);
          if (hlen !== len || hcks !== cks) break;
          if (adler32(data) !== cks) break;
          cursor = payloadPos - HEADER_SIZE;
          let value: any;
          try {
            value = msgpackDecode(data);
          } catch (e) {
            value = JSON.parse(data.toString("utf8"));
          }
          yield value;
        }
      } finally {
        await fh.close();
      }
    }
  }
}
