import fs from "node:fs";
import { fileDescriptor, dir, ensureDir } from "../utils/file";
import { fasync } from "../utils/fs";
import {
  encode as msgpackEncode,
  decode as msgpackDecode,
} from "@msgpack/msgpack";
import { adler32 } from "../utils";
import type { WalLike } from "./types";
import { open } from "node:fs/promises";

const HEADER_SIZE = 12;
const HEADER_LEN_OFFSET = 4;
const HEADER_CKS_OFFSET = 8;

export type HandoffWalOptions = {
  batching?: boolean;
  backgroundFlush?: boolean;
  rootDir?: string;
  maxBatchSize?: number;
  version?: number;
  maxQueue?: number; // max queued entries
  maxInFlightBytes?: number; // max queued bytes before backpressure
  batchMaxEntries?: number; // max entries per writev
  batchMaxBytes?: number; // max bytes per writev
  batchIntervalMs?: number; // flush interval
};

/**
 * HandoffWal is an asynchronous, handoff-style WAL optimized for high-throughput
 * append workloads.
 *
 * Behavior and characteristics:
 * - Producers enqueue encoded entries into a bounded in-memory queue. A dedicated
 *   writer loop drains the queue, batches entries and performs writev + fdatasync
 *   in the background.
 * - Provides backpressure via queue size and in-flight bytes limits so producers
 *   can be back-pressured when the writer lags.
 * - Uses a reusable scratch buffer for reads similar to `Wal`.
 * - Append returns once the entry is queued (or after backpressure wait), not after
 *   durability; callers must call `flush()` if they require durability before
 *   proceeding.
 *
 * Tradeoffs:
 * - Higher append throughput and batching efficiency at the cost of weaker
 *   synchronous durability semantics (append is not durable until drained and
 *   fdatasync completes).
 * - Slightly more complex internal coordination (writer loop, backpressure waiters).
 *
 * Use-case: prefer `HandoffWal` for high-throughput workloads where batching and
 * background durability are acceptable and lower append latency is desired.
 *
 * Note on composition vs inheritance:
 * - `HandoffWal` intentionally does not extend a scheduler-type base (e.g. `WallSched`)
 *   because its internal producer/consumer writer loop, enqueue/backpressure
 *   semantics and batching behavior differ from a simple scheduled writer.
 * - If you want to share scheduling or rotation logic between multiple WAL
 *   implementations, prefer extracting that logic into a small, testable helper
 *   (for example a `WallSched` utility) and composing it into `HandoffWal` rather
 *   than using classical inheritance. This keeps the handoff queue semantics
 *   explicit and avoids coupling writer-loop internals.
 */
export class HandoffWal implements WalLike {
  private fd: number | null = null;
  private rootDir = "./data";
  private file: string;
  private running = false;
  private version = 1;
  private batching = true;
  private backgroundFlush = true;
  // bounded queue
  private queue: Buffer[] = [];
  private queueBytes = 0;
  private maxQueue = 65536;
  private maxInFlightBytes = 4 * 1024 * 1024; // 4MB default
  private batchMaxEntries = 256;
  private batchMaxBytes = 256 * 1024; // 256KB
  private batchIntervalMs = 10;

  // backpressure waiters
  private enqueueWaiters: Array<() => void> = [];
  // notify writer when data arrives
  private waiter: (() => void) | null = null;

  // processing flag and drain waiters to signal when writer finished last batch
  private processing = false;
  private drainWaiters: Array<() => void> = [];

  // writer loop completion promise
  private writerDoneResolve: (() => void) | null = null;
  private writerDone: Promise<void> | null = null;

  // WAL metadata similar to Wal
  private currentOffset: number = 0;
  private globalBase: number = 0;
  private globalNext: number = 0;
  private segmentsIndex: Record<string, { base: number; end: number }> = {};
  private metaFlushInterval = 64;
  private appendSinceMeta = 0;

  // scratch buffer reused for reads
  private _scratch: Buffer | null = null;
  private _scratchSize = 0;

  private getScratch(minSize: number) {
    if (!this._scratch || this._scratchSize < minSize) {
      const newSize = Math.max(minSize, this._scratchSize * 2 || 1024);
      this._scratch = Buffer.alloc(newSize);
      this._scratchSize = newSize;
    }
    return this._scratch;
  }

  // metrics split between enqueue and durable
  public metrics = {
    entriesQueued: 0,
    bytesQueued: 0,
    entriesDurable: 0,
    bytesDurable: 0,
    flushCount: 0,
    // expose legacy fields expected by stress harness
    entriesAppended: 0,
    bytesAppended: 0,
  };

  constructor(file = "log.wal", opts: HandoffWalOptions = {}) {
    this.file = file;
    if (opts.rootDir) this.rootDir = opts.rootDir;
    if (opts.maxQueue != null) this.maxQueue = opts.maxQueue;
    if (opts.maxInFlightBytes != null)
      this.maxInFlightBytes = opts.maxInFlightBytes;
    if (opts.batchMaxEntries != null)
      this.batchMaxEntries = opts.batchMaxEntries;
    if (opts.batchMaxBytes != null) this.batchMaxBytes = opts.batchMaxBytes;
    if (opts.batchIntervalMs != null)
      this.batchIntervalMs = opts.batchIntervalMs;
    if (opts.batchMaxEntries == null && opts.maxBatchSize != null) {
      // allow older name mapping
      this.batchMaxEntries = opts.maxBatchSize;
    }
    if (opts.maxBatchSize != null) this.batchMaxEntries = opts.maxBatchSize;
    if (opts.version != null) this.version = opts.version;
    if (opts.batching != null) this.batching = opts.batching;
    if (opts.backgroundFlush != null) {
      // If backgroundFlush is false, writer loop will be started lazily on first append
      this.backgroundFlush = opts.backgroundFlush;
    }
  }

  async open() {
    await ensureDir(this.rootDir);
    const path = dir(this.rootDir, this.file);
    try {
      fs.writeFileSync(path, "", { flag: "a" });
    } catch {}
    this.fd = await fileDescriptor(path, "a+");
    // recover tail and refresh current offset
    await this.recoverTail();
    try {
      const st = fs.statSync(dir(this.rootDir, this.file));
      this.currentOffset = st.size;
    } catch {}
    // try to load segments/meta if present
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
      if (fs.existsSync(metaPath)) {
        const buf = fs.readFileSync(metaPath, "utf8");
        const json = JSON.parse(buf);
        if (typeof json.globalBase === "number")
          this.globalBase = json.globalBase;
        if (typeof json.globalNext === "number")
          this.globalNext = json.globalNext;
      } else {
        // derive globalBase from segments if available
        let maxEnd = 0;
        for (const k of Object.keys(this.segmentsIndex)) {
          const e = this.segmentsIndex[k];
          if (e && typeof e.end === "number" && e.end > maxEnd) maxEnd = e.end;
        }
        if (maxEnd > 0) {
          this.globalBase = maxEnd;
        } else {
          // sum rotated segments as fallback
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

    this.running = true;
    this.writerDone = new Promise((res) => (this.writerDoneResolve = res));
    this.startWriterLoop();
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
    const fh = await open(dir(this.rootDir, this.file), "r+");
    try {
      const st = await fh.stat();
      if (st.size === 0) return;
      let cursor = st.size;
      let lastGood = cursor;

      const maxEntriesToCheck = 4096;
      const maxBytesToCheck = 1024 * 1024;
      let entriesChecked = 0;
      let bytesChecked = 0;

      // search backwards a small window for an aligned valid trailer
      const maxTrailerSearch = 4096;
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
          lastGood = candidate + HEADER_SIZE;
          cursor = candidate - HEADER_SIZE;
          found = true;
          break;
        } catch {
          continue;
        }
      }

      if (!found) return;

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

  // simple scans: reproduce the basic scan/scanWithOffsets/reverseScan from Wal
  async *scan(minOffset?: number) {
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
    let acc = 0;
    for (const fname of walFiles) {
      try {
        if (fname === this.file) {
          fileBases[fname] = acc;
          try {
            const st = fs.statSync(dir(this.rootDir, fname));
            acc = (fileBases[fname] ?? 0) + st.size;
          } catch {}
          continue;
        }
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
            try {
              value = JSON.parse(data.toString("utf8"));
            } catch (ee) {
              value = data.toString("utf8");
            }
          }
          const globalEnd = fileBase + end;
          if (typeof minOffset === "number" && globalEnd <= minOffset) continue;
          yield value;
        }
      } finally {
        await fh.close();
      }
    }
  }

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
    let acc = 0;
    for (const fname of walFiles) {
      try {
        if (fname === this.file) {
          fileBases[fname] = acc;
          try {
            const st = fs.statSync(dir(this.rootDir, fname));
            acc = (fileBases[fname] ?? 0) + st.size;
          } catch {}
          continue;
        }
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
            try {
              value = JSON.parse(data.toString("utf8"));
            } catch (ee) {
              value = data.toString("utf8");
            }
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

  /**
   * Buffered scan: load each WAL file into memory and parse entries from a large buffer.
   */
  async *scanBuffered(minOffset?: number) {
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
              (this.segmentsIndex[fname].end - this.segmentsIndex[fname].base || 0)
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
      let buf: Buffer;
      try {
        buf = fs.readFileSync(path);
      } catch {
        continue;
      }
      if (buf.length === 0) continue;
      const fileBase = fileBases[fname] ?? 0;
      let cursor = 0;
      while (cursor + HEADER_SIZE * 2 <= buf.length) {
        const header = buf.subarray(cursor, cursor + HEADER_SIZE);
        const len = header.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = header.readUInt32BE(HEADER_CKS_OFFSET);
        const total = len + HEADER_SIZE;
        if (cursor + HEADER_SIZE + total > buf.length) break;
        const data = buf.subarray(cursor + HEADER_SIZE, cursor + HEADER_SIZE + len);
        const trailer = buf.subarray(cursor + HEADER_SIZE + len, cursor + HEADER_SIZE + len + HEADER_SIZE);
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
          try {
            value = JSON.parse(data.toString("utf8"));
          } catch (ee) {
            value = data.toString("utf8");
          }
        }
        const globalEnd = fileBase + end;
        if (typeof minOffset === "number" && globalEnd <= minOffset) continue;
        yield value;
      }
    }
  }

  async *reverseScan() {
    const files = fs.readdirSync(this.rootDir || ".");
    const prefix = this.file + ".seg.";
    const walFiles = files
      .filter((f: string) => f === this.file || f.startsWith(prefix))
      .sort((a: string, b: string) => {
        const tsKey = (fn: string) => {
          if (fn === this.file) return Number.MAX_SAFE_INTEGER;
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
            try {
              value = JSON.parse(data.toString("utf8"));
            } catch (ee) {
              value = data.toString("utf8");
            }
          }
          yield value;
        }
      } finally {
        await fh.close();
      }
    }
  }

  /**
   * Buffered reverse scan: load files into memory and parse entries from the buffer backwards.
   */
  async *reverseScanBuffered() {
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
      let buf: Buffer;
      try {
        buf = fs.readFileSync(path);
      } catch {
        continue;
      }
      const stSize = buf.length;
      let cursor = stSize;
      while (cursor >= HEADER_SIZE) {
        const tpos = cursor - HEADER_SIZE;
        if (tpos < 0) break;
        const trailer = buf.subarray(tpos, tpos + HEADER_SIZE);
        const len = trailer.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
        const payloadPos = tpos - len;
        if (payloadPos < HEADER_SIZE) break;
        const headerPos = payloadPos - HEADER_SIZE;
        if (headerPos < 0) break;
        const header = buf.subarray(headerPos, headerPos + HEADER_SIZE);
        const hlen = header.readUInt32BE(HEADER_LEN_OFFSET);
        const hcks = header.readUInt32BE(HEADER_CKS_OFFSET);
        if (hlen !== len || hcks !== cks) break;
        const data = buf.subarray(payloadPos, payloadPos + len);
        if (adler32(data) !== cks) break;
        cursor = headerPos;
        let value: any;
        try {
          value = msgpackDecode(data);
        } catch (e) {
          try {
            value = JSON.parse(data.toString("utf8"));
          } catch (ee) {
            value = data.toString("utf8");
          }
        }
        yield value;
      }
    }
  }

  // Wait until there's room in the queue (backpressure)
  private async waitForQueueSpace() {
    if (
      this.queue.length < this.maxQueue &&
      this.queueBytes < this.maxInFlightBytes
    )
      return;
    await new Promise<void>((res) => this.enqueueWaiters.push(res));
  }

  // Notify producers waiting for queue space
  private notifyEnqueue() {
    while (
      this.enqueueWaiters.length > 0 &&
      this.queue.length < this.maxQueue &&
      this.queueBytes < this.maxInFlightBytes
    ) {
      const w = this.enqueueWaiters.shift()!;
      w();
    }
  }

  private notifyWriter() {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w();
    }
  }

  private async startWriterLoop() {
    const fd = this.fd!;
    try {
      while (this.running || this.queue.length > 0) {
        if (this.queue.length === 0) {
          // wait until data or timeout
          await new Promise<void>((res) => {
            this.waiter = res;
            setTimeout(res, this.batchIntervalMs);
          });
        }

        if (this.queue.length === 0) continue;

        // build batch constrained by entries and bytes
        const batch: Buffer[] = [];
        let bytes = 0;
        // mark that we're processing a batch
        this.processing = true;
        while (
          this.queue.length > 0 &&
          batch.length < this.batchMaxEntries &&
          bytes < this.batchMaxBytes
        ) {
          const b = this.queue.shift()!;
          this.queueBytes -= b.length;
          batch.push(b);
          bytes += b.length;
        }

        if (batch.length === 0) continue;

        try {
          await (fasync as any).writev(fd, batch as any);
          await fasync.fdatasync(fd);
          this.metrics.flushCount++;
          this.metrics.entriesDurable += batch.length;
          this.metrics.bytesDurable += bytes;
          // also update legacy aliases so external tools/readers see durable counts
          this.metrics.entriesAppended = this.metrics.entriesDurable;
          this.metrics.bytesAppended = this.metrics.bytesDurable;
          // update offsets similar to Wal
          this.currentOffset += bytes;
          this.globalNext = this.globalBase + this.currentOffset;
          this.appendSinceMeta++;
          if (this.appendSinceMeta >= this.metaFlushInterval) {
            this.appendSinceMeta = 0;
            this.writeMetaSync();
          }
        } catch (e) {
          console.error("handoff-wal write error", e);
          // On error, attempt to re-enqueue remaining data (best-effort) and break when fatal
        }

        // notify producers that space freed
        this.notifyEnqueue();
        // mark processing done and notify any flush waiters
        this.processing = false;
        this.notifyDrainers();
      }
    } finally {
      // writer loop ending: ensure any drain waiters are notified
      try {
        this.processing = false;
        this.notifyDrainers();
        if (this.writerDoneResolve) this.writerDoneResolve();
      } catch {}
    }
  }

  // Notify flush callers when writer is idle (no queued items and not processing)
  private notifyDrainers() {
    while (
      this.drainWaiters.length > 0 &&
      this.queue.length === 0 &&
      !this.processing
    ) {
      const w = this.drainWaiters.shift()!;
      w();
    }
  }

  /**
   * Append an entry; will apply backpressure when queue is full.
   */
  async append(obj: any) {
    const encoded = (() => {
      try {
        return msgpackEncode(obj);
      } catch (e) {
        return Buffer.from(JSON.stringify(obj));
      }
    })();
    const payload = Buffer.from(encoded as Uint8Array);
    const len = payload.length;
    const cks = adler32(payload);
    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt8(1, 0);
    header.writeUInt8(0, 1);
    header.writeUInt16BE(0, 2);
    header.writeUInt32BE(len, HEADER_LEN_OFFSET);
    header.writeUInt32BE(cks, HEADER_CKS_OFFSET);
    const trailer = Buffer.alloc(HEADER_SIZE);
    trailer.writeUInt8(1, 0);
    trailer.writeUInt8(0, 1);
    trailer.writeUInt16BE(0, 2);
    trailer.writeUInt32BE(len, HEADER_LEN_OFFSET);
    trailer.writeUInt32BE(cks, HEADER_CKS_OFFSET);
    const entryBuf = Buffer.concat([header, payload, trailer]);

    // backpressure
    await this.waitForQueueSpace();
    this.queue.push(entryBuf);
    this.queueBytes += entryBuf.length;
    this.metrics.entriesQueued++;
    this.metrics.bytesQueued += entryBuf.length;
    // notify writer
    this.notifyWriter();
  }

  async flush() {
    // wait until queue drained and any in-flight batch completes
    if (this.queue.length > 0 || this.processing) {
      await new Promise<void>((res) => this.drainWaiters.push(res));
    }
  }

  async close() {
    this.running = false;
    this.notifyWriter();
    // wait for writer to finish
    if (this.writerDone) await this.writerDone;
    if (this.fd != null) {
      try {
        await fasync.close(this.fd);
      } catch {}
      this.fd = null;
    }
  }

  async truncateUpTo(offset: number) {
    if (this.fd == null) throw new Error("not open");
    // flush any pending queue
    await this.flush();
    try {
      await fasync.fdatasync(this.fd);
    } catch {}
    try {
      await fasync.close(this.fd);
    } catch {}
    this.fd = null;

    const orig = dir(this.rootDir, this.file);
    const segName = `${this.file}.seg.${Date.now()}.${offset}`;
    const segPath = dir(this.rootDir, segName);
    try {
      fs.renameSync(orig, segPath);
    } catch (e) {
      try {
        fs.copyFileSync(orig, segPath);
        fs.unlinkSync(orig);
      } catch (e2) {
        // ignore
      }
    }

    try {
      const st = fs.statSync(segPath);
      const segSize = st.size;
      const segBase = this.globalBase;
      const segEnd = offset;
      this.segmentsIndex[segName] = { base: segBase, end: segEnd };
      try {
        const segPathMeta = dir(this.rootDir, this.file + ".segments.json");
        fs.writeFileSync(segPathMeta, JSON.stringify(this.segmentsIndex));
      } catch {}
    } catch {}

    this.fd = await fileDescriptor(dir(this.rootDir, this.file), "a+");
    this.globalBase = offset;
    this.currentOffset = 0;
    this.globalNext = this.globalBase;
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

  // Return the current end offset of the WAL (last durable byte position)
  currentEndOffset() {
    return this.globalNext || this.globalBase + this.currentOffset;
  }
}
