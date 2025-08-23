import { Mutex } from "../locks";
import { open } from "node:fs/promises";
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
}

export class Wal extends WallSched {
  private fd: number | null = null;
  private rootDir: string = "./data";
  // Small pool for header buffers to avoid allocating 8 bytes every append.
  private headerPool: Buffer[] = [];
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

  constructor(private file = "log.wal", opts: WalOptions = {}) {
    super();
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
    this.fd = await fileDescriptor(dir(this.rootDir, this.file), "a+");
    // Log whether native writev is available (helps benchmark analysis)
    if (writevNative) console.log("wal: native writev is available");
    else
      console.log("wal: native writev NOT available, falling back to concat");
    // Recover: trim any trailing partial/corrupt entry so future appends succeed
    await this.recoverTail();
    // start a background flush to ensure batches are periodically flushed
    if (this.backgroundFlush) this.startBackgroundFlush();
  }

  private async recoverTail() {
    // Open file for reading
    const fh = await open(dir(this.rootDir, this.file), "r+");
    try {
      const st = await fh.stat();
      let cursor = 0;
      const header = Buffer.alloc(HEADER_SIZE);
      let lastGood = 0;
      while (cursor + HEADER_SIZE * 2 <= st.size) {
        const r1 = await fh.read(header, 0, HEADER_SIZE, cursor);
        if (r1.bytesRead !== HEADER_SIZE) break;
        const len = header.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = header.readUInt32BE(HEADER_CKS_OFFSET);

        // try to read payload
        const data = Buffer.alloc(len);
        const r2 = await fh.read(data, 0, len, cursor + HEADER_SIZE);
        if (r2.bytesRead !== len) break;

        // read trailer
        const trailer = Buffer.alloc(HEADER_SIZE);
        const r3 = await fh.read(
          trailer,
          0,
          HEADER_SIZE,
          cursor + HEADER_SIZE + len
        );
        if (r3.bytesRead !== HEADER_SIZE) break;
        const tlen = trailer.readUInt32BE(HEADER_LEN_OFFSET);
        const tcks = trailer.readUInt32BE(HEADER_CKS_OFFSET);
        if (tlen !== len || tcks !== cks) break;
        if (adler32(data) !== cks) break;

        lastGood = cursor + HEADER_SIZE + len + HEADER_SIZE;
        cursor = lastGood;
      }

      if (lastGood !== st.size) {
        // truncate to lastGood
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
      await this.writeLock.acquire();
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
      }

      this.metrics.entriesAppended++;
      this.metrics.bytesAppended += HEADER_SIZE * 2 + payloadBuffer.length;

      // Return header buffer to pool (small pool, don't grow forever)
      if (this.headerPool.length < 64) this.headerPool.push(headerBuffer);
    } finally {
      this.writeLock.release();
    }
  }

  async flushBatch() {
    if (this.batchQueue.length === 0) return;
    // writev supports array of buffers. Use the helper which falls back to concat.
    await (fasync as any).writev(this.fd!, this.batchQueue as any);
    await fasync.fdatasync(this.fd!);
    this.batchQueue.length = 0;
    this.batchCount = 0;
    this.metrics.flushCount++;
  }

  /**
   * Flush any pending batch synchronously (awaitable).
   */
  async flush() {
    await this.writeLock.acquire();
    try {
      // cancel timer if set
      if (this.flushTimer != null) {
        clearTimeout(this.flushTimer);
        this.flushTimer = null;
      }
      await this.flushBatch();
    } finally {
      this.writeLock.release();
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

  /**
   * Scan the WAL for entries.
   * This will yield each entry in the order it was written.
   */
  async *scan() {
    // Open the WAL file for reading
    const fh = await open(dir(this.rootDir, this.file), "r");
    try {
      // Get the file stats, including the size
      const st = await fh.stat();
      // Check if the file is empty
      if (st.size === 0) return;
      // Read the entries, one by one
      let cursor = 0;
      // Allocate a buffer for the header, including the payload length and checksum
      const header = Buffer.alloc(HEADER_SIZE);
      // Read the entries, one by one
      while (cursor + HEADER_SIZE * 2 <= st.size) {
        // Read the header
        const r1 = await fh.read(header, 0, HEADER_SIZE, cursor);
        if (r1.bytesRead !== HEADER_SIZE) break;
        const len = header.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = header.readUInt32BE(HEADER_CKS_OFFSET);

        // read payload
        const data = Buffer.alloc(len);
        const r2 = await fh.read(data, 0, len, cursor + HEADER_SIZE);
        if (r2.bytesRead !== len) break;

        // read trailer
        const trailer = Buffer.alloc(HEADER_SIZE);
        const r3 = await fh.read(
          trailer,
          0,
          HEADER_SIZE,
          cursor + HEADER_SIZE + len
        );
        if (r3.bytesRead !== HEADER_SIZE) break;
        const tlen = trailer.readUInt32BE(HEADER_LEN_OFFSET);
        const tcks = trailer.readUInt32BE(HEADER_CKS_OFFSET);

        // validate trailer matches header
        if (tlen !== len || tcks !== cks) break;

        // validate checksum
        if (adler32(data) !== cks) break; // tail partial

        // Successfully read an entry
        cursor += HEADER_SIZE + len + HEADER_SIZE;

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

  async *reverseScan() {
    // streaming reverse scan using trailer (located after payload)
    const fh = await open(dir(this.rootDir, this.file), "r");
    try {
      const st = await fh.stat();
      let cursor = st.size;
      const trailer = Buffer.alloc(HEADER_SIZE);

      while (cursor >= HEADER_SIZE) {
        // read trailer
        const tpos = cursor - HEADER_SIZE;
        const r1 = await fh.read(trailer, 0, HEADER_SIZE, tpos);
        if (r1.bytesRead !== HEADER_SIZE) break;
        const len = trailer.readUInt32BE(HEADER_LEN_OFFSET);
        const cks = trailer.readUInt32BE(HEADER_CKS_OFFSET);

        const payloadPos = tpos - len;
        if (payloadPos < HEADER_SIZE) break; // must have header before payload

        const data = Buffer.alloc(len);
        const r2 = await fh.read(data, 0, len, payloadPos);
        if (r2.bytesRead !== len) break;

        // read header and validate
        const header = Buffer.alloc(HEADER_SIZE);
        const r3 = await fh.read(
          header,
          0,
          HEADER_SIZE,
          payloadPos - HEADER_SIZE
        );
        if (r3.bytesRead !== HEADER_SIZE) break;
        const hlen = header.readUInt32BE(HEADER_LEN_OFFSET);
        const hcks = header.readUInt32BE(HEADER_CKS_OFFSET);
        if (hlen !== len || hcks !== cks) break;

        if (adler32(data) !== cks) break;

        // move cursor to start of header for next iteration
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
