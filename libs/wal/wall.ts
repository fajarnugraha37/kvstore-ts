import { Mutex } from "../locks";
import { open } from "node:fs/promises";
import { serialize, deserialize } from "bun:jsc";
import { adler32, dir, ensureDir, fasync, fileDescriptor, writevNative } from "../utils";

const HEADER_SIZE = 8;
const HEADER_PAYLOAD_SIZE = 4;
const HEADER_CHECKSUM_SIZE = 4;

export class Wal {
  private fd: number | null = null;
  private rootDir: string = "./data";
  private writeLock = new Mutex();
  // Small pool for header buffers to avoid allocating 8 bytes every append.
  private headerPool: Buffer[] = [];
  // Batch queue for appends. We'll flush on demand or when batch size exceeded.
  private batchQueue: Array<Buffer> = [];
  private batchCount = 0;
  private maxBatchSize = 16; // number of entries to batch
  private batching = false;

  constructor(private file = "log.wal") {}

  /**
   * Open the WAL file for reading and writing.
   * This will create the file if it does not exist.
   */
  async open() {
    await ensureDir(this.rootDir);
    this.fd = await fileDescriptor(dir(this.rootDir, this.file), "a+");
  // Log whether native writev is available (helps benchmark analysis)
  if (writevNative) console.log("wal: native writev is available");
  else console.log("wal: native writev NOT available, falling back to concat");
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
      const payloadBuffer = Buffer.from(serialize(obj));
      // Create the header, including the payload length and checksum
      // The header is 8 bytes: 4 bytes for the length and 4 bytes for the checksum
  const headerBuffer = this.headerPool.pop() ?? Buffer.alloc(HEADER_SIZE);

      // Write the payload length and checksum to the header
      headerBuffer.writeUInt32BE(payloadBuffer.length, 0);
      // Write the checksum to the header, which is the Adler-32 checksum of the payload
      headerBuffer.writeUInt32BE(adler32(payloadBuffer), HEADER_PAYLOAD_SIZE);

      // If batching is enabled, queue the buffers and flush later.
      if (this.batching) {
        this.batchQueue.push(headerBuffer, payloadBuffer);
        this.batchCount++;
        if (this.batchCount >= this.maxBatchSize) {
          await this.flushBatch();
        }
      } else {
        // Use writev to write header + payload in one syscall when possible.
  await (fasync as any).writev(this.fd!, [headerBuffer, payloadBuffer]);
        // fdatasync is typically faster than fsync because it only flushes
        // file data (not metadata).
  await fasync.fdatasync(this.fd!);
      }

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
      while (cursor + HEADER_SIZE <= st.size) {
        // Read the header
        const r1 = await fh.read(header, 0, HEADER_SIZE, cursor);
        // Check if the header was read successfully
        if (r1.bytesRead !== HEADER_SIZE) break;
        // Read the payload length and checksum from the header
        const len = header.readUInt32BE(0);
        // Read the checksum from the header
        const cks = header.readUInt32BE(HEADER_PAYLOAD_SIZE);
        // Check if the payload length is valid
        const data = Buffer.alloc(len);
        // Read the payload
        const r2 = await fh.read(data, 0, len, cursor + HEADER_SIZE);
        // Check if the payload was read successfully
        if (r2.bytesRead !== len) break;
        // Check if the checksum is valid
        if (adler32(data) !== cks) break; // tail partial
        // Successfully read an entry
        cursor += HEADER_SIZE + len;

        // Yield the parsed entry
        yield deserialize(data);
      }
    } finally {
      await fh.close();
    }
  }

  async *reverseScan() {
    const fh = await open(dir(this.rootDir, this.file), "r");
    try {
      const st = await fh.stat();
      if (st.size === 0) return;
      let cursor = st.size;
      const header = Buffer.alloc(HEADER_SIZE);

      while (cursor >= HEADER_SIZE) {
        const hdrPos = cursor - HEADER_SIZE;
        const r1 = await fh.read(header, 0, HEADER_SIZE, hdrPos);
        if (r1.bytesRead !== HEADER_SIZE) break;

        const len = header.readUInt32BE(0);
        const cks = header.readUInt32BE(HEADER_PAYLOAD_SIZE);

        const dataPos = hdrPos - len;
        if (dataPos < 0) break; // corrupted / partial

        const data = Buffer.alloc(len);
        const r2 = await fh.read(data, 0, len, dataPos);
        if (r2.bytesRead !== len) break;

        if (adler32(data) !== cks) break; // tail partial or corrupted

        cursor = dataPos;

        yield deserialize(data);
      }
    } finally {
      await fh.close();
    }
  }
}
