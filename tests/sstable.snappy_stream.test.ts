import { describe, it, expect } from "bun:test";
import * as sstable from "../libs/storage/sstable";
import { pipeline } from "node:stream/promises";
import { Readable, Writable } from "node:stream";

// Helper to detect whether snappy is available at runtime for our sstable helpers.
function snappyAvailable(): boolean {
  try {
    // try a small round-trip using compressBlock; if it throws, snappy isn't present
    const ok = sstable.compressBlock(
      Buffer.from("hello"),
      sstable.COMPRESSION_SNAPPY
    );
    return Buffer.isBuffer(ok);
  } catch (e) {
    return false;
  }
}

describe("sstable snappy streaming", () => {
  it("stream round-trip small payload", async () => {
    if (!snappyAvailable()) return;

    const srcBuf = Buffer.alloc(64 * 1024, "A"); // 64KB
    const src = Readable.from([srcBuf]);
    const comp = sstable.compressStream(sstable.COMPRESSION_SNAPPY);
    const decomp = sstable.decompressStream(sstable.COMPRESSION_SNAPPY);

    let out = Buffer.alloc(0);
    const sink = new Writable({
      write(chunk, _enc, cb) {
        out = Buffer.concat([out, Buffer.from(chunk)]);
        cb();
      },
    });

    await pipeline(src, comp, decomp, sink);
    expect(out.length).toBe(srcBuf.length);
    expect(out.equals(srcBuf)).toBe(true);
  });

  it("stream round-trip large payload (multi-MB) without full buffering", async () => {
    if (!snappyAvailable()) return;

    // generate ~8MB of semi-compressible data in many frames
    const FRAME = 64 * 1024;
    const CHUNKS = Math.floor((8 * 1024 * 1024) / FRAME); // ~8MB

    async function* gen() {
      for (let i = 0; i < CHUNKS; i++) {
        // vary pattern a bit so compressor sees repetition but not identical all-through
        const b = Buffer.alloc(FRAME, i % 256);
        yield b;
      }
    }

    const src = Readable.from(gen());
    const comp = sstable.compressStream(sstable.COMPRESSION_SNAPPY);
    const decomp = sstable.decompressStream(sstable.COMPRESSION_SNAPPY);

    // stream to a sink that counts bytes without retaining everything in memory
    let total = 0;
    const sink = new Writable({
      write(chunk, _enc, cb) {
        total += Buffer.byteLength(chunk);
        cb();
      },
    });

    await pipeline(src, comp, decomp, sink);

    const expected = CHUNKS * FRAME;
    // Validate total size matches expected multi-MB payload
    expect(total).toBe(expected);
  }, 20000);
});
