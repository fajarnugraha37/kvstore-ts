import {
  deflateSync,
  inflateSync,
  gzipSync,
  gunzipSync,
  deflate,
  inflate,
  gzip,
  gunzip,
  createDeflate,
  createInflate,
  createGzip,
  createGunzip,
} from "node:zlib";
import { promisify } from "node:util";
import { Transform } from "node:stream";
import _snappy from "snappyjs";

// Clean block-based SST implementation
// Layout:
// [header 'SST1'] [blocks...] [index] [bloom?] [footer]

export type FileMeta = {
  file: string;
  minKey: Buffer;
  maxKey: Buffer;
  size: number;
};

// Named constants for header/footer sizes and magic values so index math is clear
export const SST_HEADER_MAGIC = "SST1";
// file format versioning: header = magic(4) + version(1)
export const SST_HEADER_LEN = SST_HEADER_MAGIC.length + 1; // 5
export const SST_FOOTER_LEN = 28; // indexOffset(8) + bloomOffset(8) + indexCks(4) + footerCks(4) + magic(4)

export const sstableDebug = {
  enabled: false as boolean,
  blockReads: 0 as number,
  reset() {
    this.blockReads = 0;
  },
};

// Per-block compression support
export const COMPRESSION_NONE = 0;
export const COMPRESSION_DEFLATE = 1;
export const COMPRESSION_GZIP = 2;
export const COMPRESSION_SNAPPY = 3;
// If the high bit of the stored block length is set, the block is compressed.
export const BLOCK_COMPRESSED_FLAG = 0x80000000;

// Default threshold (in bytes) above which a block will be considered for compression.
// This can be tuned by passing `compressionThreshold` in `SSTWriterOptions`.
export const DEFAULT_COMPRESSION_THRESHOLD = 128;

/* Documentation:
 Per-block compression is optional and controlled by two knobs:
  - compressionAlgo: one of COMPRESSION_NONE (0) or COMPRESSION_DEFLATE (1)
  - compressionThreshold: minimum raw block size in bytes to attempt compression

 When a block is compressed, the stored block length written into the index has
 the high bit set (BLOCK_COMPRESSED_FLAG). The CRC in the index covers the
 on-disk bytes (i.e. compressed bytes when compression is used). Readers will
 detect the flag, read the stored bytes, verify CRC, and then decompress before
 parsing the block payload.
*/
const deflateAsync = promisify(deflate);
const inflateAsync = promisify(inflate);
const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

export function compressBlock(buf: Buffer, algo = COMPRESSION_DEFLATE): Buffer {
  if (algo === COMPRESSION_DEFLATE) return deflateSync(buf);
  if (algo === COMPRESSION_GZIP) return gzipSync(buf);
  if (algo === COMPRESSION_SNAPPY) {
    // snappyjs.compress returns Uint8Array
    const out = _snappy.compress(buf);
    return Buffer.from(out);
  }
  return buf;
}

export function decompressBlock(
  buf: Buffer,
  algo = COMPRESSION_DEFLATE
): Buffer {
  if (algo === COMPRESSION_DEFLATE) return inflateSync(buf);
  if (algo === COMPRESSION_GZIP) return gunzipSync(buf);
  if (algo === COMPRESSION_SNAPPY) {
    const out = _snappy.uncompress(buf);
    return Buffer.from(out);
  }
  return buf;
}

export async function compressBlockAsync(
  buf: Buffer,
  algo = COMPRESSION_DEFLATE
): Promise<Buffer> {
  if (algo === COMPRESSION_DEFLATE) return Buffer.from(await deflateAsync(buf));
  if (algo === COMPRESSION_GZIP) return Buffer.from(await gzipAsync(buf));
  if (algo === COMPRESSION_SNAPPY) {
    return Buffer.from(_snappy.compress(buf));
  }
  return buf;
}

export async function decompressBlockAsync(
  buf: Buffer,
  algo = COMPRESSION_DEFLATE
): Promise<Buffer> {
  if (algo === COMPRESSION_DEFLATE) return Buffer.from(await inflateAsync(buf));
  if (algo === COMPRESSION_GZIP) return Buffer.from(await gunzipAsync(buf));
  if (algo === COMPRESSION_SNAPPY) {
    return Buffer.from(_snappy.uncompress(buf));
  }
  return buf;
}

// Stream helpers: return Transform streams that compress/decompress using zlib.
export function compressStream(algo = COMPRESSION_DEFLATE): Transform {
  if (algo === COMPRESSION_DEFLATE) return createDeflate();
  if (algo === COMPRESSION_GZIP) return createGzip();
  // snappy streaming isn't standard across packages; if not available, fall back to passthrough with buffering
  if (algo === COMPRESSION_SNAPPY) {
    // implement framed streaming for snappy so we don't buffer entire stream.
    // Frame format: [uint32BE compressedLen][uint32BE uncompressedLen][compressed bytes]
    const FRAME_SIZE = 64 * 1024; // 64KB frames

    // helper to compress a single chunk with available snappy impls
    const compressSnappyChunk = async (buf: Buffer): Promise<Buffer> => {
      // snappyjs is synchronous
      return Buffer.from(_snappy.compress(buf));
    };

    let pending = Buffer.alloc(0);
    let processing = false;

    const tryProcess = async (pushFn: (b: Buffer) => void) => {
      if (processing) return;
      processing = true;
      try {
        while (pending.length >= FRAME_SIZE) {
          const chunk = pending.slice(0, FRAME_SIZE);
          pending = pending.slice(FRAME_SIZE);
          const comp = await compressSnappyChunk(chunk);
          const hdr = Buffer.alloc(8);
          hdr.writeUInt32BE(comp.length, 0);
          hdr.writeUInt32BE(chunk.length, 4);
          pushFn(Buffer.concat([hdr, comp]));
        }
      } finally {
        processing = false;
      }
    };

    return new Transform({
      transform(chunk, _enc, cb) {
        try {
          pending = Buffer.concat([pending, Buffer.from(chunk)]);
          // process available full frames and wait for them to be emitted before
          // calling the transform callback so the pipeline doesn't end before
          // frames are pushed (fixes single-frame cases).
          tryProcess((b) => this.push(b)).then(
            () => cb(),
            (err) => cb(err as any)
          );
        } catch (err) {
          cb(err as any);
        }
      },
      async flush(cb) {
        try {
          // compress any remaining bytes as a final frame
          if (pending.length > 0) {
            const chunk = pending;
            pending = Buffer.alloc(0);
            const comp = await compressSnappyChunk(chunk);
            const hdr = Buffer.alloc(8);
            hdr.writeUInt32BE(comp.length, 0);
            hdr.writeUInt32BE(chunk.length, 4);
            this.push(Buffer.concat([hdr, comp]));
          }
          cb();
        } catch (err) {
          cb(err as any);
        }
      },
    });
  }
  // default passthrough
  return new Transform({
    transform(chunk, _enc, cb) {
      cb(null, chunk);
    },
  });
}

export function decompressStream(algo = COMPRESSION_DEFLATE): Transform {
  if (algo === COMPRESSION_DEFLATE) return createInflate();
  if (algo === COMPRESSION_GZIP) return createGunzip();
  if (algo === COMPRESSION_SNAPPY) {
    // Implement framed streaming decoder matching compressStream frame format
    // Frame format: [uint32BE compressedLen][uint32BE uncompressedLen][compressed bytes]
    const decompressSnappyChunk = async (buf: Buffer): Promise<Buffer> => {
      return Buffer.from(_snappy.uncompress(buf));
    };

    let pending = Buffer.alloc(0);

    return new Transform({
      transform(chunk, _enc, cb) {
        pending = Buffer.concat([pending, Buffer.from(chunk)]);
        const processFrames = async () => {
          try {
            while (pending.length >= 8) {
              const compLen = pending.readUInt32BE(0);
              const uncompLen = pending.readUInt32BE(4);
              if (pending.length < 8 + compLen) break; // wait for full frame
              const compBuf = pending.slice(8, 8 + compLen);
              pending = pending.slice(8 + compLen);
              const out = await decompressSnappyChunk(compBuf);
              // optional sanity check
              if (out.length !== uncompLen) {
                // not fatal — push what we got
              }
              this.push(out);
            }
          } catch (err) {
            this.emit("error", err as any);
          }
        };
        processFrames().then(
          () => cb(),
          (err) => cb(err as any)
        );
      },
      flush(cb) {
        // If any trailing bytes remain, that's an error: incomplete frame.
        if (pending.length > 0) {
          cb(new Error("incomplete snappy frame at end of stream"));
          return;
        }
        cb();
      },
    });
  }
  return new Transform({
    transform(chunk, _enc, cb) {
      cb(null, chunk);
    },
  });
}
