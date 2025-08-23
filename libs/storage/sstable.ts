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

import { deflateSync, inflateSync } from 'node:zlib';

export function compressBlock(buf: Buffer, algo = COMPRESSION_DEFLATE): Buffer {
  if (algo === COMPRESSION_DEFLATE) return deflateSync(buf);
  return buf;
}

export function decompressBlock(buf: Buffer, algo = COMPRESSION_DEFLATE): Buffer {
  if (algo === COMPRESSION_DEFLATE) return inflateSync(buf);
  return buf;
}
