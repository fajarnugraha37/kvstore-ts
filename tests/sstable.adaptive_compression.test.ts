import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import * as sstable from "../libs/storage/sstable";
import {
  unlinkSync,
  existsSync,
  openSync,
  fstatSync,
  readSync,
  closeSync,
} from "node:fs";
import { randomBytes } from "node:crypto";

describe("sstable adaptive compression", () => {
  it("compresses compressible blocks and skips incompressible ones when adaptiveCompression enabled", async () => {
    // compressible SST
    const tmp1 = "./data/adaptive_comp_good.tmp";
    const final1 = "./data/adaptive_comp_good.sst";
    try {
      if (existsSync(tmp1)) unlinkSync(tmp1);
      if (existsSync(final1)) unlinkSync(final1);
    } catch {}

    try {
      const w1 = new SSTWriter(tmp1, final1, {
        blockSize: 1024,
        compressionAlgo: sstable.COMPRESSION_DEFLATE,
        compressionThreshold: 1,
        adaptiveCompression: true,
        compressionSampleSize: 512,
        minCompressionRatio: 0.95,
        useBloom: false,
      });
      const repeated = Buffer.alloc(800, "A");
      for (let i = 0; i < 10; i++) {
        w1.add(Buffer.from("k" + i), repeated, i, i * 10, 1000 + i);
      }
      w1.finish();

      // inspect index for compressed flag
      const fd = openSync(final1, "r");
      let foundCompressedGood = false;
      try {
        const stat = fstatSync(fd);
        const footer = Buffer.alloc(28);
        readSync(fd, footer, 0, 28, stat.size - 28);
        const indexOffset = footer.readBigUInt64BE(0);
        const bloomOffset = footer.readBigUInt64BE(8);
        const indexEnd =
          bloomOffset && bloomOffset > indexOffset
            ? Number(bloomOffset)
            : stat.size - 28;
        const indexSize = indexEnd - Number(indexOffset);
        const indexBuf = Buffer.alloc(indexSize);
        readSync(fd, indexBuf, 0, indexSize, Number(indexOffset));
        let pos = 0;
        const blockCount = indexBuf.readUInt32BE(pos);
        pos += 4;
        for (let i = 0; i < blockCount; i++) {
          const fklen = indexBuf.readUInt32BE(pos);
          pos += 4 + fklen;
          const of = indexBuf.readBigUInt64BE(pos);
          pos += 8;
          const blen = indexBuf.readUInt32BE(pos);
          pos += 4;
          const bcks = indexBuf.readUInt32BE(pos);
          pos += 4;
          if ((blen & sstable.BLOCK_COMPRESSED_FLAG) !== 0)
            foundCompressedGood = true;
        }
      } finally {
        try {
          closeSync(fd);
        } catch {}
      }

      expect(foundCompressedGood).toBe(true);
    } finally {
      try {
        if (existsSync(tmp1)) unlinkSync(tmp1);
      } catch {}
    }

    // incompressible (random) SST
    const tmp2 = "./data/adaptive_comp_rand.tmp";
    const final2 = "./data/adaptive_comp_rand.sst";
    try {
      if (existsSync(tmp2)) unlinkSync(tmp2);
      if (existsSync(final2)) unlinkSync(final2);
    } catch {}

    try {
      const w2 = new SSTWriter(tmp2, final2, {
        blockSize: 1024,
        compressionAlgo: sstable.COMPRESSION_DEFLATE,
        compressionThreshold: 1,
        adaptiveCompression: true,
        compressionSampleSize: 512,
        minCompressionRatio: 0.95,
        useBloom: false,
      });
      for (let i = 0; i < 10; i++) {
        const val = randomBytes(800);
        w2.add(Buffer.from("r" + i), val, i, i * 10, 2000 + i);
      }
      w2.finish();

      const fd2 = openSync(final2, "r");
      let foundCompressedRand = false;
      try {
        const stat = fstatSync(fd2);
        const footer = Buffer.alloc(28);
        readSync(fd2, footer, 0, 28, stat.size - 28);
        const indexOffset = footer.readBigUInt64BE(0);
        const bloomOffset = footer.readBigUInt64BE(8);
        const indexEnd =
          bloomOffset && bloomOffset > indexOffset
            ? Number(bloomOffset)
            : stat.size - 28;
        const indexSize = indexEnd - Number(indexOffset);
        const indexBuf = Buffer.alloc(indexSize);
        readSync(fd2, indexBuf, 0, indexSize, Number(indexOffset));
        let pos = 0;
        const blockCount = indexBuf.readUInt32BE(pos);
        pos += 4;
        for (let i = 0; i < blockCount; i++) {
          const fklen = indexBuf.readUInt32BE(pos);
          pos += 4 + fklen;
          const of = indexBuf.readBigUInt64BE(pos);
          pos += 8;
          const blen = indexBuf.readUInt32BE(pos);
          pos += 4;
          const bcks = indexBuf.readUInt32BE(pos);
          pos += 4;
          if ((blen & sstable.BLOCK_COMPRESSED_FLAG) !== 0)
            foundCompressedRand = true;
        }
      } finally {
        try {
          closeSync(fd2);
        } catch {}
      }

      // Expect adaptive compression to skip compressing random blocks
      expect(foundCompressedRand).toBe(false);
    } finally {
      try {
        if (existsSync(tmp2)) unlinkSync(tmp2);
      } catch {}
    }
  });
});
