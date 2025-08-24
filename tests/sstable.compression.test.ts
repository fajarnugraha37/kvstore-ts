import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader } from "../libs/storage";
import * as sstable from "../libs/storage/sstable";
import {
  unlinkSync,
  existsSync,
  openSync,
  fstatSync,
  readSync,
  closeSync,
} from "node:fs";

describe("sstable compression", () => {
  it("writes compressed blocks when enabled and round-trips", async () => {
    const tmp = "./data/test_comp.sst.tmp";
    const final = "./data/test_comp.sst";
    try {
      if (existsSync(tmp)) unlinkSync(tmp);
      if (existsSync(final)) unlinkSync(final);
    } catch {}

    try {
      // enable deflate and set threshold low so blocks are considered for compression
      const w = new SSTWriter(tmp, final, {
        compressionAlgo: sstable.COMPRESSION_DEFLATE,
        compressionThreshold: 1,
        useBloom: false,
      });
      // write many highly-compressible entries
      const repeated = Buffer.alloc(1024, "A");
      for (let i = 0; i < 200; i++) {
        const k = "k" + String(i).padStart(3, "0");
        w.add(Buffer.from(k), repeated, i, i * 10, 1000 + i);
      }
      const meta = w.finish();
      expect(meta.file).toBe(final);

      const r = SSTReader.open(final);
      // round-trip basic checks
      // dump first few iterator entries for debugging
      let iterCount = 0;
      for await (const it of r.iterator()) {
        if (iterCount < 10)
          console.error(
            "ITER",
            iterCount,
            it.key.toString(),
            "valLen",
            it.value?.length,
            "rev",
            it.rev,
            "wal",
            it.walOffset,
            "created",
            it.createdAt
          );
        iterCount++;
        if (iterCount > 200) break;
      }
      console.error("ITER COUNT", iterCount);
      const got = r.getWithRev(Buffer.from("k" + String(10).padStart(3, "0")));
      console.error("DEBUG got:", got);
      expect(got.value?.length).toBe(repeated.length);
      expect(got.rev).toBe(10);
      expect(got.walOffset).toBe(100);
      expect(got.createdAt).toBe(1010);

      // now inspect index to ensure at least one block has compressed flag set
      const fd = openSync(final, "r");
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
        if (indexSize < 0) {
          console.error(
            "DEBUG: indexOffset=",
            indexOffset.toString(),
            "indexEnd=",
            indexEnd,
            "stat.size=",
            stat.size
          );
          throw new Error("invalid index size: " + indexSize);
        }
        const indexBuf = Buffer.alloc(indexSize);
        readSync(fd, indexBuf, 0, indexSize, Number(indexOffset));
        let pos = 0;
        const blockCount = indexBuf.readUInt32BE(pos);
        pos += 4;
        let foundCompressed = false;
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
            foundCompressed = true;
        }
        expect(foundCompressed).toBe(true);
      } finally {
        try {
          closeSync(fd);
        } catch {}
      }
    } finally {
      try {
        if (existsSync(tmp))
          unlinkSync(
            tmp
          ); /* keep final for debugging: if (existsSync(final)) unlinkSync(final); */
      } catch {}
    }
  });
});
