import { describe, it, expect } from "bun:test";
import {
  openSync,
  writeSync,
  closeSync,
  unlinkSync,
  existsSync,
} from "node:fs";
import { SSTWriter, SSTReader } from "../libs/storage";

describe("sstable block corruption", () => {
  it("detects corrupted block checksum on get()", async () => {
    const tmp = "./data/corrupt.tmp";
    const final = "./data/corrupt.sst";
    try {
      const w = new SSTWriter(tmp, final, 64, false);
      // create many small entries to ensure multiple blocks
      for (let i = 0; i < 200; i++) {
        w.add(Buffer.from("k" + i), Buffer.from("v" + i), 0, 0, 1);
      }
      const meta = w.finish();
      expect(meta.file).toBe(final);

      // open reader to load index
      const r = SSTReader.open(meta.file);

      // corrupt the first block on disk (use test-only access to private index)
      const anyr = r as any;
      const fd = openSync(meta.file, "r+");
      try {
        const idx = anyr.index as any[] | undefined;
        if (!idx || idx.length === 0) throw new Error("no index in sst (test)");
        const off = Number(idx[0].offset);
        // write a changed byte into the block content
        writeSync(fd, Buffer.from([0xab]), 0, 1, off + 5);
      } finally {
        closeSync(fd);
      }

      // get should detect checksum mismatch and throw
      let saw = false;
      try {
        r.get(Buffer.from("k0"));
      } catch (e: any) {
        saw = /checksum/i.test(String(e));
      }
      expect(saw).toBe(true);
    } finally {
      try {
        if (existsSync("./data/corrupt.sst")) unlinkSync("./data/corrupt.sst");
      } catch {}
      try {
        if (existsSync("./data/corrupt.tmp")) unlinkSync("./data/corrupt.tmp");
      } catch {}
    }
  });
});
