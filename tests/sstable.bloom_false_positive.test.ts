import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader } from "../libs/storage";
import { unlinkSync, existsSync } from "node:fs";

// Measure bloom filter false positive rate for tuned parameters in SSTWriter
describe("sstable bloom filter false positive rate", () => {
  it("false positive rate stays below threshold", () => {
    const tmp = "./data/bloom.tmp";
    const final = "./data/bloom.sst";
    try {
      const n = 2000; // number of inserted keys
      const w = new SSTWriter(tmp, final, 4096, true);
      for (let i = 0; i < n; i++)
        w.add(Buffer.from("k" + i), Buffer.from("v" + i), 0, 0, 1);
      const meta = w.finish();
      const r = SSTReader.open(meta.file);

      const anyr = r as any;
      const notPresent = 5000;
      let falsePositives = 0;
      for (let i = n; i < n + notPresent; i++) {
        const k = Buffer.from("k" + i);
        const p = anyr.possiblyContains(k);
        if (p) falsePositives++;
      }
      const rate = falsePositives / notPresent;
      // Expected FP roughly below 5% for chosen parameters; give some slack
      expect(rate).toBeLessThan(0.06);
    } finally {
      try {
        if (existsSync("./data/bloom.sst")) unlinkSync("./data/bloom.sst");
      } catch {}
      try {
        if (existsSync("./data/bloom.tmp")) unlinkSync("./data/bloom.tmp");
      } catch {}
    }
  });
});
