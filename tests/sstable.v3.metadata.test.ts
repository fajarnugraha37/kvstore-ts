import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader } from "../libs/storage";
import { unlinkSync, existsSync } from "node:fs";

describe("sstable v3 per-entry metadata", () => {
  it("getWithRev returns walOffset and createdAt for v3 SST", async () => {
    const tmp = "./data/test_v3.sst.tmp";
    const final = "./data/test_v3.sst";
    try {
      if (existsSync(tmp)) unlinkSync(tmp);
      if (existsSync(final)) unlinkSync(final);
    } catch {}

    try {
      const w = new SSTWriter(tmp, final);
      // write entries with explicit walOffset and createdAt
      w.add(Buffer.from("k1"), Buffer.from("v1"), 123, 456, 789);
      w.add(Buffer.from("k2"), null, 124, 457, 790); // tombstone
      const meta = w.finish();
      expect(meta.file).toBe(final);

      const r = SSTReader.open(final);
      const res1 = r.getWithRev(Buffer.from("k1"));
      expect(res1.value?.toString()).toBe("v1");
      expect(res1.rev).toBe(123);
      expect(res1.walOffset).toBe(456);
      expect(res1.createdAt).toBe(789);

      const res2 = r.getWithRev(Buffer.from("k2"));
      expect(res2.value).toBeNull();
      expect(res2.rev).toBe(124);
      expect(res2.walOffset).toBe(457);
      expect(res2.createdAt).toBe(790);

      // iterator assertions: ensure iterator yields per-entry metadata
      const items: Array<any> = [];
      for await (const it of r.iterator()) items.push(it);
      const it1 = items.find((x) => x.key.equals(Buffer.from("k1")));
      expect(it1).toBeDefined();
      expect(it1.value?.toString()).toBe("v1");
      expect(it1.rev).toBe(123);
      expect(it1.walOffset).toBe(456);
      expect(it1.createdAt).toBe(789);

      const it2 = items.find((x) => x.key.equals(Buffer.from("k2")));
      expect(it2).toBeDefined();
      expect(it2.value).toBeNull();
      expect(it2.rev).toBe(124);
      expect(it2.walOffset).toBe(457);
      expect(it2.createdAt).toBe(790);
    } finally {
      try {
        if (existsSync(tmp)) unlinkSync(tmp);
        if (existsSync(final)) unlinkSync(final);
      } catch {}
    }
  });
});
