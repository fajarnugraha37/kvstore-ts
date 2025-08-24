import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader } from "../libs/storage";
import { unlinkSync, existsSync } from "node:fs";

describe("sstable write/read", () => {
  it("write and read entries", () => {
    const tmp = "./data/test.sst.tmp";
    const final = "./data/test.sst";
    try {
      if (existsSync(tmp)) unlinkSync(tmp);
      if (existsSync(final)) unlinkSync(final);
    } catch {}

    const w = new SSTWriter(tmp, final);
    w.add(Buffer.from("a"), Buffer.from("1"), 0, 0, 1);
    w.add(Buffer.from("b"), Buffer.from("2"), 0, 0, 1);
    w.add(Buffer.from("c"), null, 0, 0, 1);
    const meta = w.finish();
    expect(meta.file).toBe(final);

    const r = SSTReader.open(final);
    const v = r.get(Buffer.from("b"));
    expect(v?.toString()).toBe("2");
    const v2 = r.get(Buffer.from("c"));
    expect(v2).toBeNull();
  });
});
