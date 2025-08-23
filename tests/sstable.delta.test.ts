import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
const makeTempDir = require("./util/tmpdir");
import { existsSync, unlinkSync } from "node:fs";

describe("sstable delta size", () => {
  it("deltaSizeForEntry approximates actual growth", () => {
    const dir = makeTempDir();
    const tmp = `${dir}/d.tmp`;
    const final = `${dir}/d.sst`;
    const w = new SSTWriter(tmp, final, 64, false);
    const before = w.getEstimatedSize();
    const key = Buffer.from("hello");
    const val = Buffer.from("world");
    const delta = w.deltaSizeForEntry(key, val);
    // add and finish
    w.add(key, val, 0, 0, 1);
    const after = w.getEstimatedSize();
    expect(after - before).toBeGreaterThanOrEqual(delta - 16); // allow small difference
    // finish to exercise writer finish
    const meta = w.finish();
    expect(existsSync(meta.file)).toBe(true);
    try {
      unlinkSync(meta.file);
    } catch {}
  });
});
