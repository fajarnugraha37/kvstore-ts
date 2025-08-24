import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader, sstableDebug } from "../libs/storage";
import { unlinkSync, existsSync } from "node:fs";

describe("sstable partial reads", () => {
  it("reads only one block for point-get", () => {
    const tmp = "./data/test_multi.sst.tmp";
    const final = "./data/test_multi.sst";
    try {
      if (existsSync(tmp)) unlinkSync(tmp);
      if (existsSync(final)) unlinkSync(final);
    } catch {}

    const w = new SSTWriter(tmp, final, 64); // small block size to force multiple blocks
    // add entries so there are several blocks
    for (let i = 0; i < 200; i++) {
      const k = Buffer.from(String(i).padStart(4, "0"));
      const v = Buffer.from("v" + i);
      w.add(k, v, 0, 0, 1);
    }
    w.finish();

    const r = SSTReader.open(final);
    sstableDebug.enabled = true;
    sstableDebug.reset();

    const v = r.get(Buffer.from("0100"));
    expect(v?.toString()).toBe("v100");
    expect(sstableDebug.blockReads).toBe(1);

    sstableDebug.enabled = false;
    // cleanup
    try {
      if (existsSync(final)) unlinkSync(final);
    } catch {}
  });
});
