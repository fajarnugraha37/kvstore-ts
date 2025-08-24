import { describe, it, expect } from "bun:test";
import { Wal } from "../libs/wal/wall";
import fs, { unlinkSync, existsSync } from "node:fs";

// This test ensures global offsets are monotonic across rotations

describe("wal global offsets", () => {
  it("global offsets increase across rotated segments", async () => {
    const makeTempDir = require("./util/tmpdir");
    const dir = makeTempDir();
    // cleanup
    try {
      if (existsSync(`${dir}/log.wal`)) unlinkSync(`${dir}/log.wal`);
    } catch {}
    try {
      if (existsSync(`${dir}/log.wal.meta.json`))
        unlinkSync(`${dir}/log.wal.meta.json`);
    } catch {}
    try {
      const files = fs
        .readdirSync(dir)
        .filter((f) => f.startsWith("log.wal.seg."));
      for (const f of files) fs.unlinkSync(`${dir}/${f}`);
    } catch {}

    const w = new Wal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();

    const offs: number[] = [];
    for (let round = 0; round < 3; round++) {
      // append a few entries
      for (let i = 0; i < 5; i++) {
        await w.append({ key: `k${round}_${i}`, value: `v${round}_${i}` });
        // read current end
        const e = (w as any).currentEndOffset();
        offs.push(e as number);
      }
      // rotate using the current end offset
      const end = (w as any).currentEndOffset();
      await (w as any).truncateUpTo(end);
    }
    await w.close();

    // offsets should be strictly increasing
    for (let i = 1; i < offs.length; i++) {
      expect(typeof offs[i]).toBe("number");
      expect(typeof offs[i - 1]).toBe("number");
      expect(offs[i] as number).toBeGreaterThan(offs[i - 1] as number);
    }
  });
});
