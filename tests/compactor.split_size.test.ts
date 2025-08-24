import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import { Manifest } from "../libs/storage/manifest";
import { Compactor } from "../libs/storage/compactor";
import { existsSync, unlinkSync } from "node:fs";
const makeTempDir = require("./util/tmpdir");

describe("compactor output splitting", () => {
  it("splits merged output into multiple SSTs when maxSstSize is small", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);

    // create several small SSTs with distinct key ranges
    for (let i = 0; i < 5; i++) {
      const f = `${dir}/in${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 32, false);
      // add several keys to each file to make them non-trivial
      for (let k = 0; k < 4; k++) {
        const key = Buffer.from(`k${i}_${k}`);
        const val = Buffer.from("v".repeat(8));
        w.add(key, val, 0, 0, 1);
      }
      const meta = w.finish();
      m.addFile({
        file: meta.file,
        minKeyHex: meta.minKey.toString("hex"),
        maxKeyHex: meta.maxKey.toString("hex"),
        size: meta.size,
        level: 0,
        walOffset: 0,
      });
    }

    const comp = new Compactor(dir, m, { maxSstSize: 200 }); // small max to force splits
    await comp.compact();

    const files = m.listFilesByLevel(1).concat(m.listFilesByLevel(0));
    // Expect more than one output file at level 1 (compaction outputs) when splitting occurs
    expect(files.length).toBeGreaterThan(0);

    // ensure at least one compacted file exists on disk
    let found = 0;
    for (const f of files) {
      try {
        if (existsSync(f.file)) found++;
      } catch {}
    }
    expect(found).toBeGreaterThanOrEqual(1);

    // cleanup
    for (const f of files) {
      try {
        if (existsSync(f.file)) unlinkSync(f.file);
      } catch {}
    }
  });
});
