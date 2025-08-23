import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import Manifest from "../libs/storage/manifest";
import Engine from "../libs/storage/engine";
const makeTempDir = require("./util/tmpdir");
import { existsSync, statSync } from "node:fs";

// Ensure per-level targets are honored and produced SSTs for each level are <= target
describe("compactor per-level targets", () => {
  it("respects perLevelMax settings for output SST size", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);
    // create input SSTs
    for (let i = 0; i < 6; i++) {
      const f = `${dir}/in${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 64, false);
      for (let k = 0; k < 6; k++)
        w.add(Buffer.from(`k${i}_${k}`), Buffer.from("v".repeat(64)), 0, 0, 1);
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

    // per-level max: leave level0 default, but target level1 outputs to small size
    const perLevel = [0, 400, 800];
    const e = new Engine(dir, "log.wal", {
      compactorOptions: { maxSstSize: 1024, perLevelMax: perLevel } as any,
    });
    await e.open();
    const res = await e.compactNow();
    // reload manifest and verify level1 files
    const persisted = Manifest.load(dir);
    const lvl1 = persisted.listFilesByLevel(1);
    expect(lvl1.length).toBeGreaterThan(0);
    const target = Number(perLevel[1] ?? 0);
    for (const f of lvl1) {
      const st = statSync(f.file);
      expect(st.size).toBeLessThanOrEqual(target);
    }
    await e.close();
  });
});
