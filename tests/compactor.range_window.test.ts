import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import { Manifest } from "../libs/storage/manifest";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("compactor range-window fallback", () => {
  it("selects the smallest contiguous key-range window when overlap is absent", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);

    // create 6 level0 SSTs with varying sizes by controlling value payload
    const metas: any[] = [];
    for (let i = 0; i < 6; i++) {
      const f = `${dir}/r${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 64, false);
      const num = i === 1 || i === 2 || i === 3 ? 50 : 10; // make middle three larger
      // add repeated entries to inflate size
      for (let e = 0; e < num; e++) {
        w.add(Buffer.from(`k${i}_${e}`), Buffer.from("v".repeat(128)), 0, 0, 1);
      }
      const meta = w.finish();
      metas.push(meta);
      m.addFile({
        file: meta.file,
        minKeyHex: meta.minKey.toString("hex"),
        maxKeyHex: meta.maxKey.toString("hex"),
        size: meta.size,
        level: 0,
        walOffset: 0,
      });
    }

    // compute levelSize and choose desiredReduction = sum of metas[1..3]
    const levelFiles = m.listFilesByLevel(0);
    const levelSize = levelFiles.reduce((s, f) => s + (f.size || 0), 0);
    const desiredReduction =
      (metas[1].size || 0) + (metas[2].size || 0) + (metas[3].size || 0);
    const perLevelMax = [0, levelSize - desiredReduction];

    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: {
          maxSstSize: 1024 * 1024,
          perLevelMax: perLevelMax,
        } as any,
      });
      await e.open();
      await e.compactNow();
      await e.close();
    });

    const persisted = Manifest.load(dir);
    // Expect that the contiguous block r1,r2,r3 were selected/removed
    const removed = [metas[1].file, metas[2].file, metas[3].file].every(
      (p) => !persisted.listFiles().some((f) => f.file === p)
    );
    expect(removed).toBe(true);
  });
});
