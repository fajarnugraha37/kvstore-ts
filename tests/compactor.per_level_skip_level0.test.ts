import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import Manifest from "../libs/storage/manifest";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("compactor per-level opt-in skip level0", () => {
  it("skips level-0 compaction when perLevelMax provided and level0 size <= target", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);

    // create several very small SSTs so total size remains small
    const fileCount = 4;
    for (let i = 0; i < fileCount; i++) {
      const f = `${dir}/in${i}.sst`;
      // small block size and small values keep files tiny
      const w = new SSTWriter(`${f}.tmp`, f, 32, false);
      for (let k = 0; k < 2; k++) {
        const key = Buffer.from(`k${i}_${k}`);
        const val = Buffer.from("v");
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

    // per-level targets: set level0 target large so level0 is within target
    const perLevel = [10000, 500, 1000];
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: 1024, perLevelMax: perLevel } as any,
      });
      await e.open();
      const before = Manifest.load(dir).listFilesByLevel(0);
      expect(before.length).toBe(fileCount);

      const res = await e.compactNow();

      // After compaction, since level0 total size <= perLevel[0], there should be no level1 files
      const persisted = Manifest.load(dir);
      const lvl1 = persisted.listFilesByLevel(1);
      expect(lvl1.length).toBe(0);

      // level0 files should be unchanged (still present)
      const after0 = persisted.listFilesByLevel(0);
      expect(after0.length).toBe(before.length);

      await e.close();
    });
  });
});
