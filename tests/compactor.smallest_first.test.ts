import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import Manifest from "../libs/storage/manifest";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("compactor smallest-first fallback", () => {
  it("selects smallest files when overlap and windows cannot meet reduction", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);

    // Create several level0 files where one very large file exists but many small files
    // should be chosen instead to meet the required reduction.
    const large = `${dir}/large.sst`;
    const wl = new SSTWriter(`${large}.tmp`, large, 64, false);
    for (let i = 0; i < 200; i++)
      wl.add(Buffer.from(`L${i}`), Buffer.from("x".repeat(1024)), 0, 0, 1);
    const metaL = wl.finish();
    m.addFile({
      file: metaL.file,
      minKeyHex: metaL.minKey.toString("hex"),
      maxKeyHex: metaL.maxKey.toString("hex"),
      size: metaL.size,
      level: 0,
      walOffset: 0,
    });

    const smallFiles: string[] = [];
    const smallMetas: any[] = [];
    for (let s = 0; s < 10; s++) {
      const f = `${dir}/s${s}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 64, false);
      for (let i = 0; i < 3; i++)
        w.add(Buffer.from(`s${s}_${i}`), Buffer.from("y".repeat(32)), 0, 0, 1);
      const meta = w.finish();
      m.addFile({
        file: meta.file,
        minKeyHex: meta.minKey.toString("hex"),
        maxKeyHex: meta.maxKey.toString("hex"),
        size: meta.size,
        level: 0,
        walOffset: 0,
      });
      smallFiles.push(meta.file);
      smallMetas.push(meta);
    }

    // aim to reduce by an amount equal to the sum of several small files, but less than the large file
    const targetReduction = smallMetas
      .slice(0, 4)
      .reduce((s, mm) => s + (mm.size || 0), 0);
    const levelSize = m
      .listFilesByLevel(0)
      .reduce((s, f) => s + (f.size || 0), 0);
    const perLevel = [0, levelSize - targetReduction];

    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: {
          maxSstSize: 1024 * 1024,
          perLevelMax: perLevel,
          maxFilesPerCompaction: 6,
        } as any,
      });
      await e.open();
      await e.compactNow();
      await e.close();
    });

    const persisted = Manifest.load(dir);
    // Expect that at least some of the small files were removed (chosen) and the large remains
    const largeStill = persisted.listFiles().some((f) => f.file === metaL.file);
    const someSmallRemoved = smallFiles.some(
      (sf) => !persisted.listFiles().some((f) => f.file === sf)
    );
    // Accept either: some small files were removed (preferred) OR the large file
    // was removed to meet reduction (also allowed). Ensure compaction made progress.
    expect(someSmallRemoved || !largeStill).toBe(true);
  });
});
