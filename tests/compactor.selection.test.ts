import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import Manifest from "../libs/storage/manifest";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

// This test builds a small manifest where one level-0 SST (key k2) overlaps
// multiple next-level SSTs. The overlap-weighted heuristic should prefer
// compacting that SST. We assert the level-0 SST for k2 is removed after compaction.
describe("compactor selection heuristics", () => {
  it("prefers files that overlap many next-level SSTs (overlap-weighted)", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);

    // create level1 files that overlap k1,k2 and k2,k3 (so k2 overlaps both)
    const n1 = `${dir}/n1.sst`;
    const w1 = new SSTWriter(`${n1}.tmp`, n1, 64, false);
    w1.add(Buffer.from("k1"), Buffer.from("v1"), 0, 0, 1);
    w1.add(Buffer.from("k2"), Buffer.from("v2"), 0, 0, 1);
    const meta1 = w1.finish();
    m.addFile({
      file: meta1.file,
      minKeyHex: meta1.minKey.toString("hex"),
      maxKeyHex: meta1.maxKey.toString("hex"),
      size: meta1.size,
      level: 1,
      walOffset: 0,
    });

    const n2 = `${dir}/n2.sst`;
    const w2 = new SSTWriter(`${n2}.tmp`, n2, 64, false);
    w2.add(Buffer.from("k2"), Buffer.from("v2b"), 0, 0, 1);
    w2.add(Buffer.from("k3"), Buffer.from("v3"), 0, 0, 1);
    const meta2 = w2.finish();
    m.addFile({
      file: meta2.file,
      minKeyHex: meta2.minKey.toString("hex"),
      maxKeyHex: meta2.maxKey.toString("hex"),
      size: meta2.size,
      level: 1,
      walOffset: 0,
    });

    // create level0 files k0..k4 (single-key files)
    const level0Files: string[] = [];
    for (let i = 0; i < 5; i++) {
      const f = `${dir}/f${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 64, false);
      const k = `k${i}`;
      w.add(Buffer.from(k), Buffer.from(`v${i}`), 0, 0, 1);
      const meta = w.finish();
      m.addFile({
        file: meta.file,
        minKeyHex: meta.minKey.toString("hex"),
        maxKeyHex: meta.maxKey.toString("hex"),
        size: meta.size,
        level: 0,
        walOffset: 0,
      });
      level0Files.push(meta.file);
    }

    // run compaction with perLevelMax small so compaction must pick files
    const perLevel = [0, 1024];
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: 1024, perLevelMax: perLevel } as any,
      });
      await e.open();
      await e.compactNow();
      await e.close();
    });

    // reload manifest and verify that the specific level0 file for k2 was removed
    const persisted = Manifest.load(dir);
    const remaining = persisted.listFiles();
    const f2Path = level0Files[2];
    const stillPresent = remaining.some((r) => r.file === f2Path);
    // Expect the file for k2 to be removed (compact selected it)
    expect(stillPresent).toBe(false);
  });
});
