import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import Manifest from "../libs/storage/manifest";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");
import { existsSync, statSync } from "node:fs";

describe("compactor manifest size fidelity", () => {
  it("writes manifest.size equal to on-disk file sizes for compacted outputs", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);
    // create input SSTs
    for (let i = 0; i < 4; i++) {
      const f = `${dir}/in${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 64, false);
      for (let k = 0; k < 8; k++) {
        w.add(Buffer.from(`k${i}_${k}`), Buffer.from("v".repeat(32)), 0, 0, 1);
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

    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: 1024 } as any,
      });
      await e.open();
      // run compaction synchronously to avoid races
      const res = await e.compactNow();
      // reload manifest
      const persisted = Manifest.load(dir);
      const files = persisted.listFiles();
      expect(files.length).toBeGreaterThan(0);
      for (const f of files) {
        if (!existsSync(f.file)) continue;
        const st = statSync(f.file);
        expect(st.size).toBe(f.size);
      }
      await e.close();
    });
  });
});
