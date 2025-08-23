import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import Manifest from "../libs/storage/manifest";
const makeTempDir = require("./util/tmpdir");

describe("engine compactor options", () => {
  it("runs compaction using configured maxSstSize", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: 128 } as any,
      });
      // ensure manifest exists
      const m = Manifest.load(dir);
      // call compactNow (should not throw)
      await e.compactNow();
      // manifest should still load
      const m2 = Manifest.load(dir);
      expect(m2).toBeTruthy();
    });
  });
});
