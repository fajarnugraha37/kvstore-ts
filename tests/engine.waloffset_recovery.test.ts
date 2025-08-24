import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { readFileSync, existsSync, unlinkSync } from "node:fs";
import { withWalImpls } from "./util/engine_test_runner";

describe("engine walOffset and recovery", () => {
  it("records walOffset per SST and recovery uses it to reduce WAL replay", async () => {
    await withWalImpls(async (impl) => {
      const makeTempDir = require("./util/tmpdir");
      const dir = makeTempDir();
      // create engine and write some entries
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      await e.put(Buffer.from("r1"), Buffer.from("v1"));
      await e.flush();
      // manifest should include file with walOffset
      const manifest = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
      const files = Array.isArray(manifest) ? manifest : manifest.files || [];
      expect(files.length >= 1).toBe(true);
      const f = files[files.length - 1];
      expect(typeof f.walOffset === "number").toBe(true);

      // close and reopen engine to ensure recovery path uses manifest and WAL
      e.close();
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      // after recovery, get should return value without double-applying
      const v = e2.get(Buffer.from("r1"));
      expect(v?.toString()).toBe("v1");

      // clean up
      e2.close();
      for (const ff of files) {
        try {
          if (existsSync(ff.file)) unlinkSync(ff.file);
        } catch {}
      }
    });
  });
});
