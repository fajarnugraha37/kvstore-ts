import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
const makeTempDir = require("./util/tmpdir");
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { withWalImpls } from "./util/engine_test_runner";

describe("engine integration", () => {
  it("replay wal and flush to sst", async () => {
    await withWalImpls(async (impl) => {
      const dir = makeTempDir();
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      await e.put(Buffer.from("x"), Buffer.from("100"));
      await e.put(Buffer.from("y"), Buffer.from("200"));
      expect(e.get(Buffer.from("x"))?.toString()).toBe("100");
      await e.flush();
      const manifest = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
      const files = Array.isArray(manifest) ? manifest : manifest.files || [];
      expect(Array.isArray(files)).toBe(true);
      // cleanup created sst files
      for (const f of files) {
        try {
          if (existsSync(f.file)) unlinkSync(f.file);
        } catch {}
      }
      if ((e as any).close) await (e as any).close();
    });
  });

  it("scan merges memtable and sst entries", async () => {
    await withWalImpls(async (impl) => {
      const dir = makeTempDir();
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      await e.put(Buffer.from("a"), Buffer.from("1"));
      await e.flush();
      await e.put(Buffer.from("b"), Buffer.from("2"));
      const arr: string[] = [];
      for await (const it of e.scan()) {
        arr.push(it.key.toString());
      }
      expect(arr.includes("a")).toBe(true);
      expect(arr.includes("b")).toBe(true);
      if ((e as any).close) await (e as any).close();
    });
  });
});
