import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage/sstwriter";
import Manifest from "../libs/storage/manifest";
import Compactor from "../libs/storage/compactor";
import { existsSync, unlinkSync, mkdirSync, readdirSync } from "node:fs";
import { dirname } from "node:path";

// This test spies on throttle.maybeConsumePerEntry by replacing it with a fast no-op spy
// so we can assert that compaction calls it without incurring real sleeps.

describe("compactor per-entry throttle spy", () => {
  it("calls maybeConsumePerEntry during compaction", async () => {
    const testDir = "./data/test_compactor_spy";
    try {
      try {
        if (!existsSync(testDir)) mkdirSync(testDir, { recursive: true });
      } catch (e) {}
      // create 3 small SST files
      const files: any[] = [];
      for (let i = 0; i < 3; i++) {
        const tmp = `${testDir}/f${i}.sst.tmp`;
        const final = `${testDir}/f${i}.sst`;
        try {
          if (existsSync(tmp)) unlinkSync(tmp);
          if (existsSync(final)) unlinkSync(final);
        } catch (e) {}
        const w = new SSTWriter(tmp, final, {
          blockSize: 1024,
          useBloom: false,
        });
        for (let j = 0; j < 50; j++) {
          const k = Buffer.from(`k${i}_${String(j).padStart(3, "0")}`);
          const v = Buffer.alloc(256, String(j % 10));
          w.add(k, v, j, j * 10, Date.now());
        }
        const meta = w.finish();
        files.push(meta);
      }

      const manifest = new Manifest(testDir);
      for (const f of files) {
        manifest.addFile({
          file: f.file,
          minKeyHex: f.minKey.toString("hex"),
          maxKeyHex: f.maxKey.toString("hex"),
          size: f.size,
          level: 0,
          walOffset: 0,
        });
      }

      // Spy: use test-only setter to inject a counter that returns immediately (no throttling)
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const throttle = require("../libs/storage/throttle");
      let calls = 0;
      const origSetter = throttle.__setMaybeConsumeForTests;
      throttle.__setMaybeConsumeForTests(async function (
        tokenBucket: any,
        writer: any,
        key: any,
        val: any,
        rev: any
      ) {
        calls++;
        return;
      });

      const compactor = new Compactor(testDir, manifest, {
        bytesPerSecond: 1024 * 10,
        maxSstSize: 1024 * 2,
      });
      const res = await compactor.compact();

      // restore original
      if (typeof origSetter === "function") origSetter(null);

      expect(calls).toBeGreaterThan(0);
      // compaction should have created at least one file
      expect(res.filesCreated).toBeGreaterThanOrEqual(1);
    } finally {
      // cleanup produced files
      try {
        const items = readdirSync(testDir);
        for (const it of items) {
          try {
            unlinkSync(`${testDir}/${it}`);
          } catch (e) {}
        }
      } catch {}
    }
  }, 20000);
});
