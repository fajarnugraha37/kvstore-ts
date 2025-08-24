import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader } from "../libs/storage";
import { Manifest } from "../libs/storage/manifest";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");
import { existsSync, statSync, unlinkSync } from "node:fs";

describe("compactor strict size enforcement", () => {
  it("ensures compacted SST files are <= configured maxSstSize (zero tolerance)", async () => {
    const dir = makeTempDir();
    const maxSst = 300; // bytes

    const m = new Manifest(dir);

    // create several small SSTs with distinct key ranges
    for (let i = 0; i < 6; i++) {
      const f = `${dir}/in${i}.sst`;
      const w = new SSTWriter(`${f}.tmp`, f, 32, false);
      // add several keys to each file to make them non-trivial
      for (let k = 0; k < 5; k++) {
        const key = Buffer.from(`k${i}_${k}`);
        const val = Buffer.from("v".repeat(16));
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

    // Start Engine with compactorOptions and background compaction enabled
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: maxSst } as any,
      });
      // open engine to ensure background compactor starts
      await e.open();

      // wait up to 5s for compaction to run and manifest to be updated
      const deadline = Date.now() + 5000;
      let success = false;
      while (Date.now() < deadline) {
        // reload persisted manifest so we observe Engine-updated entries
        const persisted = Manifest.load(dir);
        const files = persisted.listFilesByLevel(1);
        if (files.length > 0) {
          // check sizes on disk
          let ok = true;
          for (const f of files) {
            try {
              if (!existsSync(f.file)) {
                ok = false;
                break;
              }
              const r = SSTReader.open(f.file);
              // file size from manifest should be exact; assert size <= maxSst
              if (typeof f.size === "number") {
                if (f.size > maxSst) {
                  ok = false;
                  break;
                }
              } else {
                // fallback: if manifest lacks size, use reader to approximate by reading footer; remove if larger
                // open throws if file corrupted
                // use fs.stat to read size
                const stat = statSync(f.file);
                if (stat.size > maxSst) {
                  ok = false;
                  break;
                }
              }
            } catch (e) {
              ok = false;
              break;
            }
          }
          if (ok) {
            success = true;
            break;
          }
        }
        await new Promise((r) => setTimeout(r, 200));
      }

      // cleanup
      try {
        await e.close();
      } catch {}
      expect(success).toBe(true);
    });
  });
});
