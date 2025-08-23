import { describe, it, expect } from "bun:test";
import Manifest from "../libs/storage/manifest";
import fs from "node:fs";
import { SSTWriter } from "../libs/storage/sstwriter";
import Compactor from "../libs/storage/compactor";
import { SSTReader } from "../libs/storage/sstreader";

const makeTempDir = require("./util/tmpdir");

describe("legacy SSTs without createdAt", () => {
  it("retains tombstone when manifest entry lacks createdAt", async () => {
    const dir = makeTempDir();
    // create SST file with tombstone
    const f = `${dir}/legacy.sst`;
    const w = new SSTWriter(`${f}.tmp`, f);
    w.add(Buffer.from("z"), null, 1, 0, 1);
    w.finish();

    // simulate an older manifest (no createdAt) by writing manifest.json directly
    const manifestObj = {
      files: [
        {
          file: f,
          minKeyHex: Buffer.from("z").toString("hex"),
          maxKeyHex: Buffer.from("z").toString("hex"),
          size: 0,
          level: 0,
          walOffset: 0,
        },
      ],
      walOffset: 0,
    };
    fs.writeFileSync(
      `${dir}/manifest.json`,
      JSON.stringify(manifestObj, null, 2)
    );
    // load manifest so the in-memory Manifest sees the entry without createdAt
    const m = Manifest.load(dir);

    // run compaction with very small retention so TTL would normally drop if createdAt were present
    const comp = new Compactor(dir, m, {
      maxSstSize: 1024 * 1024,
      tombstoneRetentionMs: 1,
    });
    await comp.compact();

    const files = m.listFiles();
    // manifest should still contain the SST (we preserved createdAt fallback in manifest.addFile, but test ensures behavior)
    expect(files.length).toBeGreaterThan(0);
    // check that the tombstone is still present in SST reader output (i.e., not evicted)
    let found = false;
    for (const fm of files) {
      const r = SSTReader.open(fm.file);
      for await (const e of r.iterator()) {
        if (e && e.key && e.key.equals(Buffer.from("z"))) {
          found = true;
          break;
        }
      }
    }
    expect(found).toBe(true);
  });
});
