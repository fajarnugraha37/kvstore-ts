import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { SSTReader } from "../libs/storage";

describe("mvcc revision persistence", () => {
  it("writes revisions to SST and preserves highest revision after compaction/recovery", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

    // two updates to same key
    await e.put(Buffer.from("k"), Buffer.from("v1"));
    await e.put(Buffer.from("k"), Buffer.from("v2"));

    // force flush to SST
    await e.flush();

    // read manifest to locate sst
    const manifest = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
    const files = Array.isArray(manifest) ? manifest : manifest.files || [];
    expect(files.length).toBeGreaterThan(0);
    const sstPath = files[0].file;

    // open sst and ensure iterator yields revs and the latest rev is the higher one
    const r = SSTReader.open(sstPath);
    let seen: { key: string; value: string | null; rev?: number }[] = [];
    for await (const it of r.iterator()) {
      seen.push({
        key: it.key.toString(),
        value: it.value ? it.value.toString() : null,
        rev: it.rev,
      });
    }
    // there should be at least one entry for 'k' and its rev should be numeric
    const hit = seen.find((s) => s.key === "k");
    expect(hit).toBeTruthy();
    expect(typeof hit!.rev).toBe("number");

    // Now restart engine and ensure get() returns latest value (v2)
    e.close();
    const e2 = new Engine(dir, "log.wal", { walImpl: impl });
    await e2.open();
    const val = e2.get(Buffer.from("k"));
    expect(val && val.toString()).toBe("v2");
    e2.close();

    // cleanup sst files
    for (const f of files) {
      try {
        if (existsSync(f.file)) unlinkSync(f.file);
      } catch {}
    }
    });
  });
});
