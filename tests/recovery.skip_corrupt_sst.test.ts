import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { readFileSync, writeFileSync, existsSync } from "node:fs";

describe("recovery policy", () => {
  it("skips corrupt sst on open and persists manifest changes", async () => {
    const makeTempDir = require("./util/tmpdir");
    const dir = makeTempDir();
    const e = new Engine(dir, "log.wal");
    await e.open();
    await e.put(Buffer.from("c1"), Buffer.from("v1"));
    await e.flush();
    // read manifest and pick last file
    const manifest = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
    const files = Array.isArray(manifest) ? manifest : manifest.files || [];
    expect(files.length >= 1).toBe(true);
    const f = files[files.length - 1];
    expect(existsSync(f.file)).toBe(true);

    // corrupt the file by flipping a byte near the start
    try {
      // corrupt the footer magic so SSTReader.open fails when reading index/footer
      const buf = readFileSync(f.file);
      if (buf && buf.length > 20) {
        const b = Buffer.from(buf);
        // overwrite last 4 bytes (magic) with zero
        b.writeUInt32BE(0, b.length - 4);
        writeFileSync(f.file, b);
      }
    } catch (e) {}

    // re-open engine which should skip/rename the corrupt SST and persist manifest removal
    e.close();
    const e2 = new Engine(dir, "log.wal");
    await e2.open();
    const manifest2 = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
    const files2 = Array.isArray(manifest2) ? manifest2 : manifest2.files || [];
    // ensure the corrupt file is no longer present in manifest
    expect(files2.find((x: any) => x.file === f.file)).toBeUndefined();
    e2.close();
  });
});
