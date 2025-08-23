import { describe, it, expect } from "bun:test";
const makeTempDir = require("./util/tmpdir");
import fs from "fs";
import Engine from "../libs/storage/engine";
import Manifest from "../libs/storage/manifest";

// This test ensures that legacy manifests without createdAt are handled conservatively
// and that newly written SSTs receive createdAt via engine timeProvider when present.

describe("Legacy manifest createdAt compatibility", () => {
  it("handles legacy manifest without createdAt and sets createdAt on new SSTs", async () => {
    const dir = makeTempDir();
    // create a manifest file with legacy array format (no createdAt)
    const manifestPath = `${dir}/manifest.json`;
    const fakeMeta = [
      {
        file: `${dir}/fake.sst`,
        minKeyHex: "",
        maxKeyHex: "",
        size: 0,
        level: 0,
        walOffset: 0,
      },
    ];
    fs.writeFileSync(manifestPath, JSON.stringify(fakeMeta));

    // use a time provider to observe createdAt propagation
    let now = 1600000000000;
    const tp = () => now;
    const e = new Engine(dir, "log.wal", {
      compactorOptions: {},
      timeProvider: tp,
    });
    await e.open();

    // perform a flush to create an SST and ensure manifest entry has createdAt set to timeProvider value
    const k = Buffer.from("foo");
    await e.put(k, Buffer.from("bar"));
    await e.flush();
    const m = Manifest.load(dir);
    const files = m.listFiles();
    expect(files.length).toBeGreaterThan(0);
    // Only assert createdAt for SSTs created by engine (names that start with 'sst_')
    const sstFiles = files.filter(
      (f) => f.file.includes("/sst_") || f.file.includes("\\sst_")
    );
    expect(sstFiles.length).toBeGreaterThan(0);
    for (const f of sstFiles) {
      expect(typeof f.createdAt).toBe("number");
      expect(f.createdAt).toBe(now);
    }

    await e.close();
  });
});
