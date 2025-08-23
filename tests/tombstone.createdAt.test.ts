import { describe, it, expect } from "bun:test";
import Manifest from "../libs/storage/manifest";
import { SSTWriter } from "../libs/storage/sstwriter";
import Compactor from "../libs/storage/compactor";
import { SSTReader } from "../libs/storage/sstreader";

const makeTempDir = require("./util/tmpdir");

describe("tombstone TTL and manifest createdAt", () => {
  it("manifest entries created by compaction have createdAt", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);
    const f = `${dir}/a.sst`;
    const w = new SSTWriter(`${f}.tmp`, f);
    w.add(Buffer.from("a"), Buffer.from("v1"), 1, 0, 1);
    w.finish();

    // add without createdAt explicitly so Manifest will populate it
    m.addFile({
      file: f,
      minKeyHex: Buffer.from("a").toString("hex"),
      maxKeyHex: Buffer.from("a").toString("hex"),
      size: 0,
    });

    const comp = new Compactor(dir, m, { maxSstSize: 1024 * 1024 });
    await comp.compact();

    const files = m.listFiles();
    expect(files.length).toBeGreaterThan(0);
    for (const fm of files) {
      expect(typeof fm.createdAt).toBe("number");
      expect(Number.isFinite(fm.createdAt)).toBe(true);
    }
  });

  it("drops tombstone older than tombstoneRetentionMs", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);
    const f = `${dir}/t_old.sst`;
    const w = new SSTWriter(`${f}.tmp`, f);
    w.add(Buffer.from("k"), null, 1, 0, 1);
    w.finish();

    // explicitly set createdAt in the past so compactor considers it old
    const old = Date.now() - 1000 * 60 * 60; // 1 hour ago
    m.addFile({
      file: f,
      minKeyHex: Buffer.from("k").toString("hex"),
      maxKeyHex: Buffer.from("k").toString("hex"),
      size: 0,
      createdAt: old,
    });

    // retention of 1 second => tombstone should be dropped. Compaction may replace
    // the original SST with an empty SST; assert the tombstone is not present in
    // any surviving SST reader output.
    const comp = new Compactor(dir, m, {
      maxSstSize: 1024 * 1024,
      tombstoneRetentionMs: 1000,
    });
    await comp.compact();
    const files = m.listFiles();
    // if manifest is empty, tombstone removed; otherwise inspect surviving SSTs
    if (files.length === 0) return;
    for (const fm of files) {
      const r = SSTReader.open(fm.file);
      let found = false;
      for await (const e of r.iterator()) {
        if (e && e.key && e.key.equals(Buffer.from("k"))) {
          found = true;
          break;
        }
      }
      expect(found).toBe(false);
    }
  });

  it("retains recent tombstone within retention window", async () => {
    const dir = makeTempDir();
    const m = new Manifest(dir);
    const f = `${dir}/t_new.sst`;
    const w = new SSTWriter(`${f}.tmp`, f);
    w.add(Buffer.from("k"), null, 1, 0, 1);
    w.finish();

    m.addFile({
      file: f,
      minKeyHex: Buffer.from("k").toString("hex"),
      maxKeyHex: Buffer.from("k").toString("hex"),
      size: 0,
      createdAt: Date.now(),
    });

    const comp = new Compactor(dir, m, {
      maxSstSize: 1024 * 1024,
      tombstoneRetentionMs: 1000,
    });
    await comp.compact();
    const files = m.listFiles();
    expect(files.length).toBe(1);
  });
});
