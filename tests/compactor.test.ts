import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import { Manifest } from "../libs/storage/manifest";
import { Compactor } from "../libs/storage/compactor";
import { existsSync, unlinkSync } from "node:fs";

describe("compactor", () => {
  it("compacts multiple sst files into one and updates manifest", async () => {
    const makeTempDir = require("./util/tmpdir");
    const dir = makeTempDir();
    const m = new Manifest(dir);
    const f1 = `${dir}/c1.sst`;
    const f2 = `${dir}/c2.sst`;
    try {
      if (existsSync(f1)) unlinkSync(f1);
      if (existsSync(f2)) unlinkSync(f2);
    } catch {}

    const w1 = new SSTWriter(`${f1}.tmp`, f1);
    w1.add(Buffer.from("a"), Buffer.from("1"), 0);
    w1.add(Buffer.from("b"), Buffer.from("2"), 0);
    w1.finish();

    const w2 = new SSTWriter(`${f2}.tmp`, f2);
    w2.add(Buffer.from("b"), Buffer.from("3"), 0);
    w2.add(Buffer.from("c"), Buffer.from("4"), 0);
    w2.finish();

    m.addFile({
      file: f1,
      minKeyHex: Buffer.from("a").toString("hex"),
      maxKeyHex: Buffer.from("b").toString("hex"),
      size: 0,
    });
    m.addFile({
      file: f2,
      minKeyHex: Buffer.from("b").toString("hex"),
      maxKeyHex: Buffer.from("c").toString("hex"),
      size: 0,
    });

    const comp = new Compactor(dir, m);
    await comp.compact();

    const files = m.listFiles();
    expect(files.length).toBe(1);
    const meta = files[0];
    expect(meta).toBeDefined();
    if (meta) {
      expect(existsSync(meta.file)).toBe(true);
      // cleanup
      try {
        if (existsSync(meta.file)) unlinkSync(meta.file);
      } catch {}
    }
  });
});
