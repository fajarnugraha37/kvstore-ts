import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import { Compactor } from "../libs/storage/compactor";
import { Manifest } from "../libs/storage/manifest";
import { existsSync, rmdirSync, mkdirSync } from "node:fs";

describe("smoke: compactor level-target default", () => {
  it("compacts level-0 when perLevelMax not provided (default behavior)", async () => {
    const dir = "./data/compactor_smoke_default";
    try {
      if (existsSync(dir)) rmdirSync(dir, { recursive: true });
      mkdirSync(dir, { recursive: true });
    } catch {}

    const tmp1 = `${dir}/a1.tmp`;
    const f1 = `${dir}/a1.sst`;
    const tmp2 = `${dir}/a2.tmp`;
    const f2 = `${dir}/a2.sst`;
    const tmp3 = `${dir}/a3.tmp`;
    const f3 = `${dir}/a3.sst`;

    const w1 = new SSTWriter(tmp1, f1, { blockSize: 64, useBloom: false });
    w1.add(Buffer.from("k1"), Buffer.alloc(40, "x"), 1, 0, 1000);
    w1.finish();
    const w2 = new SSTWriter(tmp2, f2, { blockSize: 64, useBloom: false });
    w2.add(Buffer.from("k2"), Buffer.alloc(40, "y"), 1, 0, 1001);
    w2.finish();
    const w3 = new SSTWriter(tmp3, f3, { blockSize: 64, useBloom: false });
    w3.add(Buffer.from("k3"), Buffer.alloc(40, "z"), 1, 0, 1002);
    w3.finish();

    const manifest = new Manifest(dir);
    manifest.addFile({
      file: f1,
      minKeyHex: Buffer.from("k1").toString("hex"),
      maxKeyHex: Buffer.from("k1").toString("hex"),
      size: 1,
      level: 0,
      walOffset: 0,
      createdAt: Date.now(),
    });
    manifest.addFile({
      file: f2,
      minKeyHex: Buffer.from("k2").toString("hex"),
      maxKeyHex: Buffer.from("k2").toString("hex"),
      size: 1,
      level: 0,
      walOffset: 0,
      createdAt: Date.now(),
    });
    manifest.addFile({
      file: f3,
      minKeyHex: Buffer.from("k3").toString("hex"),
      maxKeyHex: Buffer.from("k3").toString("hex"),
      size: 1,
      level: 0,
      walOffset: 0,
      createdAt: Date.now(),
    });

    const comp = new Compactor(dir, manifest, {});
    const res = await comp.compact();
    expect(typeof res.filesCreated).toBe("number");
    expect(res.filesCreated > 0).toBe(true);
  });
});
