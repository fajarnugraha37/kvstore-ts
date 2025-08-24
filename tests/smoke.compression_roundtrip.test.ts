import { describe, it, expect } from "bun:test";
import { SSTWriter, SSTReader, COMPRESSION_DEFLATE } from "../libs/storage";
import { existsSync, rmdirSync, mkdirSync } from "node:fs";

describe("smoke: compression round-trip", () => {
  it("writes compressed SST and reads back the original value", async () => {
    const dir = "./data/smoke_compress";
    try {
      if (existsSync(dir)) rmdirSync(dir, { recursive: true });
      mkdirSync(dir, { recursive: true });
    } catch {}

    const tmp = `${dir}/c.tmp`;
    const file = `${dir}/c.sst`;

    const w = new SSTWriter(tmp, file, {
      blockSize: 64,
      compressionAlgo: COMPRESSION_DEFLATE,
      compressionThreshold: 16,
    });
    const key = Buffer.from("hello");
    const val = Buffer.from("a".repeat(200));
    w.add(key, val, 1, 0, Date.now());
    w.finish();

    const r = SSTReader.open(file);
    const got = r.get(key);
    expect(got).not.toBeNull();
    expect(got!.equals(val)).toBe(true);
  });
});
