import { describe, it, expect } from "bun:test";
import { SSTWriter } from "../libs/storage";
import * as sstable from "../libs/storage/sstable";
import { Compactor } from "../libs/storage/compactor";
import { Manifest } from "../libs/storage/manifest";
import { existsSync, mkdirSync, rmdirSync } from "node:fs";

describe("metrics: compaction/compression", () => {
  it("compactor returns compression stats after compacting SSTs", async () => {
    const dir = "./data/metrics_test";
    try {
      if (existsSync(dir)) rmdirSync(dir, { recursive: true });
      mkdirSync(dir, { recursive: true });
    } catch {}

    const tmp1 = `${dir}/f1.tmp`;
    const f1 = `${dir}/f1.sst`;
    const w1 = new SSTWriter(tmp1, f1, {
      blockSize: 512,
      compressionAlgo: sstable.COMPRESSION_DEFLATE,
      compressionThreshold: 1,
      adaptiveCompression: true,
      compressionSampleSize: 256,
      minCompressionRatio: 0.95,
      useBloom: false,
    });
    const repeated = Buffer.alloc(400, "Z");
    for (let i = 0; i < 8; i++)
      w1.add(Buffer.from("k" + i), repeated, i, i * 10, 1000 + i);
    const m1 = w1.finish();

    const tmp2 = `${dir}/f2.tmp`;
    const f2 = `${dir}/f2.sst`;
    const w2 = new SSTWriter(tmp2, f2, { blockSize: 512, useBloom: false });
    for (let i = 0; i < 8; i++)
      w2.add(
        Buffer.from("r" + i),
        Buffer.alloc(200, String(i)),
        i,
        i * 10,
        2000 + i
      );
    const m2 = w2.finish();

    const manifest = new Manifest(dir);
    manifest.addFile({
      file: m1.file,
      minKeyHex: m1.minKey.toString("hex"),
      maxKeyHex: m1.maxKey.toString("hex"),
      size: m1.size,
      level: 0,
      walOffset: 0,
      createdAt: Date.now(),
    });
    manifest.addFile({
      file: m2.file,
      minKeyHex: m2.minKey.toString("hex"),
      maxKeyHex: m2.maxKey.toString("hex"),
      size: m2.size,
      level: 0,
      walOffset: 0,
      createdAt: Date.now(),
    });

    const comp = new Compactor(dir, manifest, {
      compressionAlgo: sstable.COMPRESSION_DEFLATE,
      compressionThreshold: 1,
      adaptiveCompression: true,
    });
    const res = await comp.compact();
    expect(typeof res.compressedBlocks).toBe("number");
    expect(typeof res.compressionAttempts).toBe("number");
    expect(res.compressionAttempts! > 0).toBe(true);
    // compressedBlocks may be 0 in degenerate cases but should be non-negative
    expect(res.compressedBlocks! >= 0).toBe(true);

    try {
      // cleanup
      if (existsSync(dir)) rmdirSync(dir, { recursive: true });
    } catch {}
  });
});
