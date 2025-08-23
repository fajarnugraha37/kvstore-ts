import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { existsSync, unlinkSync, statSync } from "node:fs";
import * as sstable from "../libs/storage/sstable";

describe("engine compression wiring", () => {
  it("uses engine-level compression options to produce compressed SSTs that round-trip", async () => {
    const dir = "./data";
    const wal = "log.wal";
    // cleanup any previous artifacts
    try {
      if (existsSync(`${dir}/sst_uncompressed.sst`))
        unlinkSync(`${dir}/sst_uncompressed.sst`);
    } catch {}

    // Start engine with compression enabled at engine level
    const e = new Engine(dir, wal, {
      compactorOptions: undefined,
      strictAtomicity: false,
      compressionAlgo: sstable.COMPRESSION_DEFLATE,
      compressionThreshold: 1,
    } as any);
    try {
      await e.open();

      // write a number of compressible entries
      const repeated = Buffer.alloc(1024, "A");
      for (let i = 0; i < 100; i++) {
        await e.put(Buffer.from("k" + String(i).padStart(3, "0")), repeated);
      }

      // flush memtable to SST (should use engine-level compression opts)
      await e.flush();

      // find the newest SST file from manifest via engine internals (sstReaders)
      const readers = (e as any).sstReaders as any[];
      expect(readers.length).toBeGreaterThan(0);
      const meta = readers[readers.length - 1].meta;
      const file = meta.file as string;

      // sanity checks: file exists
      expect(typeof file).toBe("string");
      expect(file.length).toBeGreaterThan(0);
      expect(existsSync(file)).toBe(true);

      // Assert compressed file size is < uncompressed estimate (rough):
      // We'll create an in-memory uncompressed writer to produce a baseline file for size comparison.
      // Instead of writing another file on disk, we check that file size is less than a naive upper bound
      // computed as (entries * (key+value)) which is a conservative uncompressed size.
      const stats = statSync(file);
      const naiveUncompressed = 100 * (4 + 3 + 4 + repeated.length + 10); // key len(4)+key(3)+vlen(4)+val+rev~10
      // Expect compression to reduce size below naive raw bytes in this test with repeated data
      expect(stats.size).toBeLessThan(naiveUncompressed);

      // verify round-trip via Engine.get
      const got = e.get(Buffer.from("k" + String(10).padStart(3, "0")));
      expect(got).not.toBeNull();
      if (got) expect(got.length).toBe(repeated.length);
    } finally {
      try {
        await e.close();
      } catch {}
    }
  });
});
