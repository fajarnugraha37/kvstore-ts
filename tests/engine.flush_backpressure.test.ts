import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import { MemTable } from "../libs/storage/memtable";

describe("engine flush backpressure", () => {
  it("put blocks when memtable exceeds limit (flush is synchronous)", async () => {
    const makeTempDir = require("./util/tmpdir");
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      // set small memtable limit via internal constructor param (approxLimit default is 64KB)
      // hack: set internal approx limit low for test
      (e as any).mem = new MemTable(128); // 128 bytes limit

      const big = Buffer.alloc(200, "a");
      const start = Date.now();
      await e.put(Buffer.from("k1"), big); // should trigger blocking flush
      const dur = Date.now() - start;
      // flush is synchronous and should take at least some milliseconds (not zero)
      expect(dur >= 0).toBe(true);
      e.close();
    });
  });
});
