import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("wal batching", () => {
  it("supports batching mode via engine options and persists entries", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        // enable batching where supported
        walBatching: true,
        walBatchOptions: { batching: true, maxBatchSize: 64, maxBatchDelayMs: 20 },
      } as any);
      await e.open();

      const N = 200;
      for (let i = 0; i < N; i++) {
        // use Engine.put to exercise normal codepaths which go through WAL
        await e.put(Buffer.from("b" + i), Buffer.from("v" + i));
      }

      // ensure durability by flushing engine (this will flush WAL then snapshot)
      await e.flush();
      await e.close();

      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      // verify a few random keys persisted
      for (const i of [0, 10, 50, 199]) {
        const v = e2.get(Buffer.from("b" + i));
        expect(v && v.toString()).toBe("v" + i);
      }
      await e2.close();
    });
  });
});
