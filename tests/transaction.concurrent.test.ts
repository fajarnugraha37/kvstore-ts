import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("transactions concurrent", () => {
  it("handles concurrent transactions without deadlock and unique revs", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      // spawn multiple concurrent transactions that touch overlapping key sets
      const tasks: Promise<any>[] = [];
      for (let i = 0; i < 8; i++) {
        tasks.push(
          (async () => {
            const tx = (e as any).beginTransaction();
            tx.put(Buffer.from("k" + (i % 3)), Buffer.from("v" + i));
            tx.put(Buffer.from("k" + ((i + 1) % 3)), Buffer.from("u" + i));
            const r = await tx.commit();
            return r;
          })()
        );
      }

      const results = await Promise.all(tasks);
      // ensure all committed and returned unique revs across ops
      const allRevs = results.flatMap((r: any) => r.revs || ([] as number[]));
      const uniq = new Set(allRevs);
      expect(uniq.size).toBe(allRevs.length);

      e.close();
    });
  });
});
