import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("transactions", () => {
  it("commits multi-key transaction atomically", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      const tx = (e as any).beginTransaction();
      tx.put(Buffer.from("a"), Buffer.from("1"));
      tx.put(Buffer.from("b"), Buffer.from("2"));
      const res = await tx.commit();
      expect(res.ok).toBe(true);
      // both keys present
      const va = e.get(Buffer.from("a"));
      const vb = e.get(Buffer.from("b"));
      expect(va && va.toString()).toBe("1");
      expect(vb && vb.toString()).toBe("2");
      e.close();
    });
  });

  it("abort drops pending ops", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      const tx = (e as any).beginTransaction();
      tx.put(Buffer.from("x"), Buffer.from("v"));
      tx.abort();
      // commit after abort should throw
      let threw = false;
      try {
        await tx.commit();
      } catch (e) {
        threw = true;
      }
      expect(threw).toBe(true);
      const vx = e.get(Buffer.from("x"));
      expect(vx).toBeNull();
      e.close();
    });
  });
});
