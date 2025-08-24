import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("transactions crash recovery", () => {
  it("committed transaction survives restart and WAL replay", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      const tx = (e as any).beginTransaction();
      tx.put(Buffer.from("crashA"), Buffer.from("VA"));
      tx.put(Buffer.from("crashB"), Buffer.from("VB"));
      const res = await tx.commit();
      expect(res.ok).toBe(true);
      const revs = res.revs || [];
      expect(revs.length).toBe(2);
      const ra = revs[0];
      const rb = revs[1];

      // values visible before close
      const va = e.get(Buffer.from("crashA"));
      const vb = e.get(Buffer.from("crashB"));
      expect(va && va.toString()).toBe("VA");
      expect(vb && vb.toString()).toBe("VB");

      await e.close();

      // reopen and verify WAL replay restored committed transaction
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      const vA = e2.get(Buffer.from("crashA"));
      const vB = e2.get(Buffer.from("crashB"));
      expect(vA && vA.toString()).toBe("VA");
      expect(vB && vB.toString()).toBe("VB");

      // verify point-in-time reads at the committed revs
      const vaAt = await e2.getAtRevision(Buffer.from("crashA"), ra);
      const vbAt = await e2.getAtRevision(Buffer.from("crashB"), rb);
      expect(vaAt && vaAt.toString()).toBe("VA");
      expect(vbAt && vbAt.toString()).toBe("VB");

      await e2.close();
    });
  });
});
