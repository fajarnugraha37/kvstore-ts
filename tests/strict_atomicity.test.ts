import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("strict atomicity", () => {
  it("ensures durability when strictAtomicity is set and flush() is called", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        strictAtomicity: true,
      });
      await e.open();

      // perform a put which should be durably persisted when we call flush
      await e.put(Buffer.from("sa-key"), Buffer.from("sa-val"));

      // flush and close
      await e.flush();
      await e.close();

      // reopen engine and verify key is present
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      const v = e2.get(Buffer.from("sa-key"));
      expect(v && v.toString()).toBe("sa-val");
      await e2.close();
    });
  });
});
