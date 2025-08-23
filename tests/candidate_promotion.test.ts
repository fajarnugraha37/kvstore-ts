import { describe, it, expect } from "bun:test";
const makeTempDir = require("./util/tmpdir");
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";

// This test verifies that when multiple SSTs contain different revisions for the
// same key and a tombstone becomes eligible for TTL-based removal, the compactor
// promotes the highest-revision non-tombstone candidate rather than dropping the key.

describe("Compactor candidate promotion", () => {
  it("promotes highest-rev candidate when tombstone expires", async () => {
    const dir = makeTempDir();
    // deterministically control time so TTL behaviors are reproducible
    let now = Date.now();
    const tp = () => now;
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { tombstoneRetentionMs: 1 },
        timeProvider: tp,
      });
      await e.open();

      const k = Buffer.from("cand");
      // produce multiple SSTs each containing a different revision
      await e.put(k, Buffer.from("v1"));
      await e.flush();
      now += 1; // advance time slightly
      await e.put(k, Buffer.from("v2"));
      await e.flush();
      now += 1;
      // tombstone (rev3)
      await e.del(k);
      await e.flush();

      // advance time beyond retention so TTL will consider the tombstone expired
      now += 1000;
      // run compaction which uses timeProvider; since tombstone TTL expired,
      // compactor should promote v2 as the latest (highest-rev) candidate
      await e.compactNow();
      const latest = e.get(k);
      expect(latest && latest.toString()).toBe("v2");
      await e.close();
    });
  });
});
