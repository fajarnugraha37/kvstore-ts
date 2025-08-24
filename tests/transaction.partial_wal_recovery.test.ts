import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import fs from "node:fs";
import path from "node:path";
const makeTempDir = require("./util/tmpdir");

describe("transactions partial WAL recovery", () => {
  it("truncates a partially-written WAL tail and preserves previous commits", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      // commit a first transaction that should survive recovery
      const tx1 = (e as any).beginTransaction();
      tx1.put(Buffer.from("k-good"), Buffer.from("good"));
      const r1 = await tx1.commit();
      expect(r1.ok).toBe(true);

      // close engine to release WAL file descriptor so we can corrupt the file
      await e.close();

      // Append a few garbage bytes to simulate a partial/crashed write
      const walPath = path.join(dir, "log.wal");
      // Ensure file exists
      expect(fs.existsSync(walPath)).toBe(true);
      // Append some random/incomplete bytes (this simulates a partial entry)
      fs.appendFileSync(walPath, Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05]));

      // Reopen engine - WAL.open() should run recoverTail and truncate the trailing garbage
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();

      // The committed key should be present after recovery
      const v = e2.get(Buffer.from("k-good"));
      expect(v && v.toString()).toBe("good");

      // There's no partially-written entry to account for; ensure no other keys present
      const missing = e2.get(Buffer.from("k-bad"));
      expect(missing).toBeNull();

      await e2.close();
    });
  });
});
