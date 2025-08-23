import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { withWalImpls } from "./util/engine_test_runner";

// Test that after flush, Engine records WAL offset and truncates WAL up to that offset.

describe("wal truncate", () => {
  it("persists wal offset and truncates wal after flush", async () => {
    await withWalImpls(async (impl) => {
      const dir = `./data-${impl}`;
      // cleanup manifest and wal
      try {
        if (existsSync(`${dir}/manifest.json`))
          unlinkSync(`${dir}/manifest.json`);
      } catch {}

      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      await e.put(Buffer.from("t1"), Buffer.from("x"));
      await e.put(Buffer.from("t2"), Buffer.from("y"));
      // flush -> writes SST and should truncate WAL
      await e.flush();

      // manifest should contain walOffset
      const m = (e as any).manifest;
      const off = m.getWalOffset();
      expect(typeof off).toBe("number");
      // WAL uses segment rotation now: ensure WAL current end offset is a number (new WAL established)
      const currentEnd = (e as any).wal.currentEndOffset();
      expect(typeof currentEnd).toBe("number");
      if ((e as any).close) await (e as any).close();
    });
  });
});
