import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import fs, { unlinkSync, existsSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";

// This test simulates a partial/truncated SST and verifies engine recovers via WAL replay.

describe("recovery tests", () => {
  it("skips truncated sst and replays wal", async () => {
    // Use a unique temp directory for this test to avoid cross-test interference.
    const dir = path.join(
      os.tmpdir(),
      `kvstore-test-${Date.now()}-${Math.random().toString(36).slice(2)}`
    );
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch (e) {}
    fs.mkdirSync(dir, { recursive: true });
    // cleanup any previous test artifacts
    try {
      if (existsSync(`${dir}/manifest.json`))
        unlinkSync(`${dir}/manifest.json`);
    } catch {}

    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();

      // put some entries and flush to create an SST
      await e.put(Buffer.from("k1"), Buffer.from("v1"));
      await e.put(Buffer.from("k2"), Buffer.from("v2"));
      await e.flush();

      // locate the SST file created in manifest
      const m = (e as any).manifest.listFiles();
      expect(m.length).toBeGreaterThan(0);
      const sstFile = m[0].file as string;
      expect(sstFile).toBeDefined();

      // truncate the SST file to simulate a crash during write
      const st = statSync(sstFile);
      // keep only first half
      const truncateTo = Math.max(16, Math.floor(st.size / 2));
      const fd = fs.openSync(sstFile, "r+");
      fs.ftruncateSync(fd, truncateTo);
      fs.closeSync(fd);

      // now reopen engine; it should remove the broken sst from manifest and replay WAL to restore data
      if (process.env.KV_DUMP_PRE_OPEN === "1") {
        try {
          console.log("--- DEBUG: data dir listing before e2.open ---");
          const files = fs.readdirSync(dir);
          for (const f of files) {
            try {
              const st = fs.statSync(path.join(dir, f));
              console.log(f, st.size, st.isFile() ? "file" : "dir");
              if (f === "manifest.json") {
                try {
                  console.log(
                    "manifest:",
                    fs.readFileSync(path.join(dir, f), "utf8")
                  );
                } catch (e) {}
              }
            } catch (e) {}
          }
          console.log("--- END DEBUG ---");
        } catch (e) {}
      }
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();

      // k1 and k2 should be available via WAL replay
      const v1 = e2.get(Buffer.from("k1"));
      const v2 = e2.get(Buffer.from("k2"));
      expect(v1?.toString()).toBe("v1");
      expect(v2?.toString()).toBe("v2");
      // close engines if present
      if ((e as any).close) await (e as any).close();
      if ((e2 as any).close) await (e2 as any).close();
    });
  });
});
