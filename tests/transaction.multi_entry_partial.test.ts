import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { Wal, HandoffWal } from "../libs/wal";
import { withWalImpls } from "./util/engine_test_runner";
import fs from "node:fs";
import path from "node:path";
const makeTempDir = require("./util/tmpdir");

describe("transactions multi-entry partial WAL", () => {
  it("preserves earlier full entries and truncates only final partial entry", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      // Step 1: commit a transaction via Engine
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      const tx1 = (e as any).beginTransaction();
      tx1.put(Buffer.from("k1"), Buffer.from("v1"));
      const r1 = await tx1.commit();
      expect(r1.ok).toBe(true);
      await e.close();

      // Step 2: append another complete transaction using WAL implementation directly
      const WalImpl = impl === "handoff" ? HandoffWal : Wal;
      const wal = new WalImpl("log.wal", { batching: false });
      // point WAL at our test dir like Engine does
      (wal as any).rootDir = dir;
      await wal.open();
      await wal.append({
        tx: true,
        ops: [{ key: "k2", value: "v2", rev: 999 }],
      });
      // ensure durable
      await wal.flush?.();

      // collect WAL entries with end offsets so we know precise cut points
      const entries: Array<{ end?: number }> = [];
      for await (const rec of (wal as any).scanWithOffsets?.(0) || []) {
        try {
          if (rec && typeof rec.end === "number")
            entries.push({ end: rec.end });
        } catch {}
      }
      // close WAL now that we captured offsets
      await wal.close();

      // read current WAL buffer and record original size
      const walPath = path.join(dir, "log.wal");
      const origBuf = fs.existsSync(walPath)
        ? fs.readFileSync(walPath)
        : Buffer.alloc(0);
      const origSize = origBuf.length;

      // Step 3: simulate crash by appending partial bytes to WAL file (partial third entry)
      expect(fs.existsSync(walPath)).toBe(true);

      // Basic corruption case: append a few arbitrary bytes to simulate an interrupted write
      fs.appendFileSync(walPath, Buffer.from([0xde, 0xad, 0xbe]));

      // reopen Engine and verify both k1 and k2 are present; partial tail truncated
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      const v1 = e2.get(Buffer.from("k1"));
      const v2 = e2.get(Buffer.from("k2"));
      expect(v1 && v1.toString()).toBe("v1");
      expect(v2 && v2.toString()).toBe("v2");

      // verify WAL file size after recovery is <= original size (truncation removed garbage)
      const afterStat = fs.statSync(walPath);
      expect(afterStat.size).toBeLessThanOrEqual(origSize);

      await e2.close();

      // Deterministic fuzz: truncate the WAL at many offsets and ensure invariants hold
      const seed = 123456789;
      let rnd = seed;
      const lcg = () => {
        rnd = (rnd * 1664525 + 1013904223) >>> 0;
        return rnd / 0xffffffff;
      };

      const ITER = 40;
      for (let i = 0; i < ITER; i++) {
        // compute a random cut position from 0 .. origSize + 16 (allow cutting beyond end to simulate garbage)
        const extra = 16;
        const maxPos = origSize + extra;
        const cut = Math.floor(lcg() * (maxPos + 1));

        // write truncated buffer (if cut > origSize, append extra junk then truncate)
        let bufToWrite: Buffer;
        if (cut <= origSize) bufToWrite = origBuf.slice(0, cut);
        else {
          const junk = Buffer.alloc(cut - origSize, 0xaa);
          bufToWrite = Buffer.concat([origBuf, junk]);
        }
        fs.writeFileSync(walPath, bufToWrite);

        // reopen engine to let it run recovery
        const eF = new Engine(dir, "log.wal", { walImpl: impl });
        await eF.open();

        // Check presence according to recorded end offsets
        const end1 =
          entries[0] && typeof entries[0].end === "number" ? entries[0].end : 0;
        const end2 =
          entries[1] && typeof entries[1].end === "number" ? entries[1].end : 0;

        const got1 = eF.get(Buffer.from("k1"));
        const got2 = eF.get(Buffer.from("k2"));

        if (cut >= end1 && end1 > 0) expect(got1 && got1.toString()).toBe("v1");
        if (cut >= end2 && end2 > 0) expect(got2 && got2.toString()).toBe("v2");

        // close engine so WAL file is released for scanning
        await eF.close();

        // Validate recovered WAL by scanning entries with WAL impl - scan validates checksums
        const checker = new WalImpl("log.wal", { batching: false });
        (checker as any).rootDir = dir;
        await checker.open();
        let scanCount = 0;
        for await (const rec of (checker as any).scan?.(0) || []) {
          scanCount++;
          // basic structural validation
          expect(typeof rec === "object").toBe(true);
          if (rec && rec.tx) {
            expect(Array.isArray(rec.ops)).toBe(true);
          }
        }
        await checker.close();

        // After recovery the WAL file should not exceed the cut we wrote (it may be <= cut)
        const st = fs.statSync(walPath);
        expect(st.size).toBeLessThanOrEqual(cut);
      }
    });
  });
});
