import { describe, it, expect } from "bun:test";
import { Engine } from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import fs from "node:fs";
import path from "node:path";
const makeTempDir = require("./util/tmpdir");

describe("transactions partial WAL variants", () => {
  it("handles truncated header/payload/trailer gracefully", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      // helper to corrupt file with different modes
      const corrupt = (mode: "header" | "payload" | "trailer") => {
        const walPath = path.join(dir, "log.wal");
        if (!fs.existsSync(walPath)) throw new Error("wal missing");
        if (mode === "header") {
          // append a few bytes that look like an incomplete header
          fs.appendFileSync(
            walPath,
            Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
          );
        } else if (mode === "payload") {
          // append a full header claiming a large payload but do not append payload/trailer
          const hdr = Buffer.alloc(12);
          hdr.writeUInt8(1, 0); // version
          hdr.writeUInt8(0, 1); // type
          hdr.writeUInt16BE(0, 2);
          // set payload length large so header indicates more data than present
          hdr.writeUInt32BE(1024, 4);
          hdr.writeUInt32BE(0xdeadbeef >>> 0, 8);
          fs.appendFileSync(walPath, hdr);
          // append a few bytes of payload only (incomplete)
          fs.appendFileSync(walPath, Buffer.from([0xaa, 0xbb, 0xcc, 0xdd]));
        } else if (mode === "trailer") {
          // append a full valid-looking entry (header + payload) then an incomplete trailer
          const payload = Buffer.from(JSON.stringify({ key: "x", value: "y" }));
          const hdr = Buffer.alloc(12);
          hdr.writeUInt8(1, 0);
          hdr.writeUInt8(0, 1);
          hdr.writeUInt16BE(0, 2);
          hdr.writeUInt32BE(payload.length, 4);
          // write a placeholder checksum (not important for this corruption test)
          hdr.writeUInt32BE(0, 8);
          const trailer = Buffer.alloc(12);
          trailer.writeUInt8(1, 0);
          trailer.writeUInt8(0, 1);
          trailer.writeUInt16BE(0, 2);
          trailer.writeUInt32BE(payload.length, 4);
          trailer.writeUInt32BE(0, 8);
          // append header + payload + partial trailer
          fs.appendFileSync(walPath, hdr);
          fs.appendFileSync(walPath, payload);
          // write only part of the trailer (simulate crash while writing trailer)
          fs.appendFileSync(walPath, trailer.slice(0, 6));
        }
      };

      for (const mode of ["header", "payload", "trailer"] as const) {
        // fresh environment per subcase
        const e = new Engine(dir, "log.wal", { walImpl: impl });
        await e.open();

        const tx = (e as any).beginTransaction();
        tx.put(Buffer.from("k-good"), Buffer.from("good"));
        const r = await tx.commit();
        expect(r.ok).toBe(true);

        await e.close();

        // corrupt according to mode
        corrupt(mode);

        // reopen and ensure the good key is still present (recoverTail should trim corruption)
        const e2 = new Engine(dir, "log.wal", { walImpl: impl });
        await e2.open();
        const v = e2.get(Buffer.from("k-good"));
        expect(v && v.toString()).toBe("good");
        await e2.close();

        // cleanup the dir for next iteration by removing WAL and recreating a fresh manifest/wal
        // remove log.wal so next iteration starts clean; other files (manifest/segments) can remain
        try {
          const wp = path.join(dir, "log.wal");
          if (fs.existsSync(wp)) fs.unlinkSync(wp);
        } catch {}
      }
    });
  });
});
