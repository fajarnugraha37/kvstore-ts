import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
import { HandoffWal } from "../libs/wal/handoff_wal";
import fs from "node:fs";

describe("HandoffWal parity tests - segments + reverseScan", () => {
  it("reverseScan across rotated segments and active WAL yields correct reverse order", async () => {
    const dir = "./data-handoff-segs-rev";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const w = new HandoffWal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();

    // append a few entries that will be rotated into a segment
    await w.append({ k: "s1a" });
    await w.append({ k: "s1b" });
    await w.append({ k: "s1c" });
    await w.flush();

    // determine current end offset and rotate
    const segEnd = fs.statSync(dir + "/log.wal").size;
    await w.truncateUpTo(segEnd);

    // append entries to active WAL after rotation
    await w.append({ k: "a1" });
    await w.append({ k: "a2" });
    await w.append({ k: "a3" });
    await w.flush();

    const out: any[] = [];
    for await (const e of w.reverseScan()) out.push(e);

    // Expect active entries first in reverse chronological order, then segment entries
    const keys = out
      .map((x) => x && x.k)
      .filter((x) => typeof x !== "undefined");
    // locate the sequence in keys; allow there to be extra entries (from prior tests) but ensure ordering exists
    const expected = ["a3", "a2", "a1", "s1c", "s1b", "s1a"];
    // check that expected appears as a contiguous subsequence starting at index 0
    for (let i = 0; i < expected.length; i++) {
      expect(keys[i]).toBe(expected[i]);
    }

    if ((w as any).close) await (w as any).close();
  });
});
