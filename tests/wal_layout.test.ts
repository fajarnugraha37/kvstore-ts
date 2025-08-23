import { rm, mkdir } from "node:fs/promises";
import fs from "node:fs";
import { describe, it, expect } from "bun:test";
import { Wal } from "../libs/wal/wall";

describe("wal layout and reverseScan edge cases", () => {
  it("handles partial tail (truncated trailer)", async () => {
    const dir = "./data-layout";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false;

    await wal.append({ k: "a", v: 1 });
    await wal.append({ k: "b", v: 2 });
    if ((wal as any).flush) await (wal as any).flush();

    // truncate the file to remove last few bytes (partial trailer)
    const p = dir + "/log.wal";
    const st = fs.statSync(p);
    fs.truncateSync(p, st.size - 4);

    // scan should stop before corrupted tail
    const out: any[] = [];
    for await (const e of wal.scan()) out.push(e);
    expect(out.length).toBe(1);

    if ((wal as any).close) await (wal as any).close();
  });

  it("reverseScan streaming works with large files", async () => {
    const dir = "./data-layout";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false;

    for (let i = 0; i < 100; i++) await wal.append({ i, v: "x".repeat(100) });
    if ((wal as any).flush) await (wal as any).flush();

    let count = 0;
    for await (const _ of (wal as any).reverseScan()) count++;
    expect(count).toBe(100);

    if ((wal as any).close) await (wal as any).close();
  });
});
