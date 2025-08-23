import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
import { Wal } from "../libs/wal/wall";

const fs = require("node:fs");

describe("wal concurrency and crash recovery", () => {
  it("handles concurrent appends correctly", async () => {
    const dir = "./data-concurrent";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false; // force immediate writes for this test

    const N = 200;
    const tasks: Promise<void>[] = [];
    for (let i = 0; i < N; i++) {
      tasks.push(wal.append({ idx: i, when: Date.now() }));
    }
    await Promise.all(tasks);

    if ((wal as any).flush) await (wal as any).flush();

    const out: any[] = [];
    for await (const e of wal.scan()) out.push(e);
    expect(out.length).toBe(N);

    if ((wal as any).close) await (wal as any).close();
  });

  it("recovers from partial tail and allows subsequent appends", async () => {
    const dir = "./data-crash";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false;

    // write two good entries
    await wal.append({ k: "pre", v: 1 });
    await wal.append({ k: "pre2", v: 2 });
    if ((wal as any).flush) await (wal as any).flush();
    if ((wal as any).close) await (wal as any).close();

    // Corrupt the file by appending partial bytes (simulate crash)
    const p = dir + "/log.wal";
    const fd = fs.openSync(p, "a");
    // write some garbage bytes (not a full header/trailer/payload)
    const garbage = Buffer.from([0xde, 0xad, 0xbe]);
    fs.writeSync(fd, garbage);
    fs.closeSync(fd);

    // Re-open wal and ensure scan returns only the valid entries
    const wal2 = new Wal("log.wal");
    (wal2 as any).rootDir = dir;
    await wal2.open();
    const out1: any[] = [];
    for await (const e of wal2.scan()) out1.push(e);
    expect(out1.length).toBe(2);

    // Now append another entry after crash; should succeed
    await wal2.append({ k: "post", v: 3 });
    if ((wal2 as any).flush) await (wal2 as any).flush();

    const out2: any[] = [];
    for await (const e of wal2.scan()) out2.push(e);
    expect(out2.length).toBe(3);
    expect(out2[2].k).toBe("post");

    if ((wal2 as any).close) await (wal2 as any).close();
  });
});
