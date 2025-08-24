import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
import { Wal } from "../libs/wal/wall";
import fs from "node:fs";

describe("wal recoverTail tail-window", () => {
  it("finds the last valid trailer in a small tail window and allows appends after crash", async () => {
    const dir = "./data-recover-tail";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    // force immediate writes for determinism in the test
    (wal as any).batching = false;

    // write two entries and flush/close
    await wal.append({ k: "pre", v: 1 });
    await wal.append({ k: "pre2", v: 2 });
    if ((wal as any).flush) await (wal as any).flush();
    if ((wal as any).close) await (wal as any).close();

    // corrupt the file by appending a few garbage bytes
    const p = dir + "/log.wal";
    const fd = fs.openSync(p, "a");
    const garbage = Buffer.from([0xde, 0xad, 0xbe, 0xff]);
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
