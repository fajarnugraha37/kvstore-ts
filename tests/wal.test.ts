import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
import { Wal } from "../libs/wal/wall";

describe("wal basic", () => {
  it("append and scan roundtrip", async () => {
    const dir = "./data-test";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false;

    await wal.append({ k: "a", v: 1 });
    await wal.append({ k: "b", v: 2 });

    if ((wal as any).flush) await (wal as any).flush();

    const out: any[] = [];
    for await (const e of wal.scan()) out.push(e);
    expect(out.length).toBe(2);
    expect(out[0].k).toBe("a");
    expect(out[1].k).toBe("b");

    if ((wal as any).close) await (wal as any).close();
  });

  it("reverseScan yields entries in reverse order", async () => {
    const dir = "./data-test";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const wal = new Wal("log.wal");
    (wal as any).rootDir = dir;
    await wal.open();
    (wal as any).batching = false;

    await wal.append({ k: "x", v: 10 });
    await wal.append({ k: "y", v: 20 });
    await wal.append({ k: "z", v: 30 });

    if ((wal as any).flush) await (wal as any).flush();

    const out: any[] = [];
    for await (const e of (wal as any).reverseScan()) out.push(e);
    expect(out.length).toBe(3);
    expect(out[0].k).toBe("z");
    expect(out[2].k).toBe("x");

    if ((wal as any).close) await (wal as any).close();
  });
});
