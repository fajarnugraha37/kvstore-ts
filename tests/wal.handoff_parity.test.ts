import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
import { HandoffWal } from "../libs/wal/handoff_wal";
import fs from "node:fs";

describe("HandoffWal parity tests", () => {
  it("basic append and scan roundtrip", async () => {
    const dir = "./data-handoff-basic";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const w = new HandoffWal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();
    await w.append({ a: 1 });
    await w.append({ b: 2 });
    await w.flush();

    const out: any[] = [];
    for await (const e of w.scan()) out.push(e);
    expect(out.length).toBe(2);
    if ((w as any).close) await (w as any).close();
  });

  it("reverseScan yields entries in reverse order", async () => {
    const dir = "./data-handoff-rev";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const w = new HandoffWal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();
    await w.append({ k: "a" });
    await w.append({ k: "b" });
    await w.append({ k: "c" });
    await w.flush();

    const out: any[] = [];
    for await (const e of w.reverseScan()) out.push(e);
    expect(out.length).toBeGreaterThanOrEqual(3);
    // first yielded should be the latest
    expect(out[0].k).toBe("c");
    if ((w as any).close) await (w as any).close();
  });

  it("concurrent appends and drain", async () => {
    const dir = "./data-handoff-concurrency";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const w = new HandoffWal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();
    const N = 200;
    const tasks: Promise<void>[] = [];
    for (let i = 0; i < N; i++) tasks.push(w.append({ i }));
    await Promise.all(tasks);
    await w.flush();

    const out: any[] = [];
    for await (const e of w.scan()) out.push(e);
    expect(out.length).toBe(N);
    if ((w as any).close) await (w as any).close();
  });

  it("truncateUpTo rotates and persists segments/meta", async () => {
    const dir = "./data-handoff-truncate";
    await rm(dir, { recursive: true, force: true });
    await mkdir(dir, { recursive: true });

    const w = new HandoffWal("log.wal", { batching: false });
    (w as any).rootDir = dir;
    await w.open();
    await w.append({ x: 1 });
    await w.append({ x: 2 });
    await w.flush();
    const before = fs.statSync(dir + "/log.wal").size;
    // rotate at currentEndOffset()
    const end = (w as any).currentEndOffset ? (w as any).currentEndOffset() : 0;
    await w.truncateUpTo(end);
    expect(fs.existsSync(dir + "/log.wal")).toBe(true);
    expect(
      fs.existsSync(dir + "/log.wal.segments.json") ||
        fs.existsSync(dir + "/log.wal.segments.json")
    ).toBe(true);
    if ((w as any).close) await (w as any).close();
  });
});
