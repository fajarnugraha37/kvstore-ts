import { rm, mkdir } from "node:fs/promises";
import { describe, it, expect } from "bun:test";
const makeTempDir = require("./util/tmpdir");
import fs from "node:fs";
import { Wal } from "../libs/wal/wall";
import { HandoffWal } from "../libs/wal/handoff_wal";

async function collectScan(w: any) {
  const out: any[] = [];
  for await (const e of w.scan()) out.push(e);
  return out;
}

async function collectReverse(w: any) {
  const out: any[] = [];
  for await (const e of w.reverseScan()) out.push(e);
  return out;
}

async function collectWithOffsets(w: any) {
  const out: any[] = [];
  for await (const e of w.scanWithOffsets()) out.push(e);
  return out;
}

describe("Wal vs HandoffWal parity", () => {
  it("behaves identically for basic ops", async () => {
    const tmp = makeTempDir();
    const d1 = `${tmp}/wal_a`;
    const d2 = `${tmp}/wal_b`;
    await rm(d1, { recursive: true, force: true });
    await rm(d2, { recursive: true, force: true });
    await mkdir(d1, { recursive: true });
    await mkdir(d2, { recursive: true });

    const wA = new Wal("log.wal", { rootDir: d1, batching: false });
    const wB = new HandoffWal("log.wal", { rootDir: d2, batching: false });
    await wA.open();
    await wB.open();

    // perform same sequence
    await wA.append({ key: "x", value: "1" });
    await wA.append({ key: "y", value: "2" });
    await wA.append({ key: "z", value: "3" });
    await wA.flush();

    await wB.append({ key: "x", value: "1" });
    await wB.append({ key: "y", value: "2" });
    await wB.append({ key: "z", value: "3" });
    await wB.flush();

    const scanA = await collectScan(wA);
    const scanB = await collectScan(wB);
    expect(scanA.length).toBeGreaterThanOrEqual(3);
    expect(scanB.length).toBeGreaterThanOrEqual(3);
    // compare serialized JSON of entries to avoid subtle Buffer differences
    expect(JSON.stringify(scanA)).toBe(JSON.stringify(scanB));

    const revA = await collectReverse(wA);
    const revB = await collectReverse(wB);
    expect(revA.length).toBeGreaterThanOrEqual(3);
    expect(revB.length).toBeGreaterThanOrEqual(3);
    expect(JSON.stringify(revA)).toBe(JSON.stringify(revB));

    const offA = await collectWithOffsets(wA);
    const offB = await collectWithOffsets(wB);
    // ensure offsets counts match and values keys match order
    expect(offA.length).toBe(offB.length);
    expect(JSON.stringify(offA.map((x) => x.value))).toBe(
      JSON.stringify(offB.map((x) => x.value))
    );

    // currentEndOffset parity
    const eA = typeof wA.currentEndOffset === "function" ? wA.currentEndOffset() : null;
    const eB = typeof wB.currentEndOffset === "function" ? wB.currentEndOffset() : null;
    expect(typeof eA).toBe(typeof eB);

    try { if (wA.close) await wA.close(); } catch {}
    try { if (wB.close) await wB.close(); } catch {}
  });
});
