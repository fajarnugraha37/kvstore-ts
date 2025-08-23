import { it, expect } from "bun:test";
import fs from "node:fs";
import { Wal } from "../libs/wal/wall";
import { HandoffWal } from "../libs/wal/handoff_wal";
import { dir as join } from "../libs/utils/file";

async function writeEntries(wal: any, rootDir: string, n = 10, payload = 16) {
  wal.file = "log.wal";
  wal.rootDir = rootDir;
  await wal.open();
  for (let i = 0; i < n; i++) {
    await wal.append({ i, payload: "x".repeat(payload) });
  }
  await wal.flush();
  // ensure writer background drains for HandoffWal
  if (typeof wal.close === "function") await wal.close();
}

it("scanBuffered and reverseScanBuffered for Wal produce correct order", async () => {
  const tmp = `tmp/wal-buffered-wal-${Date.now()}`;
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(tmp, { recursive: true });
  const wal = new Wal("log.wal", { batching: false, backgroundFlush: false });
  await writeEntries(wal, tmp, 20, 8);
  const results: any[] = [];
  for await (const v of wal.scanBuffered()) results.push(v);
  expect(results.length).toBe(20);
  for (let i = 0; i < 20; i++) expect(results[i].i).toBe(i);

  const rev: any[] = [];
  for await (const v of wal.reverseScanBuffered()) rev.push(v);
  expect(rev.length).toBe(20);
  for (let i = 0; i < 20; i++) expect(rev[i].i).toBe(19 - i);
});

it("scanBuffered and reverseScanBuffered for HandoffWal produce correct order", async () => {
  const tmp = `tmp/wal-buffered-handoff-${Date.now()}`;
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(tmp, { recursive: true });
  const wal = new HandoffWal("log.wal", {
    batching: true,
    backgroundFlush: true,
  });
  await writeEntries(wal, tmp, 20, 8);

  const results: any[] = [];
  for await (const v of wal.scanBuffered()) results.push(v);
  expect(results.length).toBe(20);
  for (let i = 0; i < 20; i++) expect(results[i].i).toBe(i);

  const rev: any[] = [];
  for await (const v of wal.reverseScanBuffered()) rev.push(v);
  expect(rev.length).toBe(20);
  for (let i = 0; i < 20; i++) expect(rev[i].i).toBe(19 - i);
});
