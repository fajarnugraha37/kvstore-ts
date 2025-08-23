import { it, expect } from "bun:test";
import fs from "node:fs";
import { HandoffWal } from "../libs/wal/handoff_wal";
import { dir as join } from "../libs/utils/file";

async function writeEntries(wal: any, rootDir: string, n = 100, payload = 64) {
  wal.file = "log.wal";
  wal.rootDir = rootDir;
  await wal.open();
  for (let i = 0; i < n; i++) {
    await wal.append({ i, payload: "x".repeat(payload) });
  }
  // For HandoffWal append is asynchronous; flush to ensure durability
  await wal.flush();
  // close writer so on-disk files are consistent for scans
  if (typeof wal.close === "function") await wal.close();
}

it("HandoffWal buffered scans with backgroundFlush=true (eager writer)", async () => {
  const tmp = `tmp/handoff-focused-bgtrue-${Date.now()}`;
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(tmp, { recursive: true });

  const wal = new HandoffWal("log.wal", {
    batching: true,
    backgroundFlush: true,
  });
  await writeEntries(wal, tmp, 200, 16);

  const results: any[] = [];
  for await (const v of wal.scanBuffered()) results.push(v);
  expect(results.length).toBe(200);
  for (let i = 0; i < 200; i++) expect(results[i].i).toBe(i);

  const rev: any[] = [];
  for await (const v of wal.reverseScanBuffered()) rev.push(v);
  expect(rev.length).toBe(200);
  for (let i = 0; i < 200; i++) expect(rev[i].i).toBe(199 - i);
});

it("HandoffWal buffered scans with backgroundFlush=false (lazy writer start)", async () => {
  const tmp = `tmp/handoff-focused-bgfalse-${Date.now()}`;
  try {
    fs.rmSync(tmp, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(tmp, { recursive: true });

  const wal = new HandoffWal("log.wal", {
    batching: true,
    backgroundFlush: false,
  });
  // writer should start lazily on first append; ensure path exercised
  await writeEntries(wal, tmp, 200, 16);

  const results: any[] = [];
  for await (const v of wal.scanBuffered()) results.push(v);
  expect(results.length).toBe(200);
  for (let i = 0; i < 200; i++) expect(results[i].i).toBe(i);
});
