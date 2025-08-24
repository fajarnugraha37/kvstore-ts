#!/usr/bin/env bun
// Simple microbenchmark comparing WAL batching vs non-batching append throughput.
import fs from "fs";
import os from "os";
import path from "path";
import { Wal, HandoffWal } from "../libs/wal";

function makeTempDir(prefix = "kv-bench-") {
  const dir = path.join(os.tmpdir(), `${prefix}${Date.now()}`);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function runMode(name: string, walCtor: any, opts: any, entries = 10000) {
  const dir = makeTempDir(`walbench-${name}-`);
  const wal = new walCtor("log.wal", opts || {});
  (wal as any).rootDir = dir;
  await wal.open();

  const start = Date.now();
  for (let i = 0; i < entries; i++) {
    await wal.append({ key: `k${i}`, value: `v${i}` });
  }
  // flush to ensure durability
  if (typeof wal.flush === "function") await wal.flush();
  const dur = Date.now() - start;
  console.log(
    `${name}: appended ${entries} entries in ${dur}ms (${(
      entries /
      (dur / 1000)
    ).toFixed(0)} ops/s)`
  );
  await wal.close();
}

async function main() {
  const N = 5000;
  console.log("WAL batching microbenchmark");
  await runMode("wal-sync", Wal, { batching: false }, N);
  await runMode(
    "handoff-batch",
    HandoffWal,
    {
      batching: true,
      batchMaxEntries: 128,
      batchMaxBytes: 128 * 1024,
      batchIntervalMs: 10,
    },
    N
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
