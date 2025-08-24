#!/usr/bin/env bun
import { KVStore } from "../apps/embedded/kv";
import fs from "fs";
import { join } from "path";

// Heavy benchmark: write 1,000,000 entries, then read 1,000,000 either by get or by prefix scan.
// WARNING: This will create a lot of data on disk and may take significant time.

const TOTAL = 1_000_000; // one million
const BATCH = 1000; // concurrent writes per batch
const dbPath = "./data/bench_kv";
const kv = new KVStore(dbPath);

function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

async function rmDirSafe(p: string) {
  try {
    if (fs.existsSync(p))
      await fs.promises.rm(p, { recursive: true, force: true });
  } catch (e) {}
}

async function writeMillion() {
  console.log(
    `Preparing to write ${TOTAL.toLocaleString()} entries to ${dbPath}`
  );
  await rmDirSafe(dbPath);
  await kv.open();

  const payload = "x".repeat(256); // 256B payload
  const t0 = nowMs();
  let written = 0;
  for (let i = 0; i < TOTAL; i += BATCH) {
    const promises: Promise<any>[] = [];
    const end = Math.min(TOTAL, i + BATCH);
    for (let j = i; j < end; j++) {
      const k = `k_${String(j).padStart(7, "0")}`;
      promises.push(kv.put(k, payload));
    }
    await Promise.all(promises);
    written += promises.length;
    if (written % (BATCH * 10) === 0)
      console.log(
        `written ${written.toLocaleString()} / ${TOTAL.toLocaleString()}`
      );
  }
  const t1 = nowMs();
  console.log(
    `Write complete: ${written} entries in ${((t1 - t0) / 1000).toFixed(2)}s`
  );
  await kv.close();
}

async function readAllByGet() {
  console.log(`Reading ${TOTAL.toLocaleString()} entries by get()`);
  await kv.open();
  const t0 = nowMs();
  let read = 0;
  for (let i = 0; i < TOTAL; i += BATCH) {
    const promises: Promise<any>[] = [];
    const end = Math.min(TOTAL, i + BATCH);
    for (let j = i; j < end; j++) {
      const k = `k_${String(j).padStart(7, "0")}`;
      promises.push(kv.get(k));
    }
    const res = await Promise.all(promises);
    read += res.length;
    if (read % (BATCH * 10) === 0)
      console.log(`read ${read.toLocaleString()} / ${TOTAL.toLocaleString()}`);
  }
  const t1 = nowMs();
  console.log(
    `Read-by-get complete: ${read} entries in ${((t1 - t0) / 1000).toFixed(2)}s`
  );
  await kv.close();
}

async function readAllByPrefix() {
  console.log(`Reading ${TOTAL.toLocaleString()} entries by prefix scan()`);
  await kv.open();
  const t0 = nowMs();
  let c = 0;
  for await (const _ of kv.scanStream({ startWith: "k_" })) {
    c++;
    if (c % (BATCH * 10) === 0)
      console.log(`scanned ${c.toLocaleString()} / ${TOTAL.toLocaleString()}`);
    if (c >= TOTAL) break;
  }
  const t1 = nowMs();
  console.log(
    `Read-by-prefix complete: ${c} entries in ${((t1 - t0) / 1000).toFixed(2)}s`
  );
  await kv.close();
}

async function stats() {
  const mem = process.memoryUsage();
  console.log(
    `Memory rss=${(mem.rss / 1024 / 1024).toFixed(2)} MB heapUsed=${(
      mem.heapUsed /
      1024 /
      1024
    ).toFixed(2)} MB`
  );
}

async function main() {
  console.log("KVStore 1M benchmark starting");
  console.log(
    "Note: this will create ~256MB plus WAL/metadata and may take several minutes depending on your machine."
  );
  await writeMillion();
  await stats();
  await readAllByGet();
  await stats();
  await readAllByPrefix();
  await stats();
  console.log("Benchmark finished.");
}

main().catch((e) => {
  console.error("bench error", e);
  process.exit(1);
});
