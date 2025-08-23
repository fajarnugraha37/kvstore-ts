import { Wal, HandoffWal } from "../../libs/wal";
import type { WalLike } from "../../libs/wal";
import fs from "fs";
import path from "path";

const N = Number(process.env.N || process.argv[2] || 100000);
const PAYLOAD_SIZE = Number(process.env.PAYLOAD || 128);

function makeTempDir(prefix = "kv-bench") {
  const dir = path.join(
    process.cwd(),
    "tmp",
    `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
  );
  fs.rmSync(path.join(process.cwd(), "tmp"), { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function writeEntries(wal: WalLike, n: number, payloadSize: number) {
  const payload = Buffer.alloc(payloadSize, "x");
  const start = Date.now();
  for (let i = 0; i < n; i++) {
    // small object to keep messagepack/encode cost realistic
    await wal.append({ i, v: payload });
  }
  await wal.flush();
  const dur = Date.now() - start;
  console.log(
    `append: ${n} entries, payload=${payloadSize} bytes -> ${dur} ms (${Math.round(
      n / (dur / 1000)
    )} e/s)`
  );
}

async function measureScan(wal: WalLike, modeName: string) {
  const start = Date.now();
  let count = 0;
  for await (const _ of wal.scan()) {
    count++;
  }
  const dur = Date.now() - start;
  console.log(
    `${modeName} scan: ${count} entries in ${dur} ms -> ${Math.round(
      count / (dur / 1000)
    )} e/s`
  );
}

async function measureReverseScan(wal: WalLike, modeName: string) {
  const start = Date.now();
  let count = 0;
  for await (const _ of wal.reverseScan()) {
    count++;
  }
  const dur = Date.now() - start;
  console.log(
    `${modeName} reverseScan: ${count} entries in ${dur} ms -> ${Math.round(
      count / (dur / 1000)
    )} e/s`
  );
}

async function runOnce(wal: WalLike, n: number, payloadSize: number) {
  await wal.open();
  await writeEntries(wal, n, payloadSize);

  // baseline: pooling enabled (default)
  await measureScan(wal, "pooling");
  await measureReverseScan(wal, "pooling");

  // now disable pooling by monkeypatching getScratch to always allocate
  // and clear header pool to force fresh header allocs
  (wal as any).getScratch = function (min: number) {
    return Buffer.alloc(min);
  };
  try {
    (wal as any).headerPool = [];
    // avoid headerPool push by temporarily monkeypatching push to no-op
    (wal as any).headerPool.push = function () {
      return 0 as any;
    };
  } catch {}

  // run again
  await measureScan(wal, "no-pool");
  await measureReverseScan(wal, "no-pool");

  await wal.close();
}

(async () => {
  for (const impl of ["wal", "handoff"] as const) {
    console.log(
      `\n\nRunning WAL scan benchmark type ${impl} N=${N} payload=${PAYLOAD_SIZE}`
    );
    const dir = makeTempDir(`wal-bench-${impl}`);
    const wal = new (impl === "wal" ? Wal : HandoffWal)("bench.wal." + impl, {
      rootDir: dir,
    });
    console.log(`WAL path: ${dir}`);
    await runOnce(wal, N, PAYLOAD_SIZE);
  }
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
