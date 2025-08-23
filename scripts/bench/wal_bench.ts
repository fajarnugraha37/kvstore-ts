import { rm, mkdir } from "node:fs/promises";
import { serialize } from "bun:jsc";
import { Wal } from "../../libs/wal/wall";

const ITER = Number(process.env.ITER) || Number(process.argv[2]) || 1000;
const PAYLOAD = Number(process.env.PAYLOAD) || Number(process.argv[3]) || 256;

function hrSec(start: bigint, end: bigint) {
  const ns = Number(end - start);
  return ns / 1e9;
}

async function bench() {
  const dir = "./data";
  // cleanup previous data
  await rm(dir, { recursive: true, force: true });
  await mkdir(dir, { recursive: true });

  const wal = new Wal("log.wal");
  await wal.open();
  // Enable batching for this benchmark run
  (wal as any).batching = true;
  (wal as any).maxBatchSize = 32;

  const payloadStr = "x".repeat(PAYLOAD);
  const sample = { i: 0, payload: payloadStr };
  const sampleBuf = Buffer.from(serialize(sample));
  const sampleLen = sampleBuf.length;

  console.log(
    `WAL benchmark: iterations=${ITER}, payload=${PAYLOAD} bytes (est serialized ${sampleLen} bytes)`
  );

  // Append benchmark
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < ITER; i++) {
    await wal.append({
      key: `key${i}`,
      v: `value-${payloadStr}`,
    });
  }
  const t1 = process.hrtime.bigint();
  const appendSec = hrSec(t0, t1);
  const opsPerSec = ITER / appendSec;
  const mbPerSec = (sampleLen * ITER) / (1024 * 1024) / appendSec;

  console.log(
    "append: total=",
    appendSec.toFixed(3),
    "s",
    "ops/s=",
    opsPerSec.toFixed(2),
    "MB/s=",
    mbPerSec.toFixed(2)
  );

  // Scan benchmark
  // Ensure any batched writes are flushed before scanning
  if (typeof (wal as any).flush === "function") await (wal as any).flush();
  const t2 = process.hrtime.bigint();
  let count = 0;
  for await (const _ of wal.scan()) count++;
  const t3 = process.hrtime.bigint();
  const scanSec = hrSec(t2, t3);
  console.log(
    "scan: items=",
    count,
    "time_s=",
    scanSec.toFixed(3),
    "ops/s=",
    (count / scanSec).toFixed(2)
  );

  console.log("--- summary ---");
  console.log(
    `append_sec=${appendSec.toFixed(3)} scan_sec=${scanSec.toFixed(3)}`
  );

  if (typeof (wal as any).close === "function") await (wal as any).close();

  process.exit(0);
}

bench().catch((err) => {
  console.error(err);
  process.exit(1);
});
