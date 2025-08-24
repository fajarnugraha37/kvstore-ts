import { HandoffWal } from "../../libs/wal/handoff_wal";
import fs from "fs";
import path from "path";

const N = Number(process.env.N || process.argv[2] || 1000000);
const PAYLOAD_SIZE = Number(process.env.PAYLOAD || 256);
const BG = process.env.BG === "false" ? false : true;

function makeTempDir(prefix = "handoff-bench") {
  const dir = path.join(
    process.cwd(),
    "tmp",
    `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
  );
  try {
    fs.rmSync(path.join(process.cwd(), "tmp"), {
      recursive: true,
      force: true,
    });
  } catch {}
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function writeEntries(wal: any, n: number, payloadSize: number) {
  const payload = "x".repeat(payloadSize);
  const start = Date.now();
  for (let i = 0; i < n; i++) {
    await wal.append({ i, v: payload });
    if (i > 0 && i % 100000 === 0) console.log(`written ${i}`);
  }
  await wal.flush();
  const dur = Date.now() - start;
  console.log(
    `append: ${n} entries, payload=${payloadSize} bytes -> ${dur} ms (${Math.round(
      n / (dur / 1000)
    )} e/s)`
  );
}

async function measureBufferedScans(wal: any) {
  if (typeof wal.scanBuffered === "function") {
    const start = Date.now();
    let c = 0;
    for await (const _ of wal.scanBuffered()) c++;
    const dur = Date.now() - start;
    console.log(
      `scanBuffered: ${c} entries in ${dur} ms -> ${Math.round(
        c / (dur / 1000)
      )} e/s`
    );
  }
  if (typeof wal.reverseScanBuffered === "function") {
    const start = Date.now();
    let c = 0;
    for await (const _ of wal.reverseScanBuffered()) c++;
    const dur = Date.now() - start;
    console.log(
      `reverseScanBuffered: ${c} entries in ${dur} ms -> ${Math.round(
        c / (dur / 1000)
      )} e/s`
    );
  }
}

(async () => {
  console.log(
    `HandoffWal buffered large bench N=${N} payload=${PAYLOAD_SIZE} backgroundFlush=${BG}`
  );
  const dir = makeTempDir(`handoff-bench-${BG ? "bg" : "nobg"}`);
  const wal = new HandoffWal("bench.wal.handoff", {
    rootDir: dir,
    backgroundFlush: BG,
    batching: true,
  });
  await wal.open();
  await writeEntries(wal, N, PAYLOAD_SIZE);
  // ensure writer drained for background writer
  await wal.flush();
  // measure buffered scans
  await measureBufferedScans(wal);
  await wal.close();
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
