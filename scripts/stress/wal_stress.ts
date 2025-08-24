#!/usr/bin/env bun
import { Wal } from "../../libs/wal/wall";
import { HandoffWal } from "../../libs/wal/handoff_wal";
import { writeFileSync, appendFileSync } from "node:fs";

function usage() {
  console.log(`Usage: bun run scripts/stress/wal_stress.ts [--dir DIR] [--file NAME] [--duration S] [--concurrency N] [--payloadSize B] [--batching true|false] [--metricsOut FILE]

Options:
  --dir DIR         Directory containing WAL (default ./data)
  --file NAME       WAL filename (default log.wal)
  --duration S      Test duration in seconds (default 30)
  --concurrency N   Number of concurrent appenders (default 4)
  --payloadSize B   Size in bytes of random payload (default 256)
  --batching VAL    Enable batching (true|false, default true)
  --metricsOut FILE Path to write metrics JSON (optional)
`);
}

const argv = process.argv.slice(2);
if (argv.includes("--help") || argv.includes("-h")) {
  usage();
  process.exit(0);
}

function getArg(name: string, def?: string) {
  const i = argv.indexOf(name);
  if (i === -1) return def;
  return argv[i + 1];
}

const dir = getArg("--dir", "./data")!;
const file = getArg("--file", "log.wal")!;
const duration = Number(getArg("--duration", "30"));
const concurrency = Number(getArg("--concurrency", "4"));
const payloadSize = Number(getArg("--payloadSize", "256"));
const batchingArg = getArg("--batching", "true")!;
const batching = batchingArg !== "false";
const metricsOut = getArg("--metricsOut");
const mode = getArg("--mode", "promise")!; // promise | handoff

console.log(
  `Starting WAL stress test: dir=${dir} file=${file} duration=${duration}s concurrency=${concurrency} payload=${payloadSize} bytes batching=${batching}`
);

(async () => {
  const wal =
    mode === "handoff"
      ? (new HandoffWal(file, { backgroundFlush: true }) as any)
      : new Wal(file, { batching, backgroundFlush: true });
  // allow overriding the rootDir (Wal keeps it private) by casting
  (wal as any).rootDir = dir;
  await wal.open();

  let stop = false;
  const endTime = Date.now() + duration * 1000;

  // simple random payload generator
  function makePayload(n: number) {
    // generate a compact object to benefit msgpack
    return {
      t: Date.now(),
      r: Math.random().toString(36).slice(2),
      d: Buffer.from(
        Array.from({ length: n }, () => Math.floor(Math.random() * 256))
      ).toString("base64"),
    };
  }

  // per-worker append loop
  const workers: Promise<void>[] = [];
  const counters = new Array(concurrency).fill(0);

  for (let i = 0; i < concurrency; i++) {
    workers.push(
      (async (idx) => {
        while (!stop && Date.now() < endTime) {
          const seq = counters[idx]++;
          await wal.append({
            worker: idx,
            seq,
            payload: makePayload(payloadSize),
          });
        }
      })(i)
    );
  }

  // metrics sampling
  const samples: any[] = [];
  let last = { entriesAppended: 0, bytesAppended: 0 };
  const sampleInterval = 1000;
  const timer = setInterval(() => {
    const cur = wal.metrics;
    const now = Date.now();
    const deltaEntries = cur.entriesAppended - last.entriesAppended;
    const deltaBytes = cur.bytesAppended - last.bytesAppended;
    const sample = {
      ts: now,
      entriesAppended: cur.entriesAppended,
      bytesAppended: cur.bytesAppended,
      deltaEntries,
      deltaBytes,
    };
    samples.push(sample);
    last = {
      entriesAppended: cur.entriesAppended,
      bytesAppended: cur.bytesAppended,
    };
    process.stdout.write(
      `\rentries=${cur.entriesAppended} bytes=${cur.bytesAppended} rps=${deltaEntries} bps=${deltaBytes}`
    );
  }, sampleInterval);

  // stop after duration
  const stopPromise = new Promise<void>((res) => {
    setTimeout(() => {
      stop = true;
      res();
    }, duration * 1000);
  });

  // handle SIGINT gracefully
  process.on("SIGINT", () => {
    console.log("\nInterrupted, stopping...");
    stop = true;
  });

  await stopPromise;
  // wait for workers to drain
  await Promise.all(workers.map((p) => p.catch(() => {})));
  console.log("All workers stopped.");

  clearInterval(timer);
  console.log("Stopping timers...");
  // final flush
  await wal.flush();
  console.log("Final flush done.");
  await wal.close();
  console.log("WAL closed.");

  console.log("\nTest finished. Final metrics:", wal.metrics);

  if (metricsOut) {
    try {
      writeFileSync(
        metricsOut,
        JSON.stringify(
          {
            config: { dir, file, duration, concurrency, payloadSize, batching },
            metrics: samples,
          },
          null,
          2
        )
      );
      console.log("Wrote metrics to", metricsOut);
    } catch (e) {
      console.error("Failed to write metrics:", e);
    }
  } else {
    // print summary CSV-like
    console.log("\nTimestamp,entries,bytes,deltaEntries,deltaBytes");
    for (const s of samples) {
      console.log(
        `${new Date(s.ts).toISOString()},${s.entriesAppended},${
          s.bytesAppended
        },${s.deltaEntries},${s.deltaBytes}`
      );
    }
  }
})();
