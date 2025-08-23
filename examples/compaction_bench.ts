import { Engine } from "../libs/storage/engine";
import { Manifest } from "../libs/storage/manifest";
import { SSTWriter } from "../libs/storage";
import { existsSync, rmSync } from "node:fs";

async function run() {
  const dir = "./bench_data";
  try {
    if (existsSync(dir)) rmSync(dir, { recursive: true });
  } catch {}
  const m = new Manifest(dir);
  // create many small SSTs to compact
  for (let f = 0; f < 200; f++) {
    const p = `${dir}/in${f}.sst`;
    const w = new SSTWriter(`${p}.tmp`, p, 256, false);
    for (let i = 0; i < 50; i++) {
      const k = Buffer.from(`key_${f}_${i}`);
      const v = Buffer.from("v".repeat(128));
      w.add(k, v, undefined, 0, Date.now());
    }
    const meta = w.finish();
    m.addFile({
      file: meta.file,
      minKeyHex: meta.minKey.toString("hex"),
      maxKeyHex: meta.maxKey.toString("hex"),
      size: meta.size,
      level: 0,
      walOffset: 0,
    });
  }

  const e = new Engine(dir, "log.wal", {
    compactorOptions: {
      maxSstSize: 64 * 1024,
      bytesPerSecond: 512 * 1024,
      entriesPerYield: 128,
    } as any,
  });
  await e.open();
  console.log("Starting compaction benchmark...");
  const start = Date.now();
  const stats = await e.compactNow();
  const dur = Date.now() - start;
  console.log("Compaction finished:", stats, "durationMs=", dur);
  await e.close();
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
