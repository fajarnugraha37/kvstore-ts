#!/usr/bin/env bun
import { Wal } from "../../libs/wal/wall";
import { open } from "node:fs/promises";
import { writeFileSync } from "node:fs";

function usage() {
  console.log(`Usage: bun run scripts/cli/wal_inspect.ts <command> [--dir DIR] [--file NAME] [--limit N] [--out FILE]

Commands:
  dump        Dump all entries (forward)
  revdump     Dump entries in reverse order
  validate    Scan and report any corrupt/partial entries
  hexdump     Print a hex dump of the WAL file

Options:
  --dir DIR   Directory containing WAL (default ./data)
  --file NAME WAL filename (default log.wal)
  --limit N   Limit output to N entries (optional)
  --out FILE  Write JSON NDUMP to FILE instead of stdout
`);
}

const argv = process.argv.slice(2);
if (argv.length === 0 || argv.includes("--help") || argv.includes("-h")) {
  usage();
  process.exit(0);
}

const cmd = argv[0];
function getArg(name: string, def?: string) {
  const i = argv.indexOf(name);
  if (i === -1) return def;
  return argv[i + 1];
}

const dir = getArg("--dir", "./data")!;
const file = getArg("--file", "log.wal")!;
const limit = getArg("--limit") ? Number(getArg("--limit")) : undefined;
const out = getArg("--out");

async function doDump(reverse = false) {
  const wal = new Wal(file, {});
  (wal as any).rootDir = dir;
  await wal.open();

  const stream = reverse ? wal.reverseScan() : wal.scan();
  const results: any[] = [];
  let i = 0;
  for await (const v of stream) {
    results.push(v);
    i++;
    if (limit && i >= limit) break;
  }

  await wal.close();
  return results;
}

async function doValidate() {
  // Use Wal.scan() to validate readable entries; scan will stop at first corrupt entry.
  const wal = new Wal(file, {});
  (wal as any).rootDir = dir;
  await wal.open();
  let count = 0;
  try {
    for await (const _ of wal.scan()) {
      count++;
    }
    console.log("validated readable entries:", count);
  } catch (e) {
    console.error("validation stopped due to error:", e);
  } finally {
    await wal.close();
  }
}

async function doHexdump() {
  const fh = await open(dir + "/" + file, "r");
  try {
    const stat = await fh.stat();
    const size = stat.size;
    const buf = Buffer.alloc(size);
    const r = await fh.read(buf, 0, size, 0);
    if (r.bytesRead > 0) {
      console.log(
        buf
          .slice(0, r.bytesRead)
          .toString("hex")
          .match(/.{1,32}/g)
          ?.join("\n")
      );
    }
  } finally {
    await fh.close();
  }
}

(async () => {
  try {
    if (cmd === "dump") {
      const res = await doDump(false);
      if (out) writeFileSync(out, JSON.stringify(res, null, 2));
      else console.log(JSON.stringify(res, null, 2));
    } else if (cmd === "revdump") {
      const res = await doDump(true);
      if (out) writeFileSync(out, JSON.stringify(res, null, 2));
      else console.log(JSON.stringify(res, null, 2));
    } else if (cmd === "validate") {
      await doValidate();
    } else if (cmd === "hexdump") {
      await doHexdump();
    } else {
      usage();
      process.exit(1);
    }
  } catch (e) {
    console.error("error:", e);
    process.exit(2);
  }
})();
