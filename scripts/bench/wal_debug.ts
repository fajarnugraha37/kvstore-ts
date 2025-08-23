import { rm, mkdir, readFile, open } from "node:fs/promises";
import { Wal } from "../../libs/wal/wall";
import { encode as msgpackEncode } from "@msgpack/msgpack";

async function hexdump(buf: Buffer, len = 128) {
  const n = Math.min(len, buf.length);
  let out = "";
  for (let i = 0; i < n; i++) {
    out += buf[i]!.toString(16).padStart(2, "0") + " ";
  }
  return out.trim();
}

async function debug() {
  const dir = "./data";
  await rm(dir, { recursive: true, force: true });
  await mkdir(dir, { recursive: true });

  const wal = new Wal("log.wal");
  await wal.open();
  (wal as any).batching = true;
  (wal as any).maxBatchSize = 10;

  console.log("Appending 3 entries...");
  const a = { i: 1, v: "one" };
  const b = { i: 2, v: "two" };
  const c = { i: 3, v: "three" };
  console.log(
    "encoded lens:",
    msgpackEncode(a).length,
    msgpackEncode(b).length,
    msgpackEncode(c).length
  );
  await wal.append(a);
  await wal.append(b);
  await wal.append(c);

  console.log("Flushing batches...");
  if (typeof (wal as any).flushBatch === "function")
    await (wal as any).flushBatch();

  // Inspect file
  const fh = await open("./data/log.wal", "r");
  const st = await fh.stat();
  console.log("file size:", st.size);

  const peek = Buffer.alloc(Math.min(256, st.size));
  const r = await fh.read(peek, 0, peek.length, 0);
  console.log("bytesRead:", r.bytesRead);
  console.log("hexdump:", await hexdump(peek, 128));

  // Parse entries using header format
  console.log("Parsing entries from file:");
  const HEADER_SIZE = 8;
  let cursor = 0;
  while (cursor + HEADER_SIZE <= st.size) {
    const hdr = Buffer.alloc(HEADER_SIZE);
    const r1 = await fh.read(hdr, 0, HEADER_SIZE, cursor);
    if (r1.bytesRead !== HEADER_SIZE) {
      console.log(`header read short: ${r1.bytesRead}`);
      break;
    }
    console.log("header bytes:", hdr.toString("hex"));
    const len = hdr.readUInt32BE(0);
    const cks = hdr.readUInt32BE(4);
    console.log(`entry at ${cursor}: len=${len} cks=${cks}`);
    const data = Buffer.alloc(len);
    const r2 = await fh.read(data, 0, len, cursor + HEADER_SIZE);
    if (r2.bytesRead !== len) {
      console.log(`payload read short: ${r2.bytesRead} expected ${len}`);
      break;
    }
    console.log("payload hexdump:", await hexdump(data, Math.min(64, len)));
    cursor += HEADER_SIZE + len;
  }

  await fh.close();
}

debug().catch((e) => {
  console.error(e);
  process.exit(1);
});
