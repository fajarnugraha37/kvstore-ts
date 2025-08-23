import { rm, mkdir } from "node:fs/promises";
import { encode as msgpackEncode } from "@msgpack/msgpack";
import { writeFileSync } from "node:fs";

const ITER = Number(process.argv[2]) || 10000;
const PAYLOAD = Number(process.argv[3]) || 256;

function hrSec(s: bigint, e: bigint) { return Number(e - s) / 1e9 }

function makePayload(i: number) {
  return { i, payload: "x".repeat(PAYLOAD) };
}

async function bench() {
  const jsBufs: Buffer[] = [];
  const mpBufs: Buffer[] = [];
  for (let i = 0; i < ITER; i++) {
    const p = makePayload(i);
    jsBufs.push(Buffer.from(JSON.stringify(p)));
    mpBufs.push(Buffer.from(msgpackEncode(p)));
  }

  // write using concat
  const t0 = process.hrtime.bigint();
  const concatAll = Buffer.concat(jsBufs);
  writeFileSync("tmp_concat_js.bin", concatAll);
  const t1 = process.hrtime.bigint();

  const t2 = process.hrtime.bigint();
  const concatAllMp = Buffer.concat(mpBufs);
  writeFileSync("tmp_concat_mp.bin", concatAllMp);
  const t3 = process.hrtime.bigint();

  console.log("concat JS write time:", hrSec(t0, t1));
  console.log("concat MP write time:", hrSec(t2, t3));

  // write using writev (many small writes) via fs.writeSync
  const t4 = process.hrtime.bigint();
  const fd = require("node:fs").openSync("tmp_writev_js.bin", "w");
  for (const b of jsBufs) require("node:fs").writeSync(fd, b);
  require("node:fs").closeSync(fd);
  const t5 = process.hrtime.bigint();

  const t6 = process.hrtime.bigint();
  const fd2 = require("node:fs").openSync("tmp_writev_mp.bin", "w");
  for (const b of mpBufs) require("node:fs").writeSync(fd2, b);
  require("node:fs").closeSync(fd2);
  const t7 = process.hrtime.bigint();

  console.log("writeSync many JS writes:", hrSec(t4,t5));
  console.log("writeSync many MP writes:", hrSec(t6,t7));

  // small report
  writeFileSync("micro_report.txt", JSON.stringify({
    iter: ITER, payload: PAYLOAD,
    concat_js: hrSec(t0,t1), concat_mp: hrSec(t2,t3),
    many_js: hrSec(t4,t5), many_mp: hrSec(t6,t7)
  }, null, 2));
}

bench().catch((e)=>{ console.error(e); process.exit(1) });
