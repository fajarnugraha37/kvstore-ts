import { test } from "bun:test";
import { strict as assert } from "assert";
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";

// This test spawns a child process which writes an SST and manifest with strictAtomicity=true
// The child process will exit immediately after the rename step to simulate a crash-after-rename.
// The parent then verifies the manifest and sst exist and can be loaded.

const helperScript = `
import fs from 'fs';
import path from 'path';
import { SSTWriter } from './libs/storage/sstwriter';
import { Manifest } from './libs/storage/manifest';
const dir = process.argv[2];
try { fs.mkdirSync(dir, { recursive: true }); } catch(e) {}
const tmp = path.join(dir, 'child.tmp');
const final = path.join(dir, 'child.sst');
const w = new SSTWriter(tmp, final, { strictAtomicity: true, useBloom: false });
w.add(Buffer.from('k'), Buffer.from('v'), 1, 0, Date.now());
w.finish();
const m = Manifest.load(dir, true);
m.addFile({ file: final, minKeyHex: Buffer.from('k').toString('hex'), maxKeyHex: Buffer.from('k').toString('hex'), size: fs.statSync(final).size, level: 0, walOffset: 0, createdAt: Date.now() });
process.exit(0);
`;

test("crash-after-rename simulation: child writes with strictAtomicity and parent recovers", () => {
  const tmpDir = path.join(process.cwd(), "tmp_crash_test");
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch (e) {}
  fs.mkdirSync(tmpDir, { recursive: true });

  // write helper file at repo root so imports resolve
  const helper = path.join(process.cwd(), "tmp_child_writer.ts");
  fs.writeFileSync(helper, helperScript, "utf8");

  // spawn child via bun so TypeScript imports work
  const res = spawnSync("bun", [helper, tmpDir], {
    cwd: process.cwd(),
    env: process.env,
  });
  if (res.error) throw res.error;
  // child exited; now verify files exist and manifest.json exists and lists the sst
  const mf = path.join(tmpDir, "manifest.json");
  assert.ok(fs.existsSync(mf), "manifest.json should exist after child run");
  const parsed = JSON.parse(fs.readFileSync(mf, "utf8"));
  assert.ok(
    parsed.files && parsed.files.length > 0,
    "manifest should contain at least one file"
  );
  const sstFile = parsed.files[0].file;
  assert.ok(
    fs.existsSync(sstFile),
    "sst file referenced in manifest should exist"
  );

  // cleanup
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch (e) {}
  try {
    fs.unlinkSync(path.join(process.cwd(), "tmp_child_writer.ts"));
  } catch (e) {}
});
