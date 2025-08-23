import { test } from "bun:test";
import { strict as assert } from "assert";
import { SSTWriter } from "../libs/storage/sstwriter";
import { Manifest } from "../libs/storage/manifest";
import fs from "fs";
import path from "path";

test("SSTWriter and Manifest fsync hooks invoked when strictAtomicity=true", () => {
  const tmpDir = path.join(process.cwd(), "tmp_strict_test");
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch {}
  fs.mkdirSync(tmpDir, { recursive: true });

  const events: Array<{ type: string; target: string }> = [];
  (SSTWriter as any).TEST_FSYNC_SPY = (type: string, t: string) => {
    events.push({ type, target: t });
  };
  (Manifest as any).TEST_FSYNC_SPY = (type: string, t: string) => {
    events.push({ type: `manifest:${type}`, target: t });
  };

  // write SST with strictAtomicity true
  const tmp = path.join(tmpDir, "a.tmp");
  const final = path.join(tmpDir, "a.sst");
  const w = new SSTWriter(tmp, final, {
    strictAtomicity: true,
    useBloom: false,
  });
  w.add(Buffer.from("k"), Buffer.from("v"), 1, 0, Date.now());
  const meta = w.finish();

  // manifest
  const m = Manifest.load(tmpDir, true);
  m.addFile({
    file: meta.file,
    minKeyHex: meta.minKey.toString("hex"),
    maxKeyHex: meta.maxKey.toString("hex"),
    size: meta.size,
    level: 0,
    walOffset: 0,
    createdAt: Date.now(),
  });

  // expect spy events for SST tmp-file fsync, rename, dir-fsync, and manifest tmp-file/rename/dir-fsync
  const gotTypes = events.map((e) => e.type);
  assert.ok(gotTypes.includes("tmp-file"), "SST tmp-file fsync recorded");
  assert.ok(gotTypes.includes("rename"), "SST rename recorded");
  assert.ok(gotTypes.includes("dir-fsync"), "SST dir-fsync recorded");
  assert.ok(
    gotTypes.includes("manifest:tmp-file"),
    "Manifest tmp-file fsync recorded"
  );
  assert.ok(gotTypes.includes("manifest:rename"), "Manifest rename recorded");
  assert.ok(
    gotTypes.includes("manifest:dir-fsync"),
    "Manifest dir-fsync recorded"
  );

  // cleanup
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch {}
  // clear spies
  delete (SSTWriter as any).TEST_FSYNC_SPY;
  delete (Manifest as any).TEST_FSYNC_SPY;
});
