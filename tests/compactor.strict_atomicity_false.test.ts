import { test } from "bun:test";
import { strict as assert } from "assert";
import { SSTWriter } from "../libs/storage/sstwriter";
import Manifest from "../libs/storage/manifest";
import fs from "fs";
import path from "path";

test("SSTWriter and Manifest fsync hooks NOT invoked when strictAtomicity=false", () => {
  const tmpDir = path.join(process.cwd(), "tmp_strict_false_test");
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

  // write SST with strictAtomicity false (default)
  const tmp = path.join(tmpDir, "a.tmp");
  const final = path.join(tmpDir, "a.sst");
  const w = new SSTWriter(tmp, final, {
    strictAtomicity: false,
    useBloom: false,
  });
  w.add(Buffer.from("k"), Buffer.from("v"), 1, 0, Date.now());
  const meta = w.finish();

  // manifest with strictAtomicity false
  const m = Manifest.load(tmpDir, false);
  m.addFile({
    file: meta.file,
    minKeyHex: meta.minKey.toString("hex"),
    maxKeyHex: meta.maxKey.toString("hex"),
    size: meta.size,
    level: 0,
    walOffset: 0,
    createdAt: Date.now(),
  });

  const gotTypes = events.map((e) => e.type);

  // tmp-file fsync and dir-fsync should NOT be recorded when strictAtomicity is false
  assert.equal(
    gotTypes.includes("tmp-file"),
    false,
    "SST tmp-file fsync should NOT be recorded"
  );
  assert.equal(
    gotTypes.includes("dir-fsync"),
    false,
    "SST dir-fsync should NOT be recorded"
  );
  assert.equal(
    gotTypes.includes("manifest:tmp-file"),
    false,
    "Manifest tmp-file fsync should NOT be recorded"
  );
  assert.equal(
    gotTypes.includes("manifest:dir-fsync"),
    false,
    "Manifest dir-fsync should NOT be recorded"
  );

  // rename events are expected (renameSpy is unconditional in code)
  assert.equal(gotTypes.includes("rename"), true, "SST rename recorded");
  assert.equal(
    gotTypes.includes("manifest:rename"),
    true,
    "Manifest rename recorded"
  );

  // cleanup
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch {}
  delete (SSTWriter as any).TEST_FSYNC_SPY;
  delete (Manifest as any).TEST_FSYNC_SPY;
});
