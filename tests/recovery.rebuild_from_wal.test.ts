import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
import { readFileSync, writeFileSync, existsSync } from "node:fs";

describe("recovery rebuild", () => {
  it("rebuilds a corrupt sst from WAL when possible", async () => {
    const makeTempDir = require("./util/tmpdir");
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", { walImpl: impl });
      await e.open();
      // insert multiple keys so SST covers a range
      await e.put(Buffer.from("a"), Buffer.from("va"));
      await e.put(Buffer.from("m"), Buffer.from("vm"));
      await e.put(Buffer.from("z"), Buffer.from("vz"));
      await e.flush();

      const manifest = JSON.parse(readFileSync(`${dir}/manifest.json`, "utf8"));
      const files = Array.isArray(manifest) ? manifest : manifest.files || [];
      expect(files.length >= 1).toBe(true);
      const f = files[files.length - 1];
      expect(existsSync(f.file)).toBe(true);

      // corrupt the index checksum in the footer to force SSTReader.open to fail
      try {
        const buf = readFileSync(f.file);
        if (buf && buf.length > 24) {
          const b = Buffer.from(buf);
          // footer layout now: indexOffset(8) bloomOffset(8) indexCks(4) magic(4)
          // zero out index checksum
          b.writeUInt32BE(0, b.length - 8);
          writeFileSync(f.file, b);
        }
      } catch (e) {}

      // reopen Engine which should attempt to rebuild the SST from WAL
      await e.close();
      const e2 = new Engine(dir, "log.wal", { walImpl: impl });
      await e2.open();
      const manifest2 = JSON.parse(
        readFileSync(`${dir}/manifest.json`, "utf8")
      );
      const files2 = Array.isArray(manifest2)
        ? manifest2
        : manifest2.files || [];

      // there should be an SST present covering the range; check that at least one file exists
      expect(files2.length >= 1).toBe(true);
      // try reading key 'm' which should be present after rebuild
      const v = e2.get(Buffer.from("m"));
      expect(v && v.toString()).toBe("vm");
      e2.close();
    });
  });
});
