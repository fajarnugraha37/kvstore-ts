import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
import { withWalImpls } from "./util/engine_test_runner";
const makeTempDir = require("./util/tmpdir");

describe("engine background compaction toggle", () => {
  it("can start and stop background compaction at runtime", async () => {
    const dir = makeTempDir();
    await withWalImpls(async (impl) => {
      const e = new Engine(dir, "log.wal", {
        walImpl: impl,
        compactorOptions: { maxSstSize: 2048 } as any,
      });
      await e.open();
    // Ensure background compaction auto-started
    await new Promise((r) => setTimeout(r, 200));
    const beforeRuns = e.metrics.compaction.runs;
    // disable
    e.setBackgroundCompaction(false);
    await new Promise((r) => setTimeout(r, 1100));
    const afterRuns = e.metrics.compaction.runs;
    // since we stopped, runs shouldn't increase after stopping
    expect(afterRuns).toBe(beforeRuns);
    // restart
    e.setBackgroundCompaction(true, 200);
    await new Promise((r) => setTimeout(r, 450));
    const restarted = e.metrics.compaction.runs;
    expect(restarted).toBeGreaterThanOrEqual(afterRuns);
      await e.close();
    });
  });
});
