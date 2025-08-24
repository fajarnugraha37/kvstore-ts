import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { describe, it, expect } from "bun:test";

function mk(dirBase: string, name: string) {
  const dir = join(process.cwd(), "data", `${dirBase}_${name}_${Date.now()}`);
  try {
    if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
  } catch {}
  return {
    dir,
    engine: new Engine(dir, "log.wal", { timeProvider: () => Date.now() }),
  };
}

async function waitFor(pred: () => boolean, timeout = 4000, interval = 50) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      if (pred()) return true;
    } catch {}
    await new Promise((r) => setTimeout(r, interval));
  }
  return false;
}

describe("lease eviction replay", () => {
  it("eviction tx includes lease metadata and replay publishes it", async () => {
    const { dir, engine } = mk("lease", "evict_replay");
    await engine.open();
    const lease = engine.grantLease(1000);
    await engine.put(
      [
        { key: Buffer.from("er1"), value: Buffer.from("a") },
        { key: Buffer.from("er2"), value: Buffer.from("b") },
      ],
      { leaseId: lease }
    );

    // wait for eviction to run and remove keys
    const ok = await waitFor(
      () =>
        engine.get(Buffer.from("er1")) === null &&
        engine.get(Buffer.from("er2")) === null,
      4000
    );
    expect(ok).toBeTruthy();
    await engine.close();

    // Open new engine instance and replay WAL via WatchManager
    const e2 = new Engine(dir, "log.wal", { timeProvider: () => Date.now() });
    await e2.open();
    const wm = e2.watchManager;

    const recs: any[] = [];
    const sub = wm.add({ type: "all" }, false);
    sub.onEvent((ev) => recs.push(ev));

    await wm.replayFromScan((e2 as any).wal.scanWithOffsets(0));

    // find eviction events for er1/er2 that include leaseId and leaseExpiresAt
    const found = recs.filter(
      (r) =>
        (r.key === "er1" || r.key === "er2") &&
        r.value === null &&
        typeof r.leaseId === "number" &&
        typeof r.leaseExpiresAt === "number"
    );
    expect(found.length).toBeGreaterThanOrEqual(2);

    await e2.close();
  });
});
