import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { describe, afterEach, it, expect } from "bun:test";

// enable eviction debug tracing for this test to capture evictor logs on Windows
process.env.KV_DEBUG_EVICT = process.env.KV_DEBUG_EVICT || "1";

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

async function waitFor(pred: () => boolean, timeout = 3000, interval = 50) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      if (pred()) return true;
    } catch {}
    await new Promise((r) => setTimeout(r, interval));
  }
  return false;
}

describe("lease WAL grouped tx", () => {
  const base = "lease_wal_tx";
  afterEach(() => {});

  it("evictor writes a single tx WAL record for grouped lease deletes", async () => {
    const { dir, engine } = mk(base, "group_tx");
    await engine.open();
    const lease = engine.grantLease(1000);
    const entries = [
      { key: Buffer.from("w1"), value: Buffer.from("a") },
      { key: Buffer.from("w2"), value: Buffer.from("b") },
      { key: Buffer.from("w3"), value: Buffer.from("c") },
    ];
    await engine.put(entries, { leaseId: lease });

    // wait until evictor removes all keys (poll) then close engine
    const ok = await waitFor(
      () => entries.every((e) => engine.get(e.key) === null),
      4000
    );
    expect(ok).toBeTruthy();
    await engine.close();

    // scan WAL and find transaction records (scan after close for deterministic view)
    const wal: any = (engine as any).wal;
    const txEntries: any[] = [];
    for await (const rec of wal.scanWithOffsets(0)) {
      if (rec && rec.value && rec.value.tx && Array.isArray(rec.value.ops)) {
        txEntries.push(rec.value);
      }
    }

    // find a tx entry whose ops all have null value (deletes) and keys match our entries
    const deletes = txEntries.filter((t) => {
      if (!Array.isArray(t.ops)) return false;
      const ops = t.ops;
      if (ops.length !== entries.length) return false;
      // ensure all ops are deletes (value == null)
      return ops.every((o) => o.value == null && typeof o.key === "string");
    });

    // debug: if no matching tx found, print discovered txEntries for inspection
    if (deletes.length === 0) {
      console.log("[debug] txEntries found:", txEntries.length);
      for (const t of txEntries) {
        try {
          console.log("[debug] tx ops:", JSON.stringify(t.ops));
        } catch (e) {
          console.log("[debug] tx ops (failed stringify)");
        }
      }
    }
    expect(deletes.length).toBeGreaterThanOrEqual(1);
    // ensure at least one such tx exists and it's a single record grouping all deletes
    const found = deletes[0];
    expect(found.ops.length).toBe(entries.length);
  });
});
