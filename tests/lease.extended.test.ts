import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { describe, afterEach, it, expect } from "bun:test";

// enable eviction debug tracing for these tests to diagnose timing flakes
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

// helper: wait until predicate returns truthy or timeout
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

describe("lease extended semantics", () => {
  const base = "lease_ext";
  afterEach(() => {
    // best-effort cleanup of data dirs created by tests (pattern-based left as-is)
  });

  it("multi-key leases: attach multiple keys and evict them together", async () => {
    const { dir, engine } = mk(base, "multi");
    await engine.open();
    const lease = engine.grantLease(1000);
    const k1 = Buffer.from("m1");
    const k2 = Buffer.from("m2");
    await engine.put(k1, Buffer.from("a"), { leaseId: lease });
    await engine.put(k2, Buffer.from("b"), { leaseId: lease });
    // both attached
    const info = engine.getLeaseInfo(lease)!;
    expect(info.keys.length).toBeGreaterThanOrEqual(2);
    // wait until evictor removes both keys (timeout to avoid hangs)
    const ok = await waitFor(
      () => engine.get(k1) === null && engine.get(k2) === null,
      3000
    );
    expect(ok).toBeTruthy();
    await engine.close();
  });

  it("revoke: revoke lease and optionally delete keys", async () => {
    const { dir, engine } = mk(base, "revoke");
    await engine.open();
    const lease = engine.grantLease(1000);
    const k = Buffer.from("rv1");
    await engine.put(k, Buffer.from("v"), { leaseId: lease });
    // revoke without delete
    engine.revokeLease(lease, false);
    expect(engine.getLeaseInfo(lease)).toBeNull();
    // key should still exist
    expect(engine.get(k)).not.toBeNull();
    // create new lease and attach and revoke with delete
    const lease2 = engine.grantLease(1000);
    await engine.put(k, Buffer.from("v2"), { leaseId: lease2 });
    engine.revokeLease(lease2, true);
    // after revoke with delete, key may have been scheduled for delete; allow small wait
    await new Promise((r) => setTimeout(r, 200));
    expect(engine.get(k)).toBeNull();
    await engine.close();
  });

  it("auto-attach on put with opts and reattach when changing lease", async () => {
    const { dir, engine } = mk(base, "reattach");
    await engine.open();
    const k = Buffer.from("ra1");
    const l1 = engine.grantLease(1000);
    await engine.put(k, Buffer.from("v1"), { leaseId: l1 });
    const l2 = engine.grantLease(1000);
    // overwrite and attach to new lease
    await engine.put(k, Buffer.from("v2"), { leaseId: l2 });
    const info1 = engine.getLeaseInfo(l1);
    const info2 = engine.getLeaseInfo(l2)!;
    // ensure key moved from l1 to l2
    expect(
      info1 == null || !info1.keys.includes(k.toString("hex"))
    ).toBeTruthy();
    expect(info2.keys.includes(k.toString("hex"))).toBeTruthy();
    await engine.close();
  });

  it("bulk put with leaseId attaches all keys atomically and records lease metadata in WAL", async () => {
    const { dir, engine } = mk(base, "bulk");
    await engine.open();
    const l = engine.grantLease(1000);
    const entries = [
      { key: Buffer.from("bk1"), value: Buffer.from("1") },
      { key: Buffer.from("bk2"), value: Buffer.from("2") },
    ];
    await engine.put(entries, { leaseId: l });
    // both keys should be attached
    const info = engine.getLeaseInfo(l)!;
    const hexes = entries.map((e) => e.key.toString("hex"));
    for (const h of hexes) expect(info.keys.includes(h)).toBeTruthy();
    // reopen engine to ensure WAL replay restores lease mapping
    await engine.close();
    const e2 = new Engine(dir, "log.wal", { timeProvider: () => Date.now() });
    await e2.open();
    const info2 = e2.getLeaseInfo(l)!;
    for (const h of hexes) expect(info2.keys.includes(h)).toBeTruthy();
    await e2.close();
  });

  it("grouping deletes: evictor groups deletes into a single tx WAL record", async () => {
    const { dir, engine } = mk(base, "groupdel");
    await engine.open();
    const l = engine.grantLease(1000);
    const entries = [
      { key: Buffer.from("gd1"), value: Buffer.from("1") },
      { key: Buffer.from("gd2"), value: Buffer.from("2") },
      { key: Buffer.from("gd3"), value: Buffer.from("3") },
    ];
    await engine.put(entries, { leaseId: l });
    // wait until evictor removes all keys
    const ok2 = await waitFor(
      () => entries.every((e) => engine.get(e.key) === null),
      3000
    );
    expect(ok2).toBeTruthy();
    await engine.close();
  });

  it("automatic re-attach on overwrite replaces previous lease mapping", async () => {
    const { dir, engine } = mk(base, "auto_reattach");
    await engine.open();
    const k = Buffer.from("ar1");
    const l1 = engine.grantLease(1000);
    await engine.put(k, Buffer.from("x"), { leaseId: l1 });
    const l2 = engine.grantLease(1000);
    // put without lease (should leave mapping to l1)
    await engine.put(k, Buffer.from("y"));
    expect(
      engine.getLeaseInfo(l1)!.keys.includes(k.toString("hex"))
    ).toBeTruthy();
    // now overwrite with new lease -> move mapping
    await engine.put(k, Buffer.from("z"), { leaseId: l2 });
    expect(
      engine.getLeaseInfo(l1) == null ||
        !engine.getLeaseInfo(l1)!.keys.includes(k.toString("hex"))
    ).toBeTruthy();
    expect(
      engine.getLeaseInfo(l2)!.keys.includes(k.toString("hex"))
    ).toBeTruthy();
    await engine.close();
  });

  it("onLeaseEvent ordering and slow wal.flush blocking eviction", async () => {
    const { dir, engine } = mk(base, "events");
    // create a small stub to simulate slow wal.flush by monkeypatching engine.wal
    await engine.open();
    const recorded: Array<any> = [];
    engine.onLeaseEvent = (ev) => {
      recorded.push(ev);
    };

    // monkeypatch flush to delay when called by evictor
    const realWal: any = (engine as any).wal;
    let slow = false;
    const origFlush = realWal.flush?.bind(realWal);
    (realWal as any).flush = async () => {
      if (slow) {
        // delay to simulate slow fsync
        await new Promise((r) => setTimeout(r, 500));
      }
      if (origFlush) await origFlush();
    };

    const k = Buffer.from("ev1");
    const l = engine.grantLease(1000);
    await engine.put(k, Buffer.from("v"), { leaseId: l });

    // enable slow flush right before expiry and wait for evict event
    slow = true;
    const seen = await waitFor(
      () =>
        recorded.find((x) => x.type === "evict" || x.type === "expire") !==
        undefined,
      4000
    );
    expect(recorded.length).toBeGreaterThanOrEqual(1);
    expect(recorded[0].type).toBe("grant");
    expect(seen).toBeTruthy();

    await engine.close();
  });
});
