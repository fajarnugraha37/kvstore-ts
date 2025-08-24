import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { describe, afterEach, it, expect } from "bun:test";

describe("lease restart recovery", () => {
  const dir = join(process.cwd(), "data", `lease_restart_${Date.now()}`);
  afterEach(() => {
    try {
      if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
    } catch (e) {}
  });

  it("persists lease mappings across close/open and still evicts after reopen", async () => {
    const tp = { now: Date.now() };
    const timeProvider = () => tp.now;

    // create engine, grant lease, put key with lease, close
    const e1 = new Engine(dir, "log.wal", { timeProvider });
    await e1.open();
    const k = Buffer.from("restart-key");
    const lease = e1.grantLease(1000);
    await e1.put(k, Buffer.from("v"), { leaseId: lease });

    // verify mapping present
    const info1 = e1.getLeaseInfo(lease);
    expect(info1).not.toBeNull();
    expect(info1!.keys).toContain(k.toString("hex"));

    await e1.close();

    // reopen engine with same time provider and confirm mapping restored
    const e2 = new Engine(dir, "log.wal", { timeProvider });
    await e2.open();
    const info2 = e2.getLeaseInfo(lease);
    expect(info2).not.toBeNull();
    expect(info2!.keys).toContain(k.toString("hex"));

    // advance time past expiry and allow evictor to run after reopen
    tp.now += 2000;
    await new Promise((r) => setTimeout(r, 1200));

    // key should be evicted
    expect(e2.get(k)).toBeNull();

    await e2.close();
  });
});
