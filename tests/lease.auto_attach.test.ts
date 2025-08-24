import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { describe, afterEach, it, expect } from "bun:test";

describe("auto-attach leases on put()", () => {
  const dir = join(process.cwd(), "data", `lease_autotest_${Date.now()}`);
  afterEach(() => {
    try {
      if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
    } catch (e) {}
  });

  it("attaches key to lease when put with leaseId and evicts on expiry", async () => {
    const tp = { now: Date.now() };
    const timeProvider = () => tp.now;
    const e = new Engine(dir, "log.wal", { timeProvider });
    await e.open();

    const lease = e.grantLease(1000);
    const k = Buffer.from("k-auto");
    await e.put(k, Buffer.from("v"), { leaseId: lease });

    // confirm lease mapping
    const info = e.getLeaseInfo(lease);
    expect(info).not.toBeNull();
    expect(info!.keys).toContain(k.toString("hex"));

    // advance time past expiry and let evictor run
    tp.now += 2000;
    await new Promise((r) => setTimeout(r, 1200));

    // key should be gone
    expect(e.get(k)).toBeNull();

    await e.close();
  });
});
