import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { Engine } from "../libs/storage/engine";
import { afterEach, describe, expect, it } from "bun:test";

describe("lease eviction and TTL semantics", () => {
  const dir = join(process.cwd(), "data", `lease_test_${Date.now()}`);
  afterEach(() => {
    try {
      if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
    } catch (e) {}
  });

  it("evicts keys attached to expired lease and preserves keys on renewal", async () => {
    const tp = { now: Date.now() };
    const timeProvider = () => tp.now;
    const e = new Engine(dir, "log.wal", { timeProvider });
    await e.open();

    // create key and attach to lease
    const k = Buffer.from("key1");
    await e.put(k, Buffer.from("v1"));

    const lease = e.grantLease(1000); // 1s TTL
    e.attachLease(lease, k);

    // key present initially
    expect(e.get(k)!.toString()).toBe("v1");

    // advance time but not past expiry; renew should extend
    tp.now += 500;
    e.renewLease(lease, 2000); // extend by 2s from now (now = original+500)

    // advance to just before original expiry (should still be present)
    tp.now += 400; // now at +900 from start
    expect(e.get(k)!.toString()).toBe("v1");

    // advance past renewed expiry
    tp.now += 2500; // now +3400 total > renewed expiry

    // wait a short moment to allow background evictor to run
    await new Promise((r) => setTimeout(r, 1200));

    // now key should be removed (get returns null)
    const got = e.get(k);
    expect(got).toBeNull();

    await e.close();
  });
});
