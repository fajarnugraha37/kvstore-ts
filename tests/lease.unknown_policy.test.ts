import { Engine } from "../libs/storage/engine";
import fs from "fs";
import { describe, expect, test } from "bun:test";

// helper to create a fresh engine with test dir
function mk(dir: string, opts: any = {}) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
  return new Engine(dir, "log.wal", opts);
}

describe("lease unknown policy", () => {
  test("create policy: single put should create lease and attach", async () => {
    const e = mk("data-test-create", { leaseUnknownPolicy: "create" });
    await e.open();
    const key = Buffer.from("a");
    await e.put(key, Buffer.from("v"), { leaseId: 999, leaseTtlMs: 1000 });
    // a lease should have been created (id != 999 necessarily) and mapping present
    const hex = key.toString("hex");
    const li = Array.from((e as any).keyToLease.values())[0];
    expect(typeof li).toBe("number");
    const info = e.getLeaseInfo(li as any);
    expect(info).not.toBeNull();
    await e.close();
  });

  test("ignore policy: single put should not attach when lease unknown", async () => {
    const e = mk("data-test-ignore", { leaseUnknownPolicy: "ignore" });
    await e.open();
    const key = Buffer.from("b");
    await e.put(key, Buffer.from("v"), { leaseId: 12345 });
    const hex = key.toString("hex");
    const lid = (e as any).keyToLease.get(hex);
    expect(lid).toBeUndefined();
    await e.close();
  });

  test("throw policy: single put should throw when lease unknown", async () => {
    const e = mk("data-test-throw", { leaseUnknownPolicy: "throw" });
    await e.open();
    const key = Buffer.from("c");
    await expect(
      e.put(key, Buffer.from("v"), { leaseId: 1 })
    ).rejects.toThrow();
    await e.close();
  });

  test("create policy: bulk put should create lease and attach", async () => {
    const e = mk("data-test-create_bulk", { leaseUnknownPolicy: "create" });
    await e.open();
    const entries = [
      { key: Buffer.from("x"), value: Buffer.from("1") },
      { key: Buffer.from("y"), value: Buffer.from("2") },
    ];
    await e.put(entries, { leaseId: 9999, leaseTtlMs: 1000 });
    const keys = entries.map((en) => en.key.toString("hex"));
    const lids = keys.map((k) => (e as any).keyToLease.get(k));
    expect(lids.every((l: any) => typeof l === "number")).toBe(true);
    await e.close();
  });

  test("ignore policy: bulk put should not attach when lease unknown", async () => {
    const e = mk("data-test-ignore_bulk", { leaseUnknownPolicy: "ignore" });
    await e.open();
    const entries = [
      { key: Buffer.from("m"), value: Buffer.from("1") },
      { key: Buffer.from("n"), value: Buffer.from("2") },
    ];
    await e.put(entries, { leaseId: 54321 });
    const lids = entries.map((en) =>
      (e as any).keyToLease.get(en.key.toString("hex"))
    );
    expect(lids.every((l: any) => typeof l === "undefined")).toBe(true);
    await e.close();
  });

  test("throw policy: bulk put should throw when lease unknown", async () => {
    const e = mk("data-test-throw_bulk", { leaseUnknownPolicy: "throw" });
    await e.open();
    const entries = [
      { key: Buffer.from("p"), value: Buffer.from("1") },
      { key: Buffer.from("q"), value: Buffer.from("2") },
    ];
    await expect(e.put(entries, { leaseId: 2 })).rejects.toThrow();
    await e.close();
  });
});
