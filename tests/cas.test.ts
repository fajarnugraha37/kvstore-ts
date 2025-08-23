import { describe, it, expect } from "bun:test";
import Engine from "../libs/storage/engine";
const makeTempDir = require("./util/tmpdir");

describe("CAS (compare-and-swap)", () => {
  it("basic CAS success and failure", async () => {
    const dir = makeTempDir();
    const e = new Engine(dir, "log.wal");
    await e.open();

    // empty store: CAS expecting null should succeed
    const k = Buffer.from("x");
    const res1 = await (e as any).cas(k, null, Buffer.from("v1"));
    expect(res1.ok).toBe(true);
    expect(typeof res1.rev).toBe("number");

    // CAS with wrong expectedRev should fail
    const res2 = await (e as any).cas(
      k,
      (res1.rev as number) - 1,
      Buffer.from("v2")
    );
    expect(res2.ok).toBe(false);

    // CAS with correct expectedRev should succeed
    const res3 = await (e as any).cas(k, res1.rev as number, Buffer.from("v2"));
    expect(res3.ok).toBe(true);

    // verify final value
    const v = e.get(k);
    expect(v && v.toString()).toBe("v2");
    e.close();
  });

  it("concurrent CAS attempts serialize and result in a single winner per key", async () => {
    const dir = makeTempDir();
    const e = new Engine(dir, "log.wal");
    await e.open();

    const k = Buffer.from("concurrent");
    const attempts = 10;
    const promises: Promise<any>[] = [];
    for (let i = 0; i < attempts; i++) {
      const val = Buffer.from("v" + i);
      // Each CAS expects whatever current rev is null at start; first will succeed, others will fail or retry
      promises.push(
        (async () => {
          const r = await (e as any).cas(k, null, val);
          return { idx: i, result: r };
        })()
      );
    }
    const results = await Promise.all(promises);
    const successes = results.filter((r) => r.result && r.result.ok);
    // exactly one success expected because all expect null initially
    expect(successes.length).toBe(1);

    // final value should equal the winner's value
    const winner = successes[0];
    const final = e.get(k);
    expect(final && final.toString()).toBe("v" + winner.idx);

    e.close();
  }, 2000);
});
