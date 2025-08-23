import { describe, it, expect } from "bun:test";
import { maybeConsumePerEntry } from "../libs/storage/throttle";

describe("throttle helper", () => {
  it("consumes writer-provided delta when available", async () => {
    let consumed = 0;
    const tokenBucket = {
      async consume(n: number) {
        consumed += n;
      },
    } as any;
    const writer = {
      deltaSizeForEntry(k: any, v: any, r: any) {
        return 42;
      },
    } as any;
    await maybeConsumePerEntry(
      tokenBucket,
      writer,
      Buffer.from("k"),
      Buffer.from("v"),
      1
    );
    expect(consumed).toBe(42);
  });

  it("uses fallback delta when writer lacks api", async () => {
    let consumed = 0;
    const tokenBucket = {
      async consume(n: number) {
        consumed += n;
      },
    } as any;
    const writer = {} as any;
    await maybeConsumePerEntry(
      tokenBucket,
      writer,
      Buffer.from("kk"),
      Buffer.from("vvv"),
      1
    );
    // fallback = klen + vlen + 10
    expect(consumed).toBe(2 + 3 + 10);
  });

  it("does nothing when tokenBucket is null", async () => {
    const writer = {
      deltaSizeForEntry(k: any, v: any, r: any) {
        return 100;
      },
    } as any;
    await maybeConsumePerEntry(
      null,
      writer,
      Buffer.from("a"),
      Buffer.from("b"),
      1
    );
    // no exception thrown
    expect(true).toBe(true);
  });
});
