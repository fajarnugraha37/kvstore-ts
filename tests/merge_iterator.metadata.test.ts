import { describe, it, expect } from "bun:test";
import { kWayMerge } from "../libs/storage/merge_iterator";

async function* makeIter(key: string, val: string, rev: number, meta: any) {
  yield { key: Buffer.from(key), value: Buffer.from(val), rev, ...meta };
}

describe("kWayMerge metadata preservation", () => {
  it("preserves auxiliary fields and chooses highest-rev payload", async () => {
    const a = makeIter("a", "v1", 1, { createdAtSrc: 1000, src: "A" });
    const b = makeIter("a", "v2", 2, { createdAtSrc: 2000, src: "B" });

    const merged: any[] = [];
    for await (const e of kWayMerge([a, b])) {
      merged.push(e);
    }

    expect(merged.length).toBe(1);
    const out = merged[0];
    expect(out.rev).toBe(2);
    expect(out.createdAtSrc).toBe(2000);
    expect(out.src).toBe("B");
    expect(out.value instanceof Buffer).toBe(true);
    expect(out.value.toString()).toBe("v2");
  });

  it("when rev equal, prefers later iterable (higher idx) and preserves its metadata", async () => {
    const a = makeIter("x", "left", 5, { createdAtSrc: 1111, which: "left" });
    const b = makeIter("x", "right", 5, { createdAtSrc: 2222, which: "right" });

    const merged: any[] = [];
    for await (const e of kWayMerge([a, b])) merged.push(e);

    expect(merged.length).toBe(1);
    const out = merged[0];
    expect(out.rev).toBe(5);
    expect(out.which).toBe("right");
    expect(out.createdAtSrc).toBe(2222);
    expect(out.value.toString()).toBe("right");
  });
});
