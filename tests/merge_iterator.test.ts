import { describe, it, expect } from "bun:test";
import { kWayMerge } from "../libs/storage/merge_iterator";

function asyncFromArray(arr: Array<{ key: Buffer; value: Buffer | null }>) {
  return (async function* () {
    for (const e of arr) yield e;
  })();
}

describe("kWayMerge", () => {
  it("merges multiple sorted async iterables and prefers later iterables on key conflicts", async () => {
    const a = asyncFromArray([
      { key: Buffer.from("a"), value: Buffer.from("1") },
      { key: Buffer.from("b"), value: Buffer.from("2") },
    ]);
    const b = asyncFromArray([
      { key: Buffer.from("b"), value: Buffer.from("3") },
      { key: Buffer.from("c"), value: Buffer.from("4") },
    ]);
    const c = asyncFromArray([
      { key: Buffer.from("d"), value: Buffer.from("5") },
    ]);

    const out: Array<{ key: string; value: string | null }> = [];
    for await (const e of kWayMerge([a, b, c])) {
      out.push({
        key: e.key.toString(),
        value: e.value ? e.value.toString() : null,
      });
    }

    // expected keys: a, b, c, d where b comes from "b" iterable (later one)
    expect(out.length).toBe(4);
    expect(out[0]).toEqual({ key: "a", value: "1" });
    expect(out[1]).toEqual({ key: "b", value: "3" });
    expect(out[2]).toEqual({ key: "c", value: "4" });
    expect(out[3]).toEqual({ key: "d", value: "5" });
  });
});
