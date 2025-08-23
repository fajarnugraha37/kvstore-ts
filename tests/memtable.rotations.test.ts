import { describe, it, expect } from "bun:test";
import MemTable from "../libs/storage/memtable";
import { getHeight, getRootKey } from "./testutils/memtable.testutils";

function keysToStrings(m: MemTable) {
  const out: string[] = [];
  const iter = m.iterator();
  return (async () => {
    for await (const e of iter) out.push(e.key.toString());
    return out;
  })();
}

describe("memtable AVL rotations", () => {
  it("performs RR rotation (right-right) when inserting increasing keys", async () => {
    const m = new MemTable(1024);
    for (let i = 0; i < 10; i++)
      m.put(Buffer.from(String(i)), Buffer.from("v"));
    // tree should be balanced and root height should be <= log2(n)+1
    const h = getHeight(m);
    expect(h).toBeLessThanOrEqual(6);
    const arr = await keysToStrings(m);
    expect(arr).toEqual(Array.from({ length: 10 }, (_, i) => String(i)));
  });

  it("performs LL rotation (left-left) when inserting decreasing keys", async () => {
    const m = new MemTable(1024);
    for (let i = 9; i >= 0; i--)
      m.put(Buffer.from(String(i)), Buffer.from("v"));
    const h = getHeight(m);
    expect(h).toBeLessThanOrEqual(6);
    const arr = await keysToStrings(m);
    expect(arr).toEqual(Array.from({ length: 10 }, (_, i) => String(i)));
  });

  it("performs LR rotation (left-right) shape", async () => {
    const m = new MemTable(1024);
    // sequence to trigger LR: insert 3,1,2
    m.put(Buffer.from("3"), Buffer.from("v"));
    m.put(Buffer.from("1"), Buffer.from("v"));
    m.put(Buffer.from("2"), Buffer.from("v"));
    const root = await getRootKey(m);
    expect(root?.toString()).toBe("2");
    const arr = await keysToStrings(m);
    expect(arr).toEqual(["1", "2", "3"]);
  });

  it("performs RL rotation (right-left) shape", async () => {
    const m = new MemTable(1024);
    // sequence to trigger RL: insert 1,3,2
    m.put(Buffer.from("1"), Buffer.from("v"));
    m.put(Buffer.from("3"), Buffer.from("v"));
    m.put(Buffer.from("2"), Buffer.from("v"));
    const root = await getRootKey(m);
    expect(root?.toString()).toBe("2");
    const arr = await keysToStrings(m);
    expect(arr).toEqual(["1", "2", "3"]);
  });
});
