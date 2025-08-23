import { describe, it, expect } from "bun:test";
import MemTable from "../libs/storage/memtable";

describe("memtable basic", () => {
  it("put/get and iterator", async () => {
    const m = new MemTable();
    m.put(Buffer.from("a"), Buffer.from("1"), 1);
    m.put(Buffer.from("b"), Buffer.from("2"), 2);
    const v = m.get(Buffer.from("a"));
    expect(v?.value?.toString()).toBe("1");

    const items: Array<string> = [];
    for await (const e of m.iterator()) {
      items.push(e.key.toString());
    }
    expect(items).toEqual(
      [Buffer.from("a").toString(), Buffer.from("b").toString()].map((s) =>
        Buffer.from(s).toString()
      )
    );
  });
});
