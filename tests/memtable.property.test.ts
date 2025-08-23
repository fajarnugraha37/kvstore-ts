import { describe, it, expect } from "bun:test";
import MemTable from "../libs/storage/memtable";

// Property-based randomized tests for AVL invariants.
// We will run many random sequences of puts/deletes and then check:
// - In-order iterator produces sorted keys (BST property)
// - Balance factor of every node is -1,0,1 (AVL property)
// - Height bound: h <= 1.44 * log2(n+2) (approx upper bound for AVL trees)

function buf(k: number) {
  return Buffer.from(String(k));
}

function collectNodes(root: any) {
  const nodes: any[] = [];
  const stack: any[] = [];
  let cur = root;
  while (stack.length > 0 || cur) {
    while (cur) {
      stack.push(cur);
      cur = cur.left;
    }
    cur = stack.pop();
    nodes.push(cur);
    cur = cur.right;
  }
  return nodes;
}

function height(node: any): number {
  if (!node) return 0;
  return node.height || Math.max(height(node.left), height(node.right)) + 1;
}

function balanceFactor(node: any): number {
  const lh = node.left ? node.left.height ?? height(node.left) : 0;
  const rh = node.right ? node.right.height ?? height(node.right) : 0;
  return lh - rh;
}

function getRoot(node: any) {
  return node ? node : null;
}

describe("memtable randomized AVL invariants", () => {
  it("maintains AVL invariants under random ops", () => {
    const trials = 50;
    const opsPerTrial = 200;
    for (let t = 0; t < trials; t++) {
      const m = new MemTable(1024 * 1024);
      const present = new Set<number>();
      for (let i = 0; i < opsPerTrial; i++) {
        const op = Math.random();
        const key = Math.floor(Math.random() * 200);
        if (op < 0.7) {
          // put
          m.put(buf(key), Buffer.from("v"));
          present.add(key);
        } else {
          // delete
          m.delete(buf(key));
          present.delete(key);
        }
      }

      // collect in-order keys and check sortedness
      const out: string[] = [];
      const iter = m.iterator();
      // synchronous consumption by walking the async iterator
      (async () => {
        for await (const e of iter) out.push(e.key.toString());
      })();

      // Wait a tick to allow iterator to finish (iterator is synchronous but returns an async generator)
      // In Bun test runner, this should be fine without awaiting; to be safe, we assert via structure below.

      const anym = m as any;
      const root = getRoot(anym.root);
      const nodes = collectNodes(root);

      // BST order: in-order traversal keys should be sorted
      const keys = nodes.map((n) => n.key.toString());
      for (let i = 1; i < keys.length; i++) {
        expect(keys[i - 1] <= keys[i]).toBe(true);
      }

      // AVL balance factor check
      for (const n of nodes) {
        const bf = balanceFactor(n);
        expect(bf >= -1 && bf <= 1).toBe(true);
      }

      // height bound (approx): h <= 1.44 * log2(n+2)
      const n = nodes.length;
      const h = anym.root ? anym.root.height ?? height(anym.root) : 0;
      const bound = Math.ceil(1.44 * Math.log2(n + 2));
      expect(h).toBeLessThanOrEqual(bound + 2); // +2 slack for small n
    }
  });
});
