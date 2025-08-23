import { bufCmp } from "./helper";
import { Node } from "./node";

// Immutable snapshot representing a frozen memtable tree. This is returned by MemTable.snapshot()
// and provides an async iterator over the frozen entries without copying all nodes.
export class MemTableSnapshot {
  constructor(
    private root: Node | null,
    private _size: number,
    private _bytes: number
  ) {}

  size() {
    return this._size;
  }

  sizeBytes() {
    return this._bytes;
  }

  // async iterator that traverses the frozen tree without modifying it
  async *iterator(): AsyncGenerator<{
    key: Buffer;
    value: Buffer | null;
    rev?: number;
  }> {
    const stack: Array<Node> = [];
    let cur: Node | null = this.root;
    while (stack.length > 0 || cur) {
      while (cur) {
        stack.push(cur);
        cur = cur.left;
      }
      cur = stack.pop()!;
      yield {
        key: Buffer.from(cur.key),
        value: cur.val.value === null ? null : Buffer.from(cur.val.value),
        rev: cur.val.rev,
      };
      cur = cur.right;
    }
  }

  async *iteratorRange(
    start?: Buffer,
    end?: Buffer
  ): AsyncGenerator<{ key: Buffer; value: Buffer | null; rev?: number }> {
    const stack: Array<Node> = [];
    let cur: Node | null = this.root;

    while (cur) {
      const cmp = start ? bufCmp(start, cur.key) : -1;
      if (cmp <= 0) {
        stack.push(cur);
        cur = cur.left;
      } else {
        cur = cur.right;
      }
    }

    while (stack.length > 0) {
      cur = stack.pop()!;
      if (start && bufCmp(cur.key, start) < 0) {
        // skip
      } else {
        if (end && bufCmp(cur.key, end) >= 0) return;
        yield {
          key: Buffer.from(cur.key),
          value: cur.val.value === null ? null : Buffer.from(cur.val.value),
          rev: cur.val.rev,
        };
      }

      let node = cur.right;
      while (node) {
        const cmp = start ? bufCmp(start, node.key) : -1;
        if (cmp <= 0) {
          stack.push(node);
          node = node.left;
        } else {
          node = node.right;
        }
      }
    }
  }
}
