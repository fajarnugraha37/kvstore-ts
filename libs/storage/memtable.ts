import { bufCmp } from "./helper";
import { MemTableSnapshot } from "./memtable_snapshot";
import { Node, type MemValue } from "./node";

export class MemTable {
  private root: Node | null = null;
  private _size = 0;
  private _bytes = 0; // approximate memory usage in bytes
  constructor(private approxLimitBytes = 64 * 1024) {}

  private height(n: Node | null) {
    return n ? n.height : 0;
  }
  private updateHeight(n: Node) {
    n.height = 1 + Math.max(this.height(n.left), this.height(n.right));
  }
  private balanceFactor(n: Node) {
    return this.height(n.left) - this.height(n.right);
  }

  private rotateRight(y: Node): Node {
    const x = y.left!;
    const T2 = x.right;
    x.right = y;
    y.left = T2;
    this.updateHeight(y);
    this.updateHeight(x);
    return x;
  }

  private rotateLeft(x: Node): Node {
    const y = x.right!;
    const T2 = y.left;
    y.left = x;
    x.right = T2;
    this.updateHeight(x);
    this.updateHeight(y);
    return y;
  }

  private balance(node: Node): Node {
    this.updateHeight(node);
    const bf = this.balanceFactor(node);
    if (bf > 1) {
      if (this.balanceFactor(node.left!) < 0)
        node.left = this.rotateLeft(node.left!);
      return this.rotateRight(node);
    }
    if (bf < -1) {
      if (this.balanceFactor(node.right!) > 0)
        node.right = this.rotateRight(node.right!);
      return this.rotateLeft(node);
    }
    return node;
  }

  put(key: Buffer, value: Buffer | null, rev?: number) {
    const v: MemValue = {
      value: value === null ? null : Buffer.from(value),
      rev,
    };
    let inserted = false;
    // approximate accounting: compute previous value size to update _bytes
    const prev = this.get(key);
    const insertRec = (node: Node | null): Node => {
      if (!node) {
        inserted = true;
        this._size++;
        // new key: account for key bytes
        this._bytes += key.length;
        // account for value bytes
        if (v.value) this._bytes += v.value.length;
        return new Node(key, v);
      }
      const cmp = bufCmp(key, node.key);
      if (cmp === 0) {
        // replace: adjust bytes delta for value size
        const prevValLen = node.val.value ? node.val.value.length : 0;
        const newValLen = v.value ? v.value.length : 0;
        this._bytes += newValLen - prevValLen;
        node.val = v; // replace
      } else if (cmp < 0) {
        node.left = insertRec(node.left);
      } else {
        node.right = insertRec(node.right);
      }
      return this.balance(node);
    };
    this.root = insertRec(this.root);
  }

  get(key: Buffer): MemValue | null {
    let cur = this.root;
    while (cur) {
      const cmp = bufCmp(key, cur.key);
      if (cmp === 0)
        return {
          value: cur.val.value === null ? null : Buffer.from(cur.val.value),
          rev: cur.val.rev,
        };
      cur = cmp < 0 ? cur.left : cur.right;
    }
    return null;
  }

  delete(key: Buffer, rev?: number) {
    let removed = false;
    const minNode = (n: Node): Node => {
      while (n.left) n = n.left;
      return n;
    };
    const deleteRec = (node: Node | null): Node | null => {
      if (!node) return null;
      const cmp = bufCmp(key, node.key);
      if (cmp < 0) node.left = deleteRec(node.left);
      else if (cmp > 0) node.right = deleteRec(node.right);
      else {
        // found
        removed = true;
        this._size--;
        if (!node.left || !node.right) {
          const t = node.left ? node.left : node.right;
          return t;
        } else {
          const succ = minNode(node.right!);
          node.key = succ.key;
          node.val = succ.val;
          node.right = deleteRec(node.right);
        }
      }
      return this.balance(node);
    };
    this.root = deleteRec(this.root);
    // if key not found, fall back to inserting tombstone (to keep behavior compatible)
    if (!removed) this.put(key, null, rev);
    else {
      // if removed, we already decreased _size and removed key bytes in deleteRec, nothing extra needed here
    }
  }

  size() {
    return this._size;
  }

  snapshot() {
    // Freeze the current tree by detaching its root and metadata into a snapshot object.
    // This is a cheap, synchronous operation (copy-on-write style): callers get an immutable
    // snapshot and the active memtable is reset to empty so flush can proceed deterministically.
    const snapRoot = this.root;
    const snapSize = this._size;
    const snapBytes = this._bytes;
    // reset active memtable
    this.root = null;
    this._size = 0;
    this._bytes = 0;
    return new MemTableSnapshot(snapRoot, snapSize, snapBytes);
  }

  sizeBytes() {
    return this._bytes;
  }
  approxLimit() {
    return this.approxLimitBytes;
  }

  // in-order async iterator
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

  /**
   * In-order async iterator over [start, end) where end is exclusive.
   * If start is undefined, iteration begins at the smallest key.
   * If end is undefined, iteration goes until the largest key.
   */
  async *iteratorRange(
    start?: Buffer,
    end?: Buffer
  ): AsyncGenerator<{ key: Buffer; value: Buffer | null; rev?: number }> {
    const stack: Array<Node> = [];
    let cur: Node | null = this.root;

    // initialize stack to the leftmost node >= start
    while (cur) {
      const cmp = start ? bufCmp(start, cur.key) : -1;
      if (cmp <= 0) {
        // cur.key >= start -> go left to find smaller >= start
        stack.push(cur);
        cur = cur.left;
      } else {
        // cur.key < start -> skip left subtree
        cur = cur.right;
      }
    }

    while (stack.length > 0) {
      cur = stack.pop()!;
      // if start is set, skip keys < start
      if (start && bufCmp(cur.key, start) < 0) {
        // continue to next
      } else {
        // if end is set and we've reached or surpassed it, stop
        if (end && bufCmp(cur.key, end) >= 0) return;
        yield {
          key: Buffer.from(cur.key),
          value: cur.val.value === null ? null : Buffer.from(cur.val.value),
          rev: cur.val.rev,
        };
      }

      // traverse right subtree: push leftmost nodes of right
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

export default MemTable;
