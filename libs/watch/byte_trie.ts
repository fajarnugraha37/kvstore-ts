// Simple byte-trie implementation to index string keys for prefix/suffix/contains lookups.
export class ByteTrieNode {
  children: Map<number, ByteTrieNode> = new Map();
  end = false;
}

export class ByteTrie {
  root = new ByteTrieNode();

  insert(key: string) {
    let node = this.root;
    for (let i = 0; i < key.length; i++) {
      const c = key.charCodeAt(i);
      let nxt = node.children.get(c);
      if (!nxt) {
        nxt = new ByteTrieNode();
        node.children.set(c, nxt);
      }
      node = nxt;
    }
    node.end = true;
  }

  // return all keys (naive traversal) that start with prefix
  startsWith(prefix: string): string[] {
    const res: string[] = [];
    const node = this._walk(prefix);
    if (!node) return res;
    this._collect(node, prefix, res);
    return res;
  }

  private _walk(prefix: string): ByteTrieNode | null {
    let node = this.root;
    for (let i = 0; i < prefix.length; i++) {
      const c = prefix.charCodeAt(i);
      const nxt = node.children.get(c);
      if (!nxt) return null;
      node = nxt;
    }
    return node;
  }

  private _collect(node: ByteTrieNode, cur: string, out: string[]) {
    if (node.end) out.push(cur);
    for (const [k, v] of node.children.entries()) {
      this._collect(v, cur + String.fromCharCode(k), out);
    }
  }
}
