import MemTable from "../../libs/storage/memtable";

// Test-only utilities for inspecting a MemTable's structure.
// These functions are intentionally kept in tests/ to avoid exporting internals to production code.

export async function getRootKey(m: MemTable): Promise<Buffer | null> {
  // Snapshot to get a separate memtable instance (safe) and traverse to find root key.
  // We rely on the implementation detail that snapshot produces a tree with same shape.
  // Use a recursion to find the root by walking from the snapshot's iterator and reconstructing.
  // Simpler: traverse using stack to find the first element's parent relationships is hard; instead
  // we reconstruct height and root key by building a map of key->height via insertion into a new tree
  // and then find the root as the middle element. But to keep it simple and robust, we'll rely on
  // inspecting the internal node via (m as any).root which is accessible in JS even though private.
  const anym = m as any;
  if (!anym.root) return null;
  return Buffer.from(anym.root.key);
}

export function getHeight(m: MemTable): number {
  const anym = m as any;
  return anym.root ? anym.root.height : 0;
}
