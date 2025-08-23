/**
 * Return the smallest key strictly greater than the given key in lexicographic byte order.
 * If the key is all 0xFF, returns undefined (no finite successor).
 * @param key The key to find the next lexicographic key for.
 * @returns The next lexicographic key, or undefined if there is none.
 */
export function lexNext(key: Uint8Array): Uint8Array | undefined {
  if (key.length === 0) return new Uint8Array([0]); // convention: next after empty is 0x00
  const out = new Uint8Array(key);
  for (let i = out.length - 1; i >= 0; i--) {
    if (out[i] !== 0xff) {
      out[i]!++;
      return out;
    }
  }
  return undefined;
}

/**
 * Compute the smallest key that is strictly greater than all keys with the given prefix.
 * If no such key exists (prefix is all 0xFF bytes), return undefined to indicate an open-ended range.
 * @param prefix The prefix to find the end of.
 * @returns The exclusive end of the prefix, or undefined if there is none.
 */
export function prefixEndExclusive(prefix: Uint8Array): Uint8Array | undefined {
  if (prefix.length === 0) return undefined; // entire keyspace
  const out = new Uint8Array(prefix);
  for (let i = out.length - 1; i >= 0; i--) {
    if (out[i] !== 0xff) {
      out[i]!++;
      return out;
    }
  }
  // All bytes were 0xFF => there is no strict upper bound
  return undefined;
}
