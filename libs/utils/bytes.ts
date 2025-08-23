export const bytes = {
  /**
   * Compare two byte arrays.
   * @param a The first byte array.
   * @param b The second byte array.
   * @returns A negative number if a < b, a positive number if a > b, and 0 if they are equal.
   */
  compare: compare,
  /**
   * Check if two byte arrays are equal.
   * @param a The first byte array.
   * @param b The second byte array.
   * @returns True if the byte arrays are equal, false otherwise.
   */
  eq: eqBytes,
  /**
   * Reverse a byte array.
   * @param a The byte array to reverse.
   * @returns The reversed byte array.
   */
  reverse: reverseBytes,
  /**
   * Convert a byte array to a base64 string.
   * @param b The byte array to convert.
   * @returns The converted base64 string.
   */
  toB64: (b: Uint8Array) => Buffer.from(b).toString("base64"),
  /**
   * Convert a base64 string to a byte array.
   * @param s The base64 string to convert.
   * @returns The converted byte array.
   */
  fromB64: (s: string) => Buffer.from(s, "base64"),
  /**
   * Check if a byte array starts with a given prefix.
   * @param a The byte array to check.
   * @param p The prefix to check for.
   * @returns True if the byte array starts with the prefix, false otherwise.
   */
  startsWith: (a: Uint8Array, p: Uint8Array) =>
    a.length >= p.length && p.every((v, i) => a[i] === v),
  /**
   * Check if a byte array ends with a given suffix.
   * @param a The byte array to check.
   * @param s The suffix to check for.
   * @returns True if the byte array ends with the suffix, false otherwise.
   */
  endsWith: (a: Uint8Array, s: Uint8Array) =>
    a.length >= s.length && s.every((v, i) => a[a.length - s.length + i] === v),
  /**
   * Find the upper bound index of a key in a sorted array of byte arrays.
   * @param a The sorted array of byte arrays.
   * @param key The key to find the upper bound for.
   * @returns The upper bound index.
   */
  upperBound: upperBound,
};

function compare(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const d = a[i]! - b[i]!;
    if (d !== 0) return d;
  }
  return a.length - b.length;
}

function eqBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function reverseBytes(a: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length);
  for (let i = 0, j = a.length - 1; i < a.length; i++, j--) {
    out[i] = a[j]!;
  }

  return out;
}

function upperBound(arr: Uint8Array[], key: Uint8Array): number {
  let lo = 0,
    hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (compare(arr[mid]!, key) <= 0) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
