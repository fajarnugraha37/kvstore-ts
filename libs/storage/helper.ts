/**
 * Write a 32-bit unsigned integer to a buffer.
 * @param v The integer value to write.
 * @returns A buffer containing the written value.
 */
export function writeUint32(v: number) {
  const b = Buffer.alloc(4);
  b.writeUInt32BE(v, 0);
  return b;
}
/**
 * Write a 64-bit unsigned integer to a buffer.
 * @param v The integer value to write.
 * @returns A buffer containing the written value.
 */
export function writeUint64(v: bigint) {
  const b = Buffer.alloc(8);
  b.writeBigUInt64BE(v, 0);
  return b;
}

/**
 * Encode a number as a varint.
 * @param n The number to encode.
 * @returns A buffer containing the encoded varint.
 */
export function varintEncode(n: number | bigint) {
  let v = typeof n === "bigint" ? n : BigInt(n);
  const parts: number[] = [];
  while (v >= 0x80n) {
    parts.push(Number((v & 0x7fn) | 0x80n));
    v >>= 7n;
  }
  parts.push(Number(v));
  return Buffer.from(parts);
}

/**
 * Decode a varint from a buffer.
 * @param buf The buffer containing the varint.
 * @param pos The position to start decoding from.
 * @returns The decoded value and the number of bytes read.
 */
export function varintDecode(buf: Buffer, pos: number) {
  let shift = 0n;
  let result = 0n;
  let i = pos;
  while (i < buf.length) {
    const b = BigInt(buf.readUInt8(i));
    result |= (b & 0x7fn) << shift;
    i++;
    if ((b & 0x80n) === 0n) break;
    shift += 7n;
  }
  return { value: Number(result), length: i - pos };
}

/**
 * Get the length of a varint.
 * @param n The number to encode.
 * @returns The length of the encoded varint.
 */
export function varintLen(n: number | bigint) {
  let v = typeof n === "bigint" ? n : BigInt(n);
  let l = 1;
  while (v >= 0x80n) {
    l++;
    v >>= 7n;
  }
  return l;
}

/**
 * Compute the FNV-1a hash of a buffer.
 * @param buf The buffer to hash.
 * @returns The FNV-1a hash.
 */
export function fnv1a(buf: Buffer) {
  let h = 0x811c9dc5 >>> 0;
  for (let i = 0; i < buf.length; i++) {
    h ^= buf.readUInt8(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/**
 * Compare two buffers.
 * @param a The first buffer.
 * @param b The second buffer.
 * @returns A negative number if a < b, a positive number if a > b, and 0 if they are equal.
 */
export function bufCmp(a: Buffer, b: Buffer) {
  return Buffer.compare(a, b);
}

