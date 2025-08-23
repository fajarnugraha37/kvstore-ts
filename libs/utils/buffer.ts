export const u64LittleEndian = {
  /**
   * Read a little-endian u64 from the buffer.
   * @param buf The buffer to read from.
   * @param off The offset to read from.
   * @returns The value read and the next offset.
   */
  read: function (
    buf: Uint8Array,
    off: number
  ): { value: bigint; next: number } {
    let n = 0n;
    for (let i = 0; i < 8; i++) {
      n |= BigInt(buf[off + i]!) << (8n * BigInt(i));
    }
    return { value: n, next: off + 8 };
  },
  /**
   * Write a little-endian u64 to the buffer.
   * @param buf The buffer to write to.
   * @param off The offset to write at.
   * @param v The value to write.
   * @returns The next offset.
   */
  write: function (buf: Uint8Array, off: number, v: bigint): number {
    let n = v;
    for (let i = 0; i < 8; i++) {
      buf[off + i] = Number(n & 0xffn);
      n >>= 8n;
    }
    return off + 8;
  },
};

export const varUint = {
  /**
   * Encode a number as a little-endian varuint.
   * @param n The number to encode.
   * @returns The encoded varuint.
   */
  encode: function (n: number): Uint8Array {
    if (n < 0) throw new RangeError("varuint negative");
    const out: number[] = [];
    let v = n >>> 0;
    while (v >= 0x80) {
      out.push((v & 0x7f) | 0x80);
      v >>>= 7;
    }
    out.push(v);
    return Uint8Array.from(out);
  },
  /**
   * Decode a little-endian varuint from the buffer.
   * @param buf The buffer to read from.
   * @param off The offset to read from.
   * @returns The decoded varuint and the next offset.
   */
  decode: function (buf: Uint8Array, off = 0): { value: number; next: number } {
    let shift = 0;
    let result = 0;
    let i = off;
    while (true) {
      const b = buf[i++]!;
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
      if (shift > 35) throw new Error("varuint too large");
    }
    return { value: result >>> 0, next: i };
  },
};