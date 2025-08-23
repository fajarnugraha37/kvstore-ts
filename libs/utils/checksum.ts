/**
 * Compute the CRC32C (Castagnoli) checksum of the given data.
 * @param data The data to checksum.
 * @param seed The initial seed value (default: 0xFFFFFFFF).
 * @returns The CRC32C checksum.
 */
export const crc32c = (function () {
  /**
   * CRC32C (Castagnoli) table-driven implementation.
   * Based on standard polynomial 0x1EDC6F41.
   */
  const POLY = 0x1edc6f41 >>> 0;

  /**
   * CRC32C (Castagnoli) table-driven implementation.
   */
  const table = new Uint32Array(256);

  /**
   * Initialize the CRC32C (Castagnoli) table.
   * This function populates the `table` array with precomputed CRC values.
   */
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      if (c & 1) c = (c >>> 1) ^ POLY;
      else c >>>= 1;
    }
    table[i] = c >>> 0;
  }

  return (data: Uint8Array, seed = 0xffffffff): number => {
    let crc = seed ^ 0xffffffff;
    for (let i = 0; i < data.length; i++) {
      const byte = data[i]!;
      crc = (crc >>> 8) ^ table[(crc ^ byte) & 0xff]!;
    }
    return (crc ^ 0xffffffff) >>> 0;
  };
})();

/**
 * Compute the Adler-32 checksum of the given data.
 * @param data The data to checksum (Uint8Array).
 * @returns The Adler-32 checksum as an unsigned 32-bit integer.
 */
export const adler32 = (function () {
  const MOD_ADLER = 65521;

  return (data: Uint8Array): number => {
    let a = 1;
    let b = 0;
    const length = data.length;
    for (let i = 0; i < length; i++) {
      a = (a + data[i]!) % MOD_ADLER;
      b = (b + a) % MOD_ADLER;
    }
    return ((b << 16) | a) >>> 0;
  };
})();
