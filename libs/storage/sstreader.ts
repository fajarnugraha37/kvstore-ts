/*
SST on-disk per-file and per-entry layouts (historical):

v1 (original):
  header: "SST1" + version(1)
  block: u32(entryCount) [ entries... ]
  entry (v1): u32 keyLen, key, u32 valueLen (0xffffffff for tombstone), [value?], u64 revision

v2 (intermediate):
  - switched revision encoding from fixed u64 to varint
  entry (v2): u32 keyLen, key, u32 valueLen (0xffffffff for tombstone), [value?], varint revision

v3 (current):
  - adds two per-entry varints after revision: walOffset and createdAt
  entry (v3): u32 keyLen, key, u32 valueLen (0xffffffff for tombstone), [value?], varint revision, varint walOffset, varint createdAt

Reader implementations must detect the file version from the header and parse/skip the appropriate tailing fields so that skipping entries keeps offsets correct.
*/
import { openSync, readSync, closeSync, fstatSync } from "node:fs";
import { acquire, release } from "./file_refcount";
import { crc32c } from "../utils";
import { writeUint32, writeUint64, fnv1a, varintDecode } from "./helper";
import { SST_HEADER_LEN, SST_HEADER_MAGIC, sstableDebug } from "./sstable";

export class SSTReader {
  private index: Array<{
    firstKey: Buffer;
    offset: bigint;
    blockLen: number;
    blockCks: number;
  }> = [];
  private bloom: Buffer | null = null;
  private fileVersion = 1; // default to v1 (fixed 8-byte rev) for backward compatibility
  constructor(private path: string) {}

  static open(path: string) {
    const fd = openSync(path, "r");
    try {
      const stat = fstatSync(fd);
      if (stat.size < 24) throw new Error("sst: file too small");
      const footer = Buffer.alloc(28);
      readSync(fd, footer, 0, 28, stat.size - 28);
      const indexOffset = footer.readBigUInt64BE(0);
      const bloomOffset = footer.readBigUInt64BE(8);
      const indexCks = footer.readUInt32BE(16);
      const footerCks = footer.readUInt32BE(20);
      const magic = footer.readUInt32BE(24);
      if (magic !== 0x53535446) throw new Error("sst: bad footer magic");
      // verify footer checksum covers indexOffset,bloomOffset,indexCks
      const footerPre = Buffer.concat([
        writeUint64(indexOffset),
        writeUint64(bloomOffset),
        writeUint32(indexCks),
      ]);
      const calcFooter = crc32c(footerPre) >>> 0;
      if (calcFooter !== footerCks)
        throw new Error("sst: footer checksum mismatch");
      const indexOffsetNum = Number(indexOffset);
      const indexEnd =
        bloomOffset && bloomOffset > indexOffset
          ? Number(bloomOffset)
          : stat.size - 28; // footer is 28 bytes now
      const indexSize = indexEnd - indexOffsetNum;
      if (indexSize <= 0) throw new Error("sst: invalid index size");
      const indexBuf = Buffer.alloc(indexSize);
      // read header magic + version at file start to detect fileVersion
      const headerBuf = Buffer.alloc(SST_HEADER_LEN);
      readSync(fd, headerBuf, 0, SST_HEADER_LEN, 0);
      const hdrMagic = headerBuf.slice(0, SST_HEADER_MAGIC.length).toString();
      if (hdrMagic !== SST_HEADER_MAGIC)
        throw new Error("sst: bad header magic");
      const version = headerBuf.readUInt8(SST_HEADER_MAGIC.length);
      // set reader version for parsing entries
      const r = new SSTReader(path);
      r.fileVersion = version || 1;
      readSync(fd, indexBuf, 0, indexSize, indexOffsetNum);
      // verify index checksum
      const calc = crc32c(indexBuf) >>> 0;
      if (calc !== indexCks) throw new Error("sst: index checksum mismatch");
      let pos = 0;
      const blockCount = indexBuf.readUInt32BE(pos);
      pos += 4;
      const idx: Array<{
        firstKey: Buffer;
        offset: bigint;
        blockLen: number;
        blockCks: number;
      }> = [];
      for (let i = 0; i < blockCount; i++) {
        const fklen = indexBuf.readUInt32BE(pos);
        pos += 4;
        const fk = indexBuf.slice(pos, pos + fklen);
        pos += fklen;
        const of = indexBuf.readBigUInt64BE(pos);
        pos += 8;
        const blen = indexBuf.readUInt32BE(pos);
        pos += 4;
        const bcks = indexBuf.readUInt32BE(pos);
        pos += 4;
        idx.push({
          firstKey: Buffer.from(fk),
          offset: of,
          blockLen: blen,
          blockCks: bcks,
        });
      }
      // r already created and has fileVersion set above
      r.index = idx;
      if (bloomOffset && bloomOffset > BigInt(0)) {
        // bloom header: bits(u32) + k(u32) + bitset
        const bloomSize = stat.size - Number(bloomOffset) - 28; // footer is 28 bytes now
        if (bloomSize > 0) {
          const bbuf = Buffer.alloc(bloomSize);
          readSync(fd, bbuf, 0, bloomSize, Number(bloomOffset));
          r.bloom = bbuf;
        }
      }
      return r;
    } finally {
      closeSync(fd);
    }
  }

  private possiblyContains(key: Buffer) {
    if (!this.bloom) return true;
    if (this.bloom.length < 8) return true;
    const bits = this.bloom.readUInt32BE(0);
    const k = this.bloom.readUInt32BE(4);
    const bitset = this.bloom.slice(8);
    const h1 = BigInt(fnv1a(key) >>> 0);
    const h2 = BigInt(crc32c(key) >>> 0);
    for (let j = 0; j < k; j++) {
      const probe = Number((h1 + BigInt(j) * h2) % BigInt(bits));
      const byte = Math.floor(probe / 8);
      const bit = probe % 8;
      if (byte < 0 || byte >= bitset.length) return true;
      const byteVal = bitset[byte];
      if (typeof byteVal !== "number") return true;
      if ((byteVal & (1 << bit)) === 0) return false;
    }
    return true;
  }

  get(key: Buffer): Buffer | null {
    if (!this.possiblyContains(key)) return null;
    const fd = openSync(this.path, "r");
    try {
      if (this.index.length === 0) return null;
      const idx = this.index;
      let lo = 0;
      let hi = idx.length - 1;
      let bi = 0;
      while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        const midEntry = idx[mid];
        if (!midEntry) break;
        const cmp = Buffer.compare(key, midEntry.firstKey);
        if (cmp >= 0) {
          bi = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      const ent = this.index[bi];
      if (!ent) return null;
      const offset = Number(ent.offset);
      const blen = ent.blockLen;
      if (sstableDebug.enabled) sstableDebug.blockReads++;
  // handle possible per-block compression flag in stored block length
  const storedLen = blen >>> 0;
  const compressed = (storedLen & require('./sstable').BLOCK_COMPRESSED_FLAG) !== 0;
  const onDiskLen = storedLen & ~require('./sstable').BLOCK_COMPRESSED_FLAG;
  const storedBuf = Buffer.alloc(onDiskLen);
  readSync(fd, storedBuf, 0, onDiskLen, offset);
  const cks = crc32c(storedBuf);
  if (cks !== ent.blockCks) throw new Error("sst: block checksum mismatch");
  const bbuf = compressed ? require('./sstable').decompressBlock(storedBuf) : storedBuf;
      let pos = 0;
      const num = bbuf.readUInt32BE(pos);
      pos += 4;
      for (let i = 0; i < num; i++) {
        const klen = bbuf.readUInt32BE(pos);
        pos += 4;
        const kbuf = bbuf.slice(pos, pos + klen);
        pos += klen;
        const vlen = bbuf.readUInt32BE(pos);
        pos += 4;
        if (kbuf.equals(key)) {
          if (vlen === 0xffffffff) {
            // tombstone: read rev depending on fileVersion and return null
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              pos += 8;
            } else {
              const dec = varintDecode(bbuf, pos);
              pos += dec.length;
              // for fileVersion >=3 there are two additional varints
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
              }
            }
            return null;
          }
          const v = Buffer.from(bbuf.slice(pos, pos + vlen));
          pos += vlen;
          if (this.fileVersion === 1) {
            pos += 8;
          } else {
            const dec2 = varintDecode(bbuf, pos);
            pos += dec2.length;
            if (this.fileVersion >= 3) {
              const w = varintDecode(bbuf, pos);
              pos += w.length;
              const c = varintDecode(bbuf, pos);
              pos += c.length;
            }
          }
          return v;
        }
        if (vlen !== 0xffffffff) pos += vlen;
        // skip revision varint for entries we don't match
        const decSkip = varintDecode(bbuf, pos);
        pos += decSkip.length;
        if (this.fileVersion >= 3) {
          const wskip = varintDecode(bbuf, pos);
          pos += wskip.length;
          const cskip = varintDecode(bbuf, pos);
          pos += cskip.length;
        }
      }
      return null;
    } finally {
      closeSync(fd);
    }
  }

  // Read a key and return both value and revision (rev may be undefined for older files)
  getWithRev(key: Buffer): { value: Buffer | null; rev?: number; walOffset?: number; createdAt?: number } {
    if (!this.possiblyContains(key)) return { value: null };
    const fd = openSync(this.path, "r");
    try {
      if (this.index.length === 0) return { value: null };
      const idx = this.index;
      let lo = 0;
      let hi = idx.length - 1;
      let bi = 0;
      while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        const midEntry = idx[mid];
        if (!midEntry) break;
        const cmp = Buffer.compare(key, midEntry.firstKey);
        if (cmp >= 0) {
          bi = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      const ent = this.index[bi];
      if (!ent) return { value: null };
      const offset = Number(ent.offset);
      const blen = ent.blockLen;
      if (sstableDebug.enabled) sstableDebug.blockReads++;
  const storedLen = blen >>> 0;
  const compressed = (storedLen & require('./sstable').BLOCK_COMPRESSED_FLAG) !== 0;
  const onDiskLen = storedLen & ~require('./sstable').BLOCK_COMPRESSED_FLAG;
  const storedBuf = Buffer.alloc(onDiskLen);
  readSync(fd, storedBuf, 0, onDiskLen, offset);
  const cks = crc32c(storedBuf);
  if (cks !== ent.blockCks) throw new Error("sst: block checksum mismatch");
  const bbuf = compressed ? require('./sstable').decompressBlock(storedBuf) : storedBuf;
      let pos = 0;
      const num = bbuf.readUInt32BE(pos);
      pos += 4;
      for (let i = 0; i < num; i++) {
        const klen = bbuf.readUInt32BE(pos);
        pos += 4;
        const kbuf = bbuf.slice(pos, pos + klen);
        pos += klen;
        const vlen = bbuf.readUInt32BE(pos);
        pos += 4;
        if (kbuf.equals(key)) {
          if (vlen === 0xffffffff) {
            // tombstone: read rev depending on fileVersion and return null
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              return { value: null, rev };
            } else {
              const dec = varintDecode(bbuf, pos);
              pos += dec.length;
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
                return { value: null, rev: dec.value, walOffset: w.value, createdAt: c.value };
              }
              return { value: null, rev: dec.value };
            }
          }
          const v = Buffer.from(bbuf.slice(pos, pos + vlen));
          pos += vlen;
          if (this.fileVersion === 1) {
            const rev = Number(bbuf.readBigUInt64BE(pos));
            return { value: v, rev };
          } else {
            const dec2 = varintDecode(bbuf, pos);
            pos += dec2.length;
            if (this.fileVersion >= 3) {
              const w = varintDecode(bbuf, pos);
              pos += w.length;
              const c = varintDecode(bbuf, pos);
              pos += c.length;
              return { value: v, rev: dec2.value, walOffset: w.value, createdAt: c.value };
            }
            return { value: v, rev: dec2.value };
          }
        }
        if (vlen !== 0xffffffff) pos += vlen;
        const decSkip = varintDecode(bbuf, pos);
        pos += decSkip.length; // skip rev
        if (this.fileVersion >= 3) {
          const wskip = varintDecode(bbuf, pos);
          pos += wskip.length;
          const cskip = varintDecode(bbuf, pos);
          pos += cskip.length;
        }
      }
      return { value: null };
    } finally {
      closeSync(fd);
    }
  }

  async *iterator(): AsyncGenerator<{
    key: Buffer;
    value: Buffer | null;
    rev?: number;
    walOffset?: number;
    createdAt?: number;
  }> {
    // register as reader for safe deletion
    acquire(this.path);
    const fd = openSync(this.path, "r");
    try {
      for (const blk of this.index) {
        const offset = Number(blk.offset);
  const blen = blk.blockLen >>> 0;
  const compressed = (blen & require('./sstable').BLOCK_COMPRESSED_FLAG) !== 0;
  const onDiskLen = blen & ~require('./sstable').BLOCK_COMPRESSED_FLAG;
  const storedBuf = Buffer.alloc(onDiskLen);
  readSync(fd, storedBuf, 0, onDiskLen, offset);
  const cks = crc32c(storedBuf);
  if (cks !== blk.blockCks) throw new Error("sst: block checksum mismatch");
  const bbuf = compressed ? require('./sstable').decompressBlock(storedBuf) : storedBuf;
        let pos = 0;
        const num = bbuf.readUInt32BE(pos);
        pos += 4;
        for (let i = 0; i < num; i++) {
          const klen = bbuf.readUInt32BE(pos);
          pos += 4;
          const kbuf = Buffer.from(bbuf.slice(pos, pos + klen));
          pos += klen;
          const vlen = bbuf.readUInt32BE(pos);
          pos += 4;
          if (vlen === 0xffffffff) {
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              pos += 8;
              yield { key: kbuf, value: null, rev };
            } else {
              const dec = varintDecode(bbuf, pos);
              pos += dec.length;
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
                yield { key: kbuf, value: null, rev: dec.value, walOffset: w.value, createdAt: c.value };
              } else {
                yield { key: kbuf, value: null, rev: dec.value };
              }
            }
          } else {
            const v = Buffer.from(bbuf.slice(pos, pos + vlen));
            pos += vlen;
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              pos += 8;
              yield { key: kbuf, value: v, rev };
            } else {
              const dec2 = varintDecode(bbuf, pos);
              pos += dec2.length;
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
                yield { key: kbuf, value: v, rev: dec2.value, walOffset: w.value, createdAt: c.value };
              } else {
                yield { key: kbuf, value: v, rev: dec2.value };
              }
            }
          }
        }
      }
    } finally {
      try {
        closeSync(fd);
      } catch {}
      release(this.path);
    }
  }

  /**
   * Iterator over a key range [start, end) — end is exclusive. If start is omitted, begins at file start.
   */
  async *iteratorRange(
    start?: Buffer,
    end?: Buffer
  ): AsyncGenerator<{ key: Buffer; value: Buffer | null; rev?: number; walOffset?: number; createdAt?: number }> {
    acquire(this.path);
    const fd = openSync(this.path, "r");
    try {
      if (this.index.length === 0) return;
      // determine starting block index via binary search over firstKey
      let bi = 0;
      if (start) {
        let lo = 0;
        let hi = this.index.length - 1;
        while (lo <= hi) {
          const mid = Math.floor((lo + hi) / 2);
          const midEntry = this.index[mid]!;
          const cmp = Buffer.compare(start, midEntry.firstKey);
          if (cmp >= 0) {
            bi = mid;
            lo = mid + 1;
          } else {
            hi = mid - 1;
          }
        }
      }

      for (let idx = bi; idx < this.index.length; idx++) {
        const blk = this.index[idx]!;
        const offset = Number(blk.offset);
        const blen = blk.blockLen;
        const bbuf = Buffer.alloc(blen);
        readSync(fd, bbuf, 0, blen, offset);
        const cks = crc32c(bbuf);
        if (cks !== blk.blockCks) throw new Error("sst: block checksum mismatch");
        let pos = 0;
        const num = bbuf.readUInt32BE(pos);
        pos += 4;
        for (let i = 0; i < num; i++) {
          const klen = bbuf.readUInt32BE(pos);
          pos += 4;
          const kbuf = Buffer.from(bbuf.slice(pos, pos + klen));
          pos += klen;
          const vlen = bbuf.readUInt32BE(pos);
          pos += 4;
          // stop if we've passed the end key
          if (end && Buffer.compare(kbuf, end) >= 0) return;
          if (start && Buffer.compare(kbuf, start) < 0) {
            // skip this entry's payload and metadata
            if (vlen !== 0xffffffff) pos += vlen;
            if (this.fileVersion === 1) pos += 8;
            else {
              const decSkip = varintDecode(bbuf, pos);
              pos += decSkip.length;
              if (this.fileVersion >= 3) {
                const wskip = varintDecode(bbuf, pos);
                pos += wskip.length;
                const cskip = varintDecode(bbuf, pos);
                pos += cskip.length;
              }
            }
            continue;
          }

          if (vlen === 0xffffffff) {
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              pos += 8;
              yield { key: kbuf, value: null, rev };
            } else {
              const dec = varintDecode(bbuf, pos);
              pos += dec.length;
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
                yield { key: kbuf, value: null, rev: dec.value, walOffset: w.value, createdAt: c.value };
              } else {
                yield { key: kbuf, value: null, rev: dec.value };
              }
            }
          } else {
            const v = Buffer.from(bbuf.slice(pos, pos + vlen));
            pos += vlen;
            if (this.fileVersion === 1) {
              const rev = Number(bbuf.readBigUInt64BE(pos));
              pos += 8;
              yield { key: kbuf, value: v, rev };
            } else {
              const dec2 = varintDecode(bbuf, pos);
              pos += dec2.length;
              if (this.fileVersion >= 3) {
                const w = varintDecode(bbuf, pos);
                pos += w.length;
                const c = varintDecode(bbuf, pos);
                pos += c.length;
                yield { key: kbuf, value: v, rev: dec2.value, walOffset: w.value, createdAt: c.value };
              } else {
                yield { key: kbuf, value: v, rev: dec2.value };
              }
            }
          }
        }
      }
    } finally {
      try {
        closeSync(fd);
      } catch {}
      release(this.path);
    }
  }
}
