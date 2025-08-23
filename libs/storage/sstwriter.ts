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
import {
  writeFileSync,
  openSync,
  closeSync,
  renameSync,
  mkdirSync,
  fsyncSync,
} from "node:fs";
import { dirname } from "node:path";
import { crc32c } from "../utils";
import {
  writeUint32,
  varintEncode,
  varintLen,
  writeUint64,
  fnv1a,
} from "./helper";
import { SST_FOOTER_LEN, SST_HEADER_LEN, type FileMeta } from "./sstable";

export type SSTWriterOptions = {
  blockSize?: number;
  useBloom?: boolean;
  bitsPerEntry?: number; // fixed bits per entry
  adaptiveBloom?: boolean; // if true, compute bitsPerEntry from entries
  minBitsPerEntry?: number;
  maxBitsPerEntry?: number;
  compressionAlgo?: number; // 0 = none, 1 = deflate
  compressionThreshold?: number; // minimum raw block size to attempt compression
};

export class SSTWriter {
  // We'll build blocks incrementally so we can provide exact size estimates while streaming
  private blocks: Array<{ firstKey: Buffer; entries: Buffer[]; len: number }> =
    [];
  private curEntries: Buffer[] = [];
  private curLen = 0; // sum of entry buffer lengths in current block
  private totalEntries = 0;
  private totalKeyValueBytes = 0; // sum of key+value lengths
  private allKeys: Buffer[] = [];
  private blockSize: number;
  private useBloom: boolean;
  private opts: SSTWriterOptions;

  constructor(
    private tmpPath: string,
    private finalPath: string,
    blockSizeOrOptions: number | SSTWriterOptions = 4 * 1024,
    useBloomFlag = true
  ) {
    if (typeof blockSizeOrOptions === "number") {
      this.blockSize = blockSizeOrOptions;
      this.useBloom = useBloomFlag;
      this.opts = {} as SSTWriterOptions;
    } else {
      this.opts = blockSizeOrOptions;
      this.blockSize = blockSizeOrOptions.blockSize ?? 4 * 1024;
      this.useBloom = blockSizeOrOptions.useBloom ?? true;
    }
  }

  // Add an entry to the writer. Returns nothing; use estimateSizeAfterAdd() to check size before adding.
  // For file format v3 we append two varints after the revision: walOffset and createdAt (both optional).
  add(
    key: Buffer,
    value: Buffer | null,
    rev?: number,
    walOffset?: number,
    createdAt?: number
  ) {
    const k = Buffer.from(key);
    const v = value === null ? null : Buffer.from(value);
    const klen = writeUint32(k.length);
    const vlen = writeUint32(v ? v.length : 0xffffffff);
    const revBuf = varintEncode(rev ?? 0);
    const walBuf = varintEncode(typeof walOffset === "number" ? walOffset : 0);
    const createdBuf = varintEncode(typeof createdAt === "number" ? createdAt : 0);
    const entry = v
      ? Buffer.concat([klen, k, vlen, v, revBuf, walBuf, createdBuf])
      : Buffer.concat([klen, k, vlen, revBuf, walBuf, createdBuf]);
    // if adding would overflow block and current block has entries, flush
    if (
      this.curLen + entry.length > this.blockSize &&
      this.curEntries.length > 0
    ) {
      this.flushCurrentBlock();
    }
    if (this.curEntries.length === 0) {
      // record first key for the new block
      this.curEntries.push(entry);
    } else {
      this.curEntries.push(entry);
    }
    this.curLen += entry.length;
    this.totalEntries++;
    this.totalKeyValueBytes += k.length + (v ? v.length : 0);
    this.allKeys.push(k);
  }

  // Flush the current in-memory block into blocks array
  private flushCurrentBlock() {
    if (this.curEntries.length === 0) return;
    // block payload includes a u32 count followed by entries
    const blockPayloadLen = this.curLen + 4; // 4 bytes for count
    const firstKey = (() => {
      // first entry => first 4 bytes length then key
      const e = this.curEntries[0]!;
      const klen = e.readUInt32BE(0);
      return Buffer.from(e.slice(4, 4 + klen));
    })();
    this.blocks.push({
      firstKey,
      entries: this.curEntries.slice(),
      len: blockPayloadLen,
    });
    this.curEntries = [];
    this.curLen = 0;
  }

  get entryCount() {
    return this.totalEntries;
  }

  // Estimate the final SST size after adding a hypothetical entry (without mutating state)
  estimateSizeAfterAdd(key: Buffer, value: Buffer | null, rev?: number) {
    // compute projected blocks and sizes
    let projBlocks = this.blocks.map((b) => ({
      len: b.len,
      firstKey: b.firstKey,
    }));
    let projCurLen = this.curLen;
    let projCurEntries = this.curEntries.length;
    let projTotalEntries = this.totalEntries;
    let projTotalKV = this.totalKeyValueBytes;

    const klen = 4 + key.length; // u32 + key
    const vlen = value === null ? 4 : 4 + value.length; // u32 + value (or 0xffffffff)
  const revLen = typeof rev === "number" ? varintLen(rev) : 10; // exact if known, else worst-case
  // include conservative sizes for walOffset and createdAt varints
  const walLen = 1;
  const createdLen = 1;
  const entryLen = klen + vlen + revLen + walLen + createdLen;

    // if adding would overflow current block and current block not empty, it will be flushed
    if (projCurLen + entryLen > this.blockSize && projCurEntries > 0) {
      // flush current
      // preserve the current block's firstKey if we can determine it
      let curFirst: Buffer = Buffer.alloc(0);
      if (this.curEntries.length > 0) {
        try {
          const e = this.curEntries[0]!;
          const fklen = e.readUInt32BE(0);
          curFirst = Buffer.from(e.slice(4, 4 + fklen));
        } catch (e) {}
      }
      projBlocks.push({ len: projCurLen + 4, firstKey: curFirst });
      projCurLen = 0;
      projCurEntries = 0;
    }
    // add entry into current block
    projCurLen += entryLen;
    projCurEntries += 1;
    projTotalEntries += 1;
    projTotalKV += key.length + (value ? value.length : 0);

    // include current block if non-empty; determine its firstKey precisely where possible
    if (projCurEntries > 0) {
      let curFirst: Buffer = Buffer.alloc(0);
      if (this.curEntries.length > 0) {
        try {
          const e = this.curEntries[0]!;
          const fklen = e.readUInt32BE(0);
          curFirst = Buffer.from(e.slice(4, 4 + fklen));
        } catch (e) {}
      } else {
        // if curEntries is empty but projection added entries, the new key will be the first key
        curFirst = key;
      }
      projBlocks.push({ len: projCurLen + 4, firstKey: curFirst });
    }

    const headerLen = SST_HEADER_LEN;
    const blocksSize = projBlocks.reduce((s, b) => s + b.len, 0);

    // index size = 4 + for each block: 4 + fklen + 8 + 4 + 4
    const indexSize =
      4 +
      projBlocks.reduce(
        (s, b) => s + (4 + (b.firstKey ? b.firstKey.length : 0) + 8 + 4 + 4),
        0
      );
    // note: firstKey lengths are not tracked for projected current block; approximate with 0 which is exact because we only need total size

    let bloomSize = 0;
    if (this.useBloom) {
      let bitsPerEntry = this.opts.bitsPerEntry ?? 10;
      if (this.opts.adaptiveBloom) {
        const avg = Math.max(
          1,
          Math.floor(projTotalKV / Math.max(1, projTotalEntries))
        );
        bitsPerEntry = Math.max(
          this.opts.minBitsPerEntry ?? 6,
          Math.min(this.opts.maxBitsPerEntry ?? 18, Math.round(avg / 8) + 6)
        );
      }
      const entriesCount = Math.max(1, projTotalEntries);
      const bits = Math.max(64, Math.ceil(entriesCount * bitsPerEntry));
      const bytes = Math.ceil(bits / 8);
      bloomSize = 4 + 4 + bytes; // bits(u32) + k(u32) + bitset
    }

    const footerSize = 28;
    const total = headerLen + blocksSize + indexSize + bloomSize + footerSize;
    return total;
  }

  // Micro API: exact number of bytes that adding this entry would append to the file
  // This returns the delta in bytes to be appended (including block flush effects) without mutating writer state.
  deltaSizeForEntry(key: Buffer, value: Buffer | null, rev?: number) {
    // compute entry encoding length
    const klen = 4 + key.length;
    const vlen = value === null ? 4 : 4 + value.length;
  const revLen = typeof rev === "number" ? varintLen(rev) : 10;
  const walLen = 1;
  const createdLen = 1;
  const entryLen = klen + vlen + revLen + walLen + createdLen; // exact if rev provided

    // if current block empty, adding will add entryLen to current block plus possibly index/footer growth later
    // if current block has some content, adding may either fit or cause a flush then start a new block
    const curBlockLen = this.curLen;
    const willFlush =
      curBlockLen > 0 && curBlockLen + entryLen > this.blockSize;

    // immediate data added is either entryLen (if no flush) or (curBlockLen + 4) [flushed block] + entryLen (new block) + 4 (count for new block)
    if (!willFlush) {
      return entryLen; // data appended into current in-memory block
    } else {
      // flush current block to disk (curBlockLen + 4), then new block has entry (entryLen) and later index/footer growth
      return curBlockLen + 4 + entryLen;
    }
  }

  // Simulate final SST file size after adding the given entry without mutating state.
  // This builds an in-memory projection of blocks, index and bloom sizes and returns the exact final byte length.
  simulatedSizeAfterAdd(key: Buffer, value: Buffer | null, rev?: number) {
    // create projected blocks (shallow copy of lengths and firstKey)
    const projBlocks: Array<{ len: number; firstKey: Buffer }> =
      this.blocks.map((b) => ({ len: b.len, firstKey: b.firstKey }));
    let projCurLen = this.curLen;
    let projCurEntries = this.curEntries.length;
    let projTotalEntries = this.totalEntries;
    let projTotalKV = this.totalKeyValueBytes;

    const klen = 4 + key.length;
    const vlen = value === null ? 4 : 4 + value.length;
  const revLen = typeof rev === "number" ? varintLen(rev) : 10;
  const walLen = 1;
  const createdLen = 1;
  const entryLen = klen + vlen + revLen + walLen + createdLen; // conservative varint max len unless rev known

    // if adding would overflow current block and current block not empty, it will be flushed
    if (projCurLen + entryLen > this.blockSize && projCurEntries > 0) {
      // flush current
      // determine firstKey of current block if available
      let curFirst: Buffer = Buffer.alloc(0);
      if (this.curEntries.length > 0) {
        try {
          const e = this.curEntries[0]!;
          const fklen = e.readUInt32BE(0);
          curFirst = Buffer.from(e.slice(4, 4 + fklen));
        } catch (e) {}
      }
      projBlocks.push({ len: projCurLen + 4, firstKey: curFirst });
      projCurLen = 0;
      projCurEntries = 0;
    }

    // add entry into current block
    // if current block is empty, this entry becomes its firstKey
    let curFirstForNewBlock: Buffer | null = null;
    if (projCurEntries === 0) curFirstForNewBlock = key;
    projCurLen += entryLen;
    projCurEntries += 1;
    projTotalEntries += 1;
    projTotalKV += key.length + (value ? value.length : 0);

    // include the (projected) current block
    if (projCurEntries > 0) {
      const fk =
        this.curEntries.length > 0
          ? (() => {
              try {
                const e = this.curEntries[0]!;
                const fklen = e.readUInt32BE(0);
                return Buffer.from(e.slice(4, 4 + fklen));
              } catch (e) {
                return curFirstForNewBlock ?? Buffer.alloc(0);
              }
            })()
          : curFirstForNewBlock ?? Buffer.alloc(0);
      projBlocks.push({ len: projCurLen + 4, firstKey: fk });
    }

    const headerLen = SST_HEADER_LEN;
    const blocksSize = projBlocks.reduce((s, b) => s + b.len, 0);

    // compute exact index size using exact firstKey lengths
    const indexSize =
      4 +
      projBlocks.reduce(
        (s, b) => s + (4 + (b.firstKey ? b.firstKey.length : 0) + 8 + 4 + 4),
        0
      );

    // compute exact bloom size if enabled
    let bloomSize = 0;
    if (this.useBloom) {
      let bitsPerEntry = this.opts.bitsPerEntry ?? 10;
      if (this.opts.adaptiveBloom) {
        const avg = Math.max(
          1,
          Math.floor(projTotalKV / Math.max(1, projTotalEntries))
        );
        bitsPerEntry = Math.max(
          this.opts.minBitsPerEntry ?? 6,
          Math.min(this.opts.maxBitsPerEntry ?? 18, Math.round(avg / 8) + 6)
        );
      }
      const entriesCount = Math.max(1, projTotalEntries);
      const bits = Math.max(64, Math.ceil(entriesCount * bitsPerEntry));
      const bytes = Math.ceil(bits / 8);
      bloomSize = 4 + 4 + bytes;
    }

    const footerSize = SST_FOOTER_LEN;
    const total = headerLen + blocksSize + indexSize + bloomSize + footerSize;
    return total;
  }

  // Return an exact estimate for the current in-progress SST file size (without writing)
  getEstimatedSize() {
    // include flushed blocks
    const blocksLen = this.blocks.reduce((s, b) => s + b.len, 0);
    const curBlockLen = this.curEntries.length > 0 ? this.curLen + 4 : 0;
    const blocksSize = blocksLen + curBlockLen;

    const totalBlocks =
      this.blocks.length + (this.curEntries.length > 0 ? 1 : 0);
    // index size: 4 + for each block (4 + fklen + 8 + 4 + 4)
    // compute fklen for flushed blocks exactly; for current block use actual firstKey if available
    let idxSize = 4;
    for (let i = 0; i < this.blocks.length; i++) {
      const blk = this.blocks[i];
      const fklen = blk && blk.firstKey ? blk.firstKey.length : 0;
      idxSize += 4 + fklen + 8 + 4 + 4;
    }
    if (this.curEntries.length > 0) {
      // determine firstKey of current block if possible
      let fklen = 0;
      try {
        const e = this.curEntries[0]!;
        fklen = e.readUInt32BE(0);
      } catch (e) {
        fklen = 0;
      }
      idxSize += 4 + fklen + 8 + 4 + 4;
    }
    const indexSize = idxSize;

    let bloomSize = 0;
    if (this.useBloom) {
      let bitsPerEntry = this.opts.bitsPerEntry ?? 10;
      if (this.opts.adaptiveBloom) {
        const avg = Math.max(
          1,
          Math.floor(this.totalKeyValueBytes / Math.max(1, this.totalEntries))
        );
        bitsPerEntry = Math.max(
          this.opts.minBitsPerEntry ?? 6,
          Math.min(this.opts.maxBitsPerEntry ?? 18, Math.round(avg / 8) + 6)
        );
      }
      const entriesCount = Math.max(1, this.totalEntries);
      const bits = Math.max(64, Math.ceil(entriesCount * bitsPerEntry));
      const bytes = Math.ceil(bits / 8);
      bloomSize = 4 + 4 + bytes;
    }

    const footerSize = 28;
    const headerLen = SST_HEADER_LEN;
    return headerLen + blocksSize + indexSize + bloomSize + footerSize;
  }

  finish(): FileMeta {
    try {
      mkdirSync(dirname(this.tmpPath), { recursive: true });
    } catch {}

    // ensure current block is flushed into blocks
    if (this.curEntries.length > 0) this.flushCurrentBlock();

  const blocksBufs: Buffer[] = [];
  const firstKeys: Buffer[] = [];
  const storedLens: number[] = [];
    let off = BigInt(SST_HEADER_LEN); // header length
    const compression = this.opts.compressionAlgo ?? 0;
  const compressionThreshold = this.opts.compressionThreshold ?? require('./sstable').DEFAULT_COMPRESSION_THRESHOLD;
  for (const b of this.blocks) {
      // build block buffer: count + entries
      const countBuf = writeUint32(b.entries.length);
      const rawBlock = Buffer.concat([countBuf, ...b.entries]);
      let blockBuf = rawBlock;
      let storedLen = rawBlock.length;
      // compress per-block if requested and compressed smaller
      // attempt compression only when requested and block is reasonably large
      if (compression && rawBlock.length > compressionThreshold) {
        // noop here; actual compression done via sstable helper below
      }
      // apply compression using helper from sstable.ts if available
      try {
        // require dynamically to avoid top-level circular import
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const sstable = require('./sstable');
        if (compression && sstable.COMPRESSION_DEFLATE) {
          const compressed = sstable.compressBlock(rawBlock, compression);
          if (compressed.length < rawBlock.length) {
            blockBuf = compressed;
            // avoid JS bitwise signed-int behavior; use addition to set high bit in unsigned space
            storedLen = compressed.length + sstable.BLOCK_COMPRESSED_FLAG;
          }
        }
      } catch (e) {}

  blocksBufs.push(blockBuf);
  // store the on-disk stored length (with compressed flag if set)
  storedLens.push(storedLen);
  // note: index stores original firstKey (uncompressed)
  firstKeys.push(b.firstKey ?? Buffer.alloc(0));
    }

  const header = Buffer.concat([Buffer.from("SST1"), Buffer.from([3])]); // version 3: rev + walOffset + createdAt
    const offsets: bigint[] = [];
    const lens: number[] = [];
    const cks: number[] = [];
    // compute offsets by walking blocks in order
    let pos = BigInt(SST_HEADER_LEN);
    for (let i = 0; i < blocksBufs.length; i++) {
      const b = blocksBufs[i]!;
      offsets.push(pos);
      // index stores the stored length which may include the compressed flag
      lens.push(storedLens[i] ?? b.length);
      cks.push(crc32c(b));
      pos += BigInt(b.length);
    }

    const idxParts: Buffer[] = [writeUint32(blocksBufs.length)];
    for (let i = 0; i < blocksBufs.length; i++) {
      const fk = firstKeys[i] ?? Buffer.alloc(0);
      const offBig = offsets[i] ?? BigInt(0);
      const ln = lens[i] ?? 0;
      const ck = cks[i] ?? 0;
      idxParts.push(
        writeUint32(fk.length),
        fk,
        writeUint64(offBig),
        writeUint32(ln),
        writeUint32(ck)
      );
    }
    const indexBuf = Buffer.concat(idxParts);
    // indexOffset follows the header and all block buffers
    const indexOffset = pos;
    off = pos + BigInt(indexBuf.length);

    let bloomOffset = BigInt(0);
    let bloomBuf = Buffer.alloc(0);
    if (this.useBloom) {
      let bitsPerEntry = this.opts.bitsPerEntry ?? 10;
      if (this.opts.adaptiveBloom) {
        const avg = Math.max(
          1,
          Math.floor(this.totalKeyValueBytes / Math.max(1, this.totalEntries))
        );
        bitsPerEntry = Math.max(
          this.opts.minBitsPerEntry ?? 6,
          Math.min(this.opts.maxBitsPerEntry ?? 18, Math.round(avg / 8) + 6)
        );
      }
      const entriesCount = Math.max(1, this.totalEntries);
      const bits = Math.max(64, Math.ceil(entriesCount * bitsPerEntry));
      const k = Math.max(1, Math.round((bits / entriesCount) * Math.log(2)));
      const bytes = Math.ceil(bits / 8);
      const bitset = Buffer.alloc(bytes, 0);
      // populate bloom using stored keys
      for (let i = 0; i < this.allKeys.length; i++) {
        const key = this.allKeys[i];
        if (!key) continue;
        const h1 = BigInt(fnv1a(key) >>> 0);
        const h2 = BigInt(crc32c(key) >>> 0);
        for (let j = 0; j < k; j++) {
          const probe = (h1 + BigInt(j) * h2) % BigInt(bits);
          const p = Number(probe);
          const byteIdx = Math.floor(p / 8);
          const bit = p % 8;
          const prev = bitset.readUInt8(byteIdx);
          bitset.writeUInt8(prev | (1 << bit), byteIdx);
        }
      }
      bloomOffset = off;
      bloomBuf = Buffer.concat([writeUint32(bits), writeUint32(k), bitset]);
      off += BigInt(bloomBuf.length);
    }

    const indexCks = crc32c(indexBuf) >>> 0;
    if (process.env.SST_DEBUG) {
      try {
        console.error("SSTWriter.finish debug:");
        console.error(
          " offsets:",
          offsets.map((o) => o.toString())
        );
        console.error(" lens:", lens);
        console.error(" indexLen:", indexBuf.length);
        console.error(" indexCks:", indexCks.toString(16));
      } catch (e) {}
    }
    const footerPre = Buffer.concat([
      writeUint64(indexOffset),
      writeUint64(bloomOffset),
      writeUint32(indexCks),
    ]);
    const footerCks = crc32c(footerPre) >>> 0;
    const footer = Buffer.concat([
      footerPre,
      writeUint32(footerCks),
      writeUint32(0x53535446),
    ]);

    const parts = [header, ...blocksBufs, indexBuf];
    if (bloomBuf.length) parts.push(bloomBuf);
    parts.push(footer);
    const fileBuf = Buffer.concat(parts as Buffer[]);

    writeFileSync(this.tmpPath, fileBuf);
    try {
      const fd2 = openSync(this.tmpPath, "r");
      try {
        fsyncSync(fd2);
      } finally {
        closeSync(fd2);
      }
    } catch (e) {}
    renameSync(this.tmpPath, this.finalPath);

    const minKey = this.blocks[0]?.firstKey ?? Buffer.alloc(0);
    const lastBlock = this.blocks[this.blocks.length - 1];
    const maxKey =
      lastBlock && lastBlock.entries && lastBlock.entries.length
        ? (() => {
            const e = lastBlock.entries[lastBlock.entries.length - 1]!;
            const klen = e.readUInt32BE(0);
            return Buffer.from(e.slice(4, 4 + klen));
          })()
        : Buffer.alloc(0);

    return { file: this.finalPath, minKey, maxKey, size: fileBuf.length };
  }
}
