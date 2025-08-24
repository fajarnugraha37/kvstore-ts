const fs = require('fs');
const z = require('node:zlib');
const path = 'data/test_comp.sst';
if (!fs.existsSync(path)) { console.error('sst file missing:', path); process.exit(1); }
const buf = fs.readFileSync(path);
const footer = buf.slice(buf.length - 28);
const indexOffset = Number(footer.readBigUInt64BE(0));
const bloomOffset = Number(footer.readBigUInt64BE(8));
const indexEnd = bloomOffset > 0 ? bloomOffset : buf.length - 28;
const indexBuf = buf.slice(indexOffset, indexEnd);
let pos = 0;
const bc = indexBuf.readUInt32BE(pos); pos += 4;
const idx = [];
for (let i = 0; i < bc; i++) {
    const fklen = indexBuf.readUInt32BE(pos); pos += 4;
    const fk = indexBuf.slice(pos, pos + fklen); pos += fklen;
    const of = Number(indexBuf.readBigUInt64BE(pos)); pos += 8;
    const blen = indexBuf.readUInt32BE(pos); pos += 4;
    const bcks = indexBuf.readUInt32BE(pos); pos += 4;
    idx.push({ firstKey: fk, offset: of, blen, bcks });
}
console.log('blockCount', idx.length);
const target = 'k10';
function findBlockForKey(key) {
    let lo = 0, hi = idx.length - 1, bi = 0;
    while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        const cmp = Buffer.compare(Buffer.from(key), idx[mid].firstKey);
        if (cmp >= 0) { bi = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    return bi;
}
const bi = findBlockForKey(target);
console.log('blockIndex', bi, 'firstKey', idx[bi].firstKey.toString());
const ent = idx[bi];
const storedLen = ent.blen >>> 0;
const compressed = (storedLen & 0x80000000) !== 0;
const onDisk = storedLen & ~0x80000000;
console.log('compressed', compressed, 'onDisk', onDisk, 'offset', ent.offset, 'blockCks', ent.bcks.toString(16));
const storedBuf = buf.slice(ent.offset, ent.offset + onDisk);
function crc32c(data) {
    const POLY = 0x1edc6f41 >>> 0;
    const table = new Uint32Array(256);
    for (let i = 0; i < 256; i++) { let c = i; for (let j = 0; j < 8; j++) { if (c & 1) c = (c >>> 1) ^ POLY; else c >>>= 1; } table[i] = c >>> 0; }
    let crc = 0xffffffff ^ 0xffffffff;
    for (let i = 0; i < data.length; i++) { const byte = data[i]; crc = (crc >>> 8) ^ table[(crc ^ byte) & 0xff]; }
    return (crc ^ 0xffffffff) >>> 0;
}
const crc = crc32c(storedBuf);
console.log('computed crc', crc.toString(16));
let blockBuf;
try { blockBuf = compressed ? z.inflateSync(storedBuf) : storedBuf; } catch (e) { console.error('inflate failed', e); process.exit(1); }
console.log('blockBufLen', blockBuf.length);
function varintDecode(buf, pos) {
    let shift = 0n; let result = 0n; let i = pos;
    while (i < buf.length) {
        const b = BigInt(buf.readUInt8(i));
        result |= (b & 0x7fn) << shift;
        i++;
        if ((b & 0x80n) === 0n) break;
        shift += 7n;
    }
    return { value: Number(result), length: i - pos };
}
let p2 = 0;
const count = blockBuf.readUInt32BE(p2); p2 += 4;
console.log('entryCount', count);
let found = false;
for (let j = 0; j < count; j++) {
    const klen = blockBuf.readUInt32BE(p2); p2 += 4;
    const k = blockBuf.slice(p2, p2 + klen).toString(); p2 += klen;
    const vlen = blockBuf.readUInt32BE(p2); p2 += 4;
    if (vlen === 0xffffffff) {
        const dec = varintDecode(blockBuf, p2); p2 += dec.length;
        if (p2 > blockBuf.length) { console.log('short read after rev'); break; }
        if (true) {
            // for v3 also read wal/created
            const w = varintDecode(blockBuf, p2); p2 += w.length;
            const c = varintDecode(blockBuf, p2); p2 += c.length;
        }
        console.log('entry', j, k, 'tombstone', 'rev', dec.value);
    } else {
        const v = blockBuf.slice(p2, p2 + vlen); p2 += vlen;
        const rev = varintDecode(blockBuf, p2); p2 += rev.length;
        const wal = varintDecode(blockBuf, p2); p2 += wal.length;
        const created = varintDecode(blockBuf, p2); p2 += created.length;
        console.log('entry', j, k, 'vlen', v.length, 'rev', rev.value, 'wal', wal.value, 'created', created.value, k === target ? '<-TARGET' : '');
        if (k === target) found = true;
    }
}
console.log('found', found);
