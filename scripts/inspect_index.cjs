const fs = require('fs');
const path = 'data/test_comp.sst';
if (!fs.existsSync(path)) { console.error('missing sst'); process.exit(1); }
const buf = fs.readFileSync(path);
const footer = buf.slice(buf.length - 28);
const indexOffset = Number(footer.readBigUInt64BE(0));
const bloomOffset = Number(footer.readBigUInt64BE(8));
const indexEnd = bloomOffset > 0 ? bloomOffset : buf.length - 28;
const indexBuf = buf.slice(indexOffset, indexEnd);
let pos = 0;
const bc = indexBuf.readUInt32BE(pos); pos += 4;
console.log('blockCount', bc);
for (let i = 0; i < Math.min(30, bc); i++) {
    const fklen = indexBuf.readUInt32BE(pos); pos += 4;
    const fk = indexBuf.slice(pos, pos + fklen).toString(); pos += fklen;
    const of = Number(indexBuf.readBigUInt64BE(pos)); pos += 8;
    const blen = indexBuf.readUInt32BE(pos); pos += 4;
    const bcks = indexBuf.readUInt32BE(pos); pos += 4;
    console.log(i, fk, 'offset', of, 'blen', blen.toString(16));
}

// binary search function
function findBlockForKey(idx, key) {
    let lo = 0, hi = idx.length - 1, bi = 0;
    while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        const cmp = Buffer.compare(Buffer.from(key), idx[mid].firstKey);
        if (cmp >= 0) { bi = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    return bi;
}

// build array
pos = 4; const arr = [];
for (let i = 0; i < bc; i++) {
    const fklen = indexBuf.readUInt32BE(pos); pos += 4;
    const fk = indexBuf.slice(pos, pos + fklen); pos += fklen;
    const of = Number(indexBuf.readBigUInt64BE(pos)); pos += 8;
    const blen = indexBuf.readUInt32BE(pos); pos += 4;
    const bcks = indexBuf.readUInt32BE(pos); pos += 4;
    arr.push({ firstKey: fk, offset: of, blen });
}

['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k100'].forEach(k => {
    const bi = findBlockForKey(arr, k);
    console.log('key', k, '-> block', bi, 'firstKey', arr[bi].firstKey.toString());
});
