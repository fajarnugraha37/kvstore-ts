const { spawnSync } = require('node:child_process');
const fs = require('fs');
const path = require('path');
// bun run scripts/stress/wal_stress.ts --dir bench-out/handoff --duration 5 --concurrency 8 --payloadSize 512 --batching false --mode handoff --metricsOut bench-out/metrics_handoff.json
function run(mode, outDir) {
    const metricsOut = path.join(outDir, `metrics_${mode}.json`);
    const args = [
        'run',
        'scripts/stress/wal_stress.ts',
        '--dir', path.join(outDir, mode),
        '--duration', '5',
        '--concurrency', '8',
        '--payloadSize', '512',
        '--batching', 'false',
        '--mode', mode,
        '--metricsOut', metricsOut,
    ];
    console.log('Running:', 'bun', args.join(' '));
    const r = spawnSync('bun', args, { stdio: 'inherit' });
    if (r.status !== 0) {
        console.error('run failed for mode', mode);
        return null;
    }
    if (!fs.existsSync(metricsOut)) return null;
    return JSON.parse(fs.readFileSync(metricsOut, 'utf8'));
}

function summarize(m) {
    const last = m.metrics[m.metrics.length - 1] || { entriesAppended: 0, bytesAppended: 0 };
    return { entries: last.entriesAppended, bytes: last.bytesAppended };
}

const outDir = './bench-out';
try { fs.rmSync(outDir, { recursive: true }); } catch { }
fs.mkdirSync(outDir, { recursive: true });

const promise = run('promise', outDir);
const handoff = run('handoff', outDir);

console.log('\nRESULTS:\n');
console.log('Promise-chain:', promise ? summarize(promise) : 'failed');
console.log('Handoff:', handoff ? summarize(handoff) : 'failed');
