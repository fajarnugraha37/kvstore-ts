const { spawnSync } = require('child_process');
const { writeFileSync, mkdirSync, appendFileSync } = require('fs');
const path = require('path');
const os = require('os');

function run(cmd, args, opts = {}) {
    const res = spawnSync(cmd, args, Object.assign({ encoding: 'utf8' }, opts));
    return res;
}

function main() {
    try {
        const outDir = path.resolve('tmp');
        try { mkdirSync(outDir, { recursive: true }); } catch (e) { }
        const outFile = path.join(outDir, 'mitata_last.txt');

        console.log('Running bench (this may take a moment)...');
        // capture CPU totals before
        function cpuTotals() {
            const cpus = os.cpus();
            let user = 0,
                nice = 0,
                sys = 0,
                idle = 0,
                irq = 0;
            for (const c of cpus) {
                user += c.times.user;
                nice += c.times.nice;
                sys += c.times.sys;
                idle += c.times.idle;
                irq += c.times.irq;
            }
            return { user, nice, sys, idle, irq };
        }

        const before = cpuTotals();
        const t0 = Date.now();
        const r = run('bun', ['run', 'bench/kv_bench.ts']);
        const t1 = Date.now();
        const after = cpuTotals();

        const deltaIdle = after.idle - before.idle;
        const deltaTotal =
            after.user + after.nice + after.sys + after.idle + after.irq -
            (before.user + before.nice + before.sys + before.idle + before.irq);
        const cpuBusyPercent = deltaTotal > 0 ? ((deltaTotal - deltaIdle) / deltaTotal) * 100 : 0;

        const combined = (r.stdout || '') + (r.stderr || '');
        writeFileSync(outFile, combined, 'utf8');
        // append run meta so parser can pick it up
        appendFileSync(
            outFile,
            `\nRUN_META cpu_percent=${cpuBusyPercent.toFixed(2)} duration_ms=${t1 - t0}\n`,
            'utf8'
        );
        console.log('Bench finished, output written to', outFile);

        console.log('Updating README.md with parsed summary...');
        const r2 = run('bun', ['run', './scripts/append_bench_readme.ts', outFile]);
        if (r2.error) {
            console.error('Failed to run append script:', r2.error);
            process.exit(1);
        }
        console.log(r2.stdout || r2.stderr || 'Append script completed');
        process.exit(0);
    } catch (e) {
        console.error('Error:', e && e.stack ? e.stack : e);
        process.exit(1);
    }
}

main();
