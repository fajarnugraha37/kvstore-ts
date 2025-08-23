const fs = require('node:fs');
const path = require('node:path');

const dir = path.join(__dirname, '..', 'data');
try {
    if (fs.existsSync(dir)) {
        // Remove directory and contents synchronously
        fs.rmSync(dir, { recursive: true, force: true });
        console.log('clean_data: removed', dir);
    }
    // recreate empty data dir
    fs.mkdirSync(dir, { recursive: true });
    console.log('clean_data: created', dir);
} catch (e) {
    console.error('clean_data: failed to clean data dir', e && e.message ? e.message : e);
    process.exitCode = 1;
}
