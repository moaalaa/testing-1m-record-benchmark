const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const fastcsv = require('fast-csv');
const cliProgress = require('cli-progress');
const pidusage = require('pidusage');

const csvFile = path.resolve(__dirname, '../../../test-file.csv');
const resultsDir = path.resolve(__dirname, '../../../results');
const table = 'products_mysql_boring_plain';
const batchSize = 1000;

// Ensure results dir exists
if (!fs.existsSync(resultsDir)) fs.mkdirSync(resultsDir);

// ---------------- METRICS ----------------
const memoryUsage = [];
const memorySpikes = [];
const cpuUsage = [];
const cpuSpikes = [];

let rowsInserted = 0;

// ---------------- HELPERS ----------------
async function recordMetrics() {
    try {
        const stats = await pidusage(process.pid);
        const mem = stats.memory;
        const cpu = stats.cpu;

        memoryUsage.push(mem);
        memorySpikes.push(Math.max(mem, memorySpikes[memorySpikes.length - 1] || 0));

        cpuUsage.push(cpu);
        cpuSpikes.push(Math.max(cpu, cpuSpikes[cpuSpikes.length - 1] || 0));
    } catch (err) {
        // pidusage may fail on some Windows setups (spawn UNKNOWN). Fall back
        // to Node's process.memoryUsage() for memory and 0 for CPU so the
        // import keeps running and metrics arrays remain aligned.
        console.warn('pidusage failed, using fallback metrics:', err && err.message ? err.message : err);
        const mem = (process && process.memoryUsage && process.memoryUsage().rss) || 0;
        const cpu = 0;

        memoryUsage.push(mem);
        memorySpikes.push(Math.max(mem, memorySpikes[memorySpikes.length - 1] || 0));

        cpuUsage.push(cpu);
        cpuSpikes.push(Math.max(cpu, cpuSpikes[cpuSpikes.length - 1] || 0));
    }
}

async function insertBatch(conn, batch) {
    if (batch.length === 0) return;
    const placeholders = batch.map(() => '(?,?,?,?,?,?,?,?,?,?,?,?,?)').join(',');

    // helper to support different header casings and provide defaults
    const get = (row, ...keys) => {
        for (const k of keys) {
            if (Object.prototype.hasOwnProperty.call(row, k) && row[k] !== undefined && row[k] !== null) return row[k];
        }
        return '';
    };

    const values = batch.flatMap(row => [
        // id fields: prefer numeric if possible
        parseInt(get(row, 'Id', 'id'), 10) || 0,
        get(row, 'Name', 'name'),
        get(row, 'Description', 'description'),
        get(row, 'Brand', 'brand'),
        get(row, 'Category', 'category'),
        // Price may be numeric/float
        (function () { const v = get(row, 'Price', 'price'); return v === '' ? 0 : parseFloat(v); })(),
        get(row, 'Currency', 'currency'),
        parseInt(get(row, 'Stock', 'stock'), 10) || 0,
        get(row, 'EAN', 'ean'),
        get(row, 'Color', 'color'),
        get(row, 'Size', 'size'),
        get(row, 'Availability', 'availability'),
        parseInt(get(row, 'InternalID', 'internalid', 'InternalId', 'internalID'), 10) || 0
    ]);

    // Use INSERT IGNORE to skip duplicate primary-key rows instead of failing
    const sql = `INSERT IGNORE INTO ${table} (id, Name, Description, Brand, Category, Price, Currency, Stock, EAN, Color, Size, Availability, InternalID) VALUES ${placeholders}`;
    try {
        const [res] = await conn.query(sql, values);
        return res && (res.affectedRows || res.affectedRows === 0) ? res.affectedRows : 0;
    } catch (err) {
        // Log and continue; duplicates will be ignored by INSERT IGNORE, but other errors logged for inspection
        console.error('Batch insert error:', err && err.message ? err.message : err);
        return 0;
    }
}


// ---------------- MAIN ----------------
(async () => {
    const conn = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'benchmark'
    });

    // Truncate table
    await conn.query(`TRUNCATE TABLE ${table}`);

    const progress = new cliProgress.SingleBar({
        format: 'Progress |{bar}| {value}/{total} rows'
    }, cliProgress.Presets.shades_classic);

    const startTime = Date.now();
    let batch = [];
    const totalRows = 1000000;

    progress.start(totalRows, 0);

    fs.createReadStream(csvFile)
        .pipe(fastcsv.parse({ headers: true }))
        .on('data', async (row) => {
            batch.push(row);
            if (batch.length >= batchSize) {
                const inserted = await insertBatch(conn, batch);
                rowsInserted += inserted;
                batch = [];
                progress.update(rowsInserted);
                await recordMetrics();
            }
        })
        .on('end', async () => {
            if (batch.length > 0) {
                const inserted = await insertBatch(conn, batch);
                rowsInserted += inserted;
                await recordMetrics();
                progress.update(rowsInserted);
            }
            progress.stop();
            const totalTime = (Date.now() - startTime) / 1000;
            const rowsPerSec = rowsInserted / totalTime;

            // Save metrics
            fs.writeFileSync(path.join(resultsDir, 'mysql_boring_a_plain_node.json'), JSON.stringify({
                db: 'MySQL',
                mode: 'Boring',
                variant: 'Plain',
                language: 'Node.js',
                total_rows: rowsInserted,
                total_time_sec: totalTime,
                rows_per_sec: rowsPerSec,
                memory_usage: memoryUsage,
                memory_spikes: memorySpikes,
                cpu_usage: cpuUsage,
                cpu_spikes: cpuSpikes
            }, null, 2));

            console.log(`\nDone! Inserted ${rowsInserted} rows in ${totalTime.toFixed(2)}s (${rowsPerSec.toFixed(0)} rows/sec)`);
            await conn.end();
        });
})();