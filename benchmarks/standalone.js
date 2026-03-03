const { RmConnection } = require('rm-mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;

async function runStandaloneBenchmarks(connCreds) {
  const results = [];

  // Create a single connection for all tests
  console.log('\nConnecting (standalone)...');
  const connTime = await timeIt('Connection init', async () => {
    const conn = new RmConnection(connCreds || creds, { libraries: TEST_SCHEMA });
    await conn.init();
    return conn;
  });
  results.push({ label: 'Connection init', stats: { avg: connTime.durationMs, min: connTime.durationMs, max: connTime.durationMs, median: connTime.durationMs, p95: connTime.durationMs } });
  const conn = connTime.result;

  try {
    // 1. VALUES 1 — minimal round-trip latency
    const valuesResult = await benchmark(
      'VALUES 1 (round-trip)',
      () => conn.query('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => conn.query(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => conn.query(`SELECT * FROM ${tableName} WHERE ID = ?`, { parameters: [42] }),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => conn.query(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => conn.query(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 1000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => conn.query(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => conn.query(`UPDATE ${tableName} SET VALUE = 11.11 WHERE ID = 1`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 1001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => conn.query(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);

    // 9. Sequential batch — 50 SELECTs in a loop
    const batchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} SELECTs)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => conn.query('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    const batchDurations = batchResult.result;
    const { stats } = require('../utils');
    const batchStats = stats(batchDurations);
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: batchStats,
    });

  } finally {
    await conn.close();
  }

  printResults('Standalone (RmConnection) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runStandaloneBenchmarks().catch((err) => {
    console.error('Standalone benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runStandaloneBenchmarks };
