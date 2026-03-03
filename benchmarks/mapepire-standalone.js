const { SQLJob } = require('@ibm/mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, stats, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;

async function runMapepireStandaloneBenchmarks(connCreds) {
  const results = [];
  const jobCreds = connCreds || creds;

  // Create a single SQLJob for all tests
  console.log('\nConnecting (mapepire standalone)...');
  const connTime = await timeIt('Connection init', async () => {
    const job = new SQLJob({ libraries: [TEST_SCHEMA] });
    await job.connect(jobCreds);
    return job;
  });
  results.push({
    label: 'Connection init',
    stats: { avg: connTime.durationMs, min: connTime.durationMs, max: connTime.durationMs, median: connTime.durationMs, p95: connTime.durationMs },
  });
  const job = connTime.result;

  try {
    // 1. VALUES 1 — minimal round-trip latency
    const valuesResult = await benchmark(
      'VALUES 1 (round-trip)',
      () => job.execute('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => job.execute(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => job.execute(`SELECT * FROM ${tableName} WHERE ID = ?`, { parameters: [42] }),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => job.execute(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => job.execute(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 3000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => job.execute(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => job.execute(`UPDATE ${tableName} SET VALUE = 33.33 WHERE ID = 1`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 3001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => job.execute(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);

    // 9. Sequential batch — 50 SELECTs in a loop
    const batchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} SELECTs)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => job.execute('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: stats(batchResult.result),
    });

  } finally {
    await job.close();
  }

  printResults('Mapepire Standalone (SQLJob) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runMapepireStandaloneBenchmarks().catch((err) => {
    console.error('Mapepire standalone benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runMapepireStandaloneBenchmarks };
