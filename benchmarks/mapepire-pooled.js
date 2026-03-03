const { Pool } = require('@ibm/mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, stats, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;
const POOL_SIZE = 5;

async function runMapepirePooledBenchmarks(connCreds) {
  const results = [];
  const poolCreds = connCreds || creds;

  // Create and initialize pool
  console.log(`\nInitializing mapepire pool (${POOL_SIZE} connections)...`);
  const poolInitTime = await timeIt('Pool init', async () => {
    const pool = new Pool({
      creds: poolCreds,
      opts: { libraries: [TEST_SCHEMA] },
      maxSize: BATCH_SIZE,
      startingSize: POOL_SIZE,
    });
    await pool.init();
    return pool;
  });
  results.push({
    label: `Pool init (${POOL_SIZE} conns)`,
    stats: { avg: poolInitTime.durationMs, min: poolInitTime.durationMs, max: poolInitTime.durationMs, median: poolInitTime.durationMs, p95: poolInitTime.durationMs },
  });
  // Per-connection estimate (pool creates connections internally)
  const perConnMs = poolInitTime.durationMs / POOL_SIZE;
  results.push({
    label: `Avg conn create (from pool)`,
    stats: { avg: perConnMs, min: perConnMs, max: perConnMs, median: perConnMs, p95: perConnMs },
  });
  const pool = poolInitTime.result;
  console.log(`  Active jobs after init: ${pool.getActiveJobCount()}`);

  try {
    // 1. VALUES 1 — minimal round-trip via pool.execute()
    const valuesResult = await benchmark(
      'VALUES 1 (pool.execute)',
      () => pool.execute('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => pool.execute(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => pool.execute(`SELECT * FROM ${tableName} WHERE ID = ?`, { parameters: [42] }),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => pool.execute(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => pool.execute(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 4000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => pool.execute(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => pool.execute(`UPDATE ${tableName} SET VALUE = 44.44 WHERE ID = 2`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 4001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => pool.execute(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);
    console.log(`  Active jobs after CRUD tests: ${pool.getActiveJobCount()}`);

    // 9. Sequential batch — 50 queries via pool.execute()
    const seqBatchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} queries)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => pool.execute('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: stats(seqBatchResult.result),
    });
    console.log(`  Active jobs after sequential batch: ${pool.getActiveJobCount()}`);

    // 10. Concurrent batch — 50 queries via Promise.all
    const concurrentResult = await timeIt(
      `Concurrent batch (${BATCH_SIZE} queries)`,
      async () => {
        const promises = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          promises.push(timeIt('', () => pool.execute('VALUES 1')));
        }
        const settled = await Promise.all(promises);
        return settled.map((r) => r.durationMs);
      }
    );
    results.push({
      label: `Concurrent ${BATCH_SIZE}x VALUES 1`,
      stats: stats(concurrentResult.result),
    });
    results.push({
      label: `Concurrent total wallclock`,
      stats: { avg: concurrentResult.durationMs, min: concurrentResult.durationMs, max: concurrentResult.durationMs, median: concurrentResult.durationMs, p95: concurrentResult.durationMs },
    });
    console.log(`  Active jobs after concurrent batch: ${pool.getActiveJobCount()}`);

    // 11. Manual getJob + execute (job stays in pool)
    const manualResult = await benchmark(
      'Manual getJob+execute',
      async () => {
        const job = pool.getJob();
        await job.execute('VALUES 1');
      },
      ITERATIONS
    );
    results.push(manualResult);
    console.log(`  Active jobs after all tests: ${pool.getActiveJobCount()}`);

  } finally {
    // pool.end() calls job.close() without awaiting the promises,
    // so WebSocket connections linger. We wait briefly for them to close.
    pool.end();
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  printResults('Mapepire Pooled (Pool) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runMapepirePooledBenchmarks().catch((err) => {
    console.error('Mapepire pooled benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runMapepirePooledBenchmarks };
