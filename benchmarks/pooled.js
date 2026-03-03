const { RmPool } = require('rm-mapepire-js');
const { creds, tableName, TEST_SCHEMA } = require('../config');
const { benchmark, timeIt, stats, printResults } = require('../utils');

const ITERATIONS = 10;
const BATCH_SIZE = 50;
const POOL_SIZE = 5;

async function runPooledBenchmarks(connCreds) {
  const results = [];
  const poolCreds = connCreds || creds;

  // Create and initialize pool
  console.log(`\nInitializing pool (${POOL_SIZE} connections)...`);
  const poolInitTime = await timeIt('Pool init', async () => {
    const pool = new RmPool(
      {
        id: 'bench',
        config: {
          id: 'bench',
          PoolOptions: {
            creds: poolCreds,
            maxSize: BATCH_SIZE,
            initialConnections: { size: POOL_SIZE, expiry: null },
            incrementConnections: { size: 5, expiry: 1 },
            JDBCOptions: { libraries: TEST_SCHEMA },
            healthCheck: { onAttach: false },
          },
        },
      },
      false
    );
    await pool.init();
    return pool;
  });
  results.push({
    label: `Pool init (${POOL_SIZE} conns)`,
    stats: { avg: poolInitTime.durationMs, min: poolInitTime.durationMs, max: poolInitTime.durationMs, median: poolInitTime.durationMs, p95: poolInitTime.durationMs },
  });
  // Per-connection stats from pool init
  const perConnMs = poolInitTime.durationMs / POOL_SIZE;
  results.push({
    label: `Avg conn create (from pool)`,
    stats: { avg: perConnMs, min: perConnMs, max: perConnMs, median: perConnMs, p95: perConnMs },
  });
  const pool = poolInitTime.result;
  console.log(`  Total connections after init: ${pool.getStats().total}`);

  try {
    // 1. VALUES 1 — minimal round-trip via pool.query() (auto attach/detach)
    const valuesResult = await benchmark(
      'VALUES 1 (pool.query)',
      () => pool.query('VALUES 1'),
      ITERATIONS
    );
    results.push(valuesResult);

    // 2. Simple SELECT — all rows
    const selectAllResult = await benchmark(
      'SELECT * (100 rows)',
      () => pool.query(`SELECT * FROM ${tableName}`),
      ITERATIONS
    );
    results.push(selectAllResult);

    // 3. Parameterized SELECT
    const paramSelectResult = await benchmark(
      'SELECT WHERE ID = ? (param)',
      () => pool.query(`SELECT * FROM ${tableName} WHERE ID = ?`, { parameters: [42] }),
      ITERATIONS
    );
    results.push(paramSelectResult);

    // 4. SELECT with LIMIT
    const limitResult = await benchmark(
      'SELECT LIMIT 10',
      () => pool.query(`SELECT * FROM ${tableName} ORDER BY ID FETCH FIRST 10 ROWS ONLY`),
      ITERATIONS
    );
    results.push(limitResult);

    // 5. COUNT aggregate
    const countResult = await benchmark(
      'SELECT COUNT(*)',
      () => pool.query(`SELECT COUNT(*) AS CNT FROM ${tableName}`),
      ITERATIONS
    );
    results.push(countResult);

    // 6. INSERT single row
    let insertId = 2000;
    const insertResult = await benchmark(
      'INSERT single row',
      () => pool.query(`INSERT INTO ${tableName} (ID, NAME, VALUE) VALUES (${++insertId}, 'Bench', 99.99)`),
      ITERATIONS
    );
    results.push(insertResult);

    // 7. UPDATE single row
    const updateResult = await benchmark(
      'UPDATE single row',
      () => pool.query(`UPDATE ${tableName} SET VALUE = 22.22 WHERE ID = 2`),
      ITERATIONS
    );
    results.push(updateResult);

    // 8. DELETE single row
    let deleteId = 2001;
    const deleteResult = await benchmark(
      'DELETE single row',
      () => pool.query(`DELETE FROM ${tableName} WHERE ID = ${deleteId++}`),
      ITERATIONS
    );
    results.push(deleteResult);
    console.log(`  Total connections after CRUD tests: ${pool.getStats().total}`);

    // 9. Sequential batch — 50 queries via pool.query()
    const seqBatchResult = await timeIt(
      `Sequential batch (${BATCH_SIZE} queries)`,
      async () => {
        const durations = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          const { durationMs } = await timeIt('', () => pool.query('VALUES 1'));
          durations.push(durationMs);
        }
        return durations;
      }
    );
    results.push({
      label: `Seq. batch ${BATCH_SIZE}x VALUES 1`,
      stats: stats(seqBatchResult.result),
    });
    console.log(`  Total connections after sequential batch: ${pool.getStats().total}`);

    // 10. Concurrent batch — 50 queries via Promise.all
    const concurrentResult = await timeIt(
      `Concurrent batch (${BATCH_SIZE} queries)`,
      async () => {
        const promises = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
          promises.push(timeIt('', () => pool.query('VALUES 1')));
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
    console.log(`  Total connections after concurrent batch: ${pool.getStats().total}`);

    // 11. Manual attach / query / detach
    const manualResult = await benchmark(
      'Manual attach+query+detach',
      async () => {
        const conn = await pool.attach();
        await conn.query('VALUES 1');
        await pool.detach(conn);
      },
      ITERATIONS
    );
    results.push(manualResult);

    // 12. Manual attach, run 10 queries, then detach
    const bulkManualResult = await benchmark(
      'Attach + 10 queries + detach',
      async () => {
        const conn = await pool.attach();
        for (let i = 0; i < 10; i++) {
          await conn.query('VALUES 1');
        }
        await pool.detach(conn);
      },
      ITERATIONS
    );
    results.push(bulkManualResult);
    console.log(`  Total connections after all tests: ${pool.getStats().total}`);

  } finally {
    await pool.close();
  }

  printResults('Pooled (RmPool) Benchmarks', results);
  return results;
}

// Allow standalone execution
if (require.main === module) {
  require('dotenv').config();
  runPooledBenchmarks().catch((err) => {
    console.error('Pooled benchmarks failed:', err);
    process.exit(1);
  });
}

module.exports = { runPooledBenchmarks };
